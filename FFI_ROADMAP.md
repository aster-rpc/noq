# noq FFI Readiness Roadmap

Analysis of what can be done at the noq layer to improve FFI performance for
language bindings (Go, Java, Python, etc.) in the Aster stack.

**Context:** Aster uses Iroh, which uses noq. The FFI boundary in Aster
(`aster_transport_ffi`) exposes a C ABI with a completion queue and SPSC ring
buffer for RPC dispatch. This document focuses on what noq (the lowest layer)
can do to reduce friction and overhead at that boundary.

---

## The Problem

noq's async API (`noq` crate) is designed for Rust consumers using Tokio or
Smol. Every operation goes through `async fn` → Tokio task → waker → poll.
When the consumer is a C ABI bridge that just needs to move bytes between a
QUIC connection and a ring buffer, the async machinery adds:

1. **Unnecessary task scheduling** — each connection spawns a `ConnectionDriver`
   task; the FFI bridge spawns another task per operation to call async methods
   and post results to the completion queue. That's 2 task wake-ups per I/O.

2. **Mandatory `Bytes` allocation** — received data arrives as `Bytes` (heap
   ref-counted). The FFI layer then copies it into a buffer the foreign
   language can read. Two allocations, one copy.

3. **No batch API** — `RecvStream::read()` returns one chunk. The FFI bridge
   calls it in a loop, posting one completion event per chunk. For small RPC
   messages this is fine; for streaming, the per-call overhead accumulates.

4. **Tokio dependency** — Go and Java have their own event loops. Embedding a
   Tokio runtime inside a Go process (which has its own goroutine scheduler)
   wastes threads and creates contention. The sans-IO `noq-proto` layer
   doesn't require Tokio, but there's no convenient way to drive it without
   the async `noq` crate.

---

## What noq Already Has (Sans-IO)

`noq-proto` is a **sans-IO** protocol library. It doesn't do I/O itself — it
processes incoming bytes and produces outgoing bytes. The API is:

```rust
// Feed a received UDP datagram
endpoint.handle(now, addresses, ecn, data, &mut response_buffer) -> Option<DatagramEvent>

// Get bytes to send
connection.poll_transmit(now, max_datagrams, &mut send_buffer) -> Option<Transmit>

// Process a connection event
connection.handle_event(event)

// Read from a stream
connection.recv_stream(stream_id).read(buf) -> Result<Option<usize>>
```

This is already runtime-agnostic. The problem is that the `noq` async crate
(the thing Iroh and Aster actually use) wraps this in Tokio tasks and async
channels, and there's no intermediate "poll-based" API that an FFI bridge
could use without Tokio.

---

## Proposed Changes (Prioritized)

### 1. Poll-Based Connection Driver (Medium effort, High FFI impact)

**Problem:** The async `noq` crate spawns a Tokio task per connection
(`ConnectionDriver`) that blocks on wakers. An FFI bridge can't call
`connection.poll()` from a non-Tokio thread without a full runtime.

**Proposal:** Add a `PollConnection` wrapper in the `noq` crate (or a new
`noq-poll` crate) that exposes a synchronous poll-based API:

```rust
pub struct PollConnection {
    inner: proto::Connection,
    socket: UdpSocket,        // std::net, not Tokio
    send_buffer: Vec<u8>,
}

impl PollConnection {
    /// Drive the connection forward. Call this after socket recv or on timer.
    /// Returns events to process (stream readable, stream writable, etc.)
    pub fn drive(&mut self, now: Instant) -> Vec<ConnectionEvent>;

    /// Get datagrams to send
    pub fn poll_transmit(&mut self, now: Instant) -> Option<Transmit>;

    /// Read from a stream (non-blocking, returns what's available)
    pub fn stream_recv(&mut self, stream: StreamId, buf: &mut [u8]) -> Result<usize>;

    /// Write to a stream (non-blocking, returns bytes accepted)
    pub fn stream_send(&mut self, stream: StreamId, data: &[u8]) -> Result<usize>;

    /// Next timer deadline
    pub fn poll_timeout(&self) -> Option<Instant>;
}
```

This lets the FFI bridge drive connections from its own event loop (epoll,
kqueue, io_uring, or Go's netpoller) without Tokio. The Aster reactor's pump
task could call `drive()` directly instead of going through async channels.

**Tracking upstream:** This is additive — it doesn't modify existing APIs.
Could live in a separate `noq-poll` crate or behind a feature flag.

---

### 2. Zero-Copy Receive Into Caller-Provided Buffers (Medium effort, High impact)

**Problem:** Currently, received stream data is delivered as `Bytes` (heap
allocated, reference counted). The FFI bridge then copies this into a buffer
that the foreign language owns. Two allocations and one memcpy per read.

**Proposal:** Add a `read_into` variant to `RecvStream` that writes directly
into a caller-provided `&mut [u8]`:

```rust
impl RecvStream {
    /// Read directly into caller's buffer. No intermediate Bytes allocation.
    pub async fn read_into(&mut self, buf: &mut [u8]) -> Result<Option<usize>>;
}
```

The proto layer's `Assembler` already stores data in contiguous chunks. The
change is to allow copying from the assembler directly into the target buffer
instead of returning a `Bytes` handle.

**For the SPSC ring buffer pattern:** The FFI bridge could provide a slice of
the ring buffer itself as the target, achieving true zero-copy from QUIC
decrypt → ring buffer → foreign language reader.

**Tracking upstream:** `read_into` is a natural addition to the stream API.
Quinn has had requests for this. Non-breaking, additive.

---

### 3. Batch Stream Read API (Low effort, Medium impact)

**Problem:** `RecvStream::read()` / `read_chunk()` return one chunk per call.
For the FFI completion queue pattern, each chunk generates one event. When
receiving many small messages, the per-event overhead dominates.

**Proposal:** Add a batch read that fills a buffer with as many available
chunks as fit:

```rust
impl RecvStream {
    /// Read all available data into buf, up to buf.len() bytes.
    /// Returns total bytes read (may be 0 if nothing available yet).
    pub async fn read_available(&mut self, buf: &mut [u8]) -> Result<usize>;
}
```

This lets the FFI bridge drain a stream into a ring buffer slot in one call
instead of looping.

**Tracking upstream:** Additive, non-breaking.

---

### 4. Expose `noq-proto` Connection Events as C-Compatible Enum (Low effort, Medium impact)

**Problem:** The proto layer emits `ConnectionEvent` and `EndpointEvent` as
Rust enums with associated data. These can't cross an FFI boundary. The async
`noq` crate translates them into channel messages between internal tasks, then
re-translates them into user-facing async methods. The FFI bridge then wraps
those methods in yet another translation to `iroh_event_t`.

**Proposal:** Add `#[repr(C)]` event descriptors to `noq-proto` that can be
used directly by an FFI bridge:

```rust
#[repr(C)]
pub struct RawConnectionEvent {
    pub kind: u32,          // EventKind discriminant
    pub stream_id: u64,     // if applicable
    pub error_code: u64,    // if applicable
    pub data_offset: u64,   // for stream data events
    pub data_len: u32,      // for stream data events
}
```

This lets an FFI consumer drive `noq-proto` directly and translate events to
its completion queue format without going through the async layer.

**Tracking upstream:** This could be behind a `ffi` feature flag. Non-breaking.

---

### 5. Shared-Memory / mmap-Friendly Buffer Management (High effort, High impact, Long-term)

**Problem:** The Aeron-inspired SPSC ring buffer in Aster's reactor works
great for in-process FFI. But for out-of-process IPC (separate QUIC process
serving multiple language runtimes), the data still needs to cross a process
boundary.

**Proposal:** Make the receive buffer pool (from PR #583's `BytesMut` per-slot
design) optionally backed by a shared memory region. The proto layer writes
received data into a memory-mapped buffer; the FFI consumer reads from the
same mapping. Zero copies end-to-end.

This mirrors Aeron's design where the log buffer is an mmap'd file accessible
to both the media driver and clients.

**Tracking upstream:** This would be a significant change. More appropriate as
a `noq-shm` crate or behind an `ipc` feature. Long-term.

---

### 6. Runtime Trait for Non-Tokio Event Loops (Medium effort, High impact)

**Problem:** noq's `Runtime` trait requires `spawn()` (fire-and-forget task),
`new_timer()` (async timer), and `wrap_udp_socket()` (async socket). Go, Java,
and .NET have their own event loops and don't want to embed Tokio.

**Proposal:** The `Runtime` trait already exists and is abstract. What's
missing is a **reference implementation** for a minimal poll-based runtime
that doesn't require Tokio or Smol:

```rust
pub struct PollRuntime {
    // Uses std::net::UdpSocket + epoll/kqueue directly
    // Timers via a simple deadline heap
    // No task spawning — caller drives the event loop
}
```

This would let an FFI bridge (or Go/Java) provide their own socket and timer
implementations, driving noq without a Rust async runtime.

**Tracking upstream:** The trait is already pluggable. A poll-based runtime
implementation would be valuable to the broader noq community.

---

## What NOT to Change at the noq Layer

- **Don't bypass TLS** — the security boundary must stay. FFI performance
  gains come from reducing copies and task overhead, not skipping crypto.
- **Don't make noq-proto async** — its sans-IO design is exactly what FFI
  wants. Keep it synchronous and poll-based.
- **Don't add language-specific code** — noq should expose generic C-friendly
  primitives. Language-specific wrappers belong in Iroh or Aster.

---

## Recommended Implementation Order

| # | Change | Where | Effort | Impact |
|---|--------|-------|--------|--------|
| 1 | `read_into(&mut [u8])` on RecvStream | noq | 1-2 days | Eliminates one alloc+copy per read in FFI path |
| 2 | Batch `read_available` API | noq | 0.5 day | Reduces per-chunk FFI event overhead |
| 3 | Poll-based connection driver | noq-poll (new) | 3-5 days | Eliminates Tokio dependency for FFI |
| 4 | `#[repr(C)]` event descriptors | noq-proto | 1-2 days | Direct proto→FFI event translation |
| 5 | Minimal poll-based Runtime impl | noq | 2-3 days | Go/Java can provide own event loop |
| 6 | Shared-memory buffer backing | noq-shm (new) | 5+ days | Zero-copy IPC, Aeron-style |

Items 1-2 are small, additive changes that directly benefit Aster's FFI
layer. Items 3-5 are the architectural pieces for a Tokio-free FFI path.
Item 6 is a long-term goal that builds on the Aeron design work.

---

## Relationship to Iroh

Iroh sits between noq and Aster. Changes at the noq layer propagate up:

- `read_into` on noq's `RecvStream` → Iroh exposes it → Aster's FFI bridge
  uses it to write directly into ring buffer slots
- Poll-based driver in noq → Iroh's endpoint could optionally use it →
  Aster's reactor drives connections without Tokio task overhead
- `#[repr(C)]` events in noq-proto → Iroh could expose them → Aster's FFI
  maps them to `aster_reactor_call_t` with minimal translation

The key principle: push zero-copy and poll-based APIs as low in the stack as
possible, so every layer above benefits.
