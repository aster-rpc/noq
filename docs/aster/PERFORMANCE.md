# noq Performance Improvement Roadmap

Tracking suspected high-value performance improvements for noq, informed by
Aeron IPC design patterns and dhat allocation profiling.

## Status of Submitted PRs

| PR | Status | Description |
|----|--------|-------------|
| #581 | Submitted | Reuse `response_buffer` across datagrams in recv loop |
| #582 | Submitted | Add dhat heap allocation profiling support |
| #583 | Submitted | Eliminate per-datagram memcpy and allocation in recv path |

## Profiling Baseline

Measured with dhat, 100 MB upload + 100 MB download, localhost:

- **Total program allocations:** 5.69M blocks / 1.16 GB
- **`poll_socket` (recv hot path):** 1.35M blocks / 681 MB
  - 677K blocks: BytesMut data copy from recv buffer (654 MB)
  - 677K blocks: `split_to` SharedVtable allocation (27 MB)
- **Uninstrumented throughput:** ~1.7 Gbps (single connection, localhost)

PR #583 reduces `poll_socket` to 668K blocks (−50.7%) and eliminates the
654 MB memcpy. Remaining 668K blocks are `BytesMut::zeroed` refills.

---

## Candidate Improvements (Prioritized)

### 1. SPSC Ring Buffer for Endpoint ↔ Connection Communication

**Aeron concept:** `OneToOneRingBuffer` — pre-allocated power-of-2 buffer,
single-writer tail advance (no CAS), cached head position to reduce
cross-core cache line bouncing, padding records at wrap boundaries.

**Current noq:** `mpsc::UnboundedSender<ConnectionEvent>` per connection
(`endpoint.rs:680`). Tokio's unbounded channel is a linked list — each
`send()` allocates a node. No backpressure.

**Expected impact:** Eliminates per-datagram channel node allocation, provides
natural backpressure, reduces atomic operations on the dispatch hot path.
Most impactful at high connection counts (100+) and high packet rates.

**How to prove:** Add a connection-scaling benchmark (100, 1000, 10000
concurrent connections with modest traffic each). Profile channel allocation
overhead with dhat. Microbenchmark Tokio unbounded mpsc vs custom SPSC ring
for the specific message types used.

**Effort:** 2–3 days. Medium-high complexity — touches the endpoint/connection
communication contract.

**Prerequisite:** PR #583 accepted (establishes allocation profiling pattern).

---

### 2. Split Endpoint Mutex (Driver-Owned vs Shared State)

**Aeron concept:** Single-writer principle. The conductor owns mutable state
exclusively; publications/subscriptions communicate through lock-free
structures. No mutex on data paths.

**Current noq:** Single `Mutex<State>` on `EndpointInner` (`endpoint.rs:457`),
held across the entire `drive_recv` + `handle_events` cycle. Every
`accept()`, `local_addr()`, `open_connections()`, `stats()` call from
application code contends on this same lock.

**Expected impact:** Removes the mutex from the recv hot path entirely.
Matters under concurrent access patterns (accepting connections while
processing traffic). Less impactful for single-connection benchmarks.

**How to prove:** Add a benchmark with concurrent accept + high-rate traffic.
Use the existing lock duration tracking (`noq/src/mutex.rs`) to measure
contention before/after. Profile with `mutrace` on Linux.

**Effort:** 1–2 days. Requires splitting `State` into `DriverState`
(owned exclusively by the driver task, no mutex) and `SharedState`
(behind a lightweight lock or atomics for stats/ref_count/waker).

---

### 3. Send Pacing

**Aeron concept:** `ControlledMessageDrain` — spread transmissions evenly
across RTT intervals instead of bursting.

**Current noq:** `poll_transmit` writes as fast as the congestion window allows
within a single call. BBR and Cubic control the window size, but there is no
inter-packet pacing. This can cause buffer bloat at intermediate switches.

**Expected impact:** Reduced packet loss under congestion, improved throughput
in lossy/WAN conditions. BBR explicitly models a pacing rate that noq doesn't
currently enforce.

**How to prove:** Run the existing netsim benchmarks (WAN: 50ms/100Mbps/0.1%
loss, Lossy: 20ms/100Mbps/5% loss) before/after. Measure retransmission
rates and goodput.

**Effort:** 1–2 days. Add a token-bucket or leaky-bucket pacer between
`poll_transmit` and the socket send path.

---

### 4. BytesMut Recv Buffer Pooling

**Aeron concept:** Pre-allocated term buffers that are recycled after consumers
finish processing.

**Current noq (after PR #583):** Each consumed recv slot is refilled with
`BytesMut::zeroed(chunk_size)` — a fresh allocation per batch slot per recv
cycle (~668K allocations for a 200 MB transfer).

**Expected impact:** Eliminates the remaining recv-path allocations by
recycling BytesMut buffers that the proto layer has dropped. Requires
tracking buffer lifecycle or using a thread-local pool.

**How to prove:** dhat before/after showing `poll_socket` allocation count
dropping toward zero. Would also show up in Criterion throughput benchmarks
for small-packet workloads.

**Effort:** 1 day. Either a `VecDeque<BytesMut>` pool on `RecvState` with a
custom drop hook, or leverage the allocator's thread-local free lists (which
already provide some of this benefit implicitly).

**Prerequisite:** PR #583 accepted.

---

### 5. Slab Allocator for Stream State

**Aeron concept:** Arrays indexed by stream/registration ID for O(1) lookup
with cache-friendly iteration.

**Current noq:** `FxHashMap<StreamId, Send/Recv>` in the proto layer. FxHashMap
is fast but each entry is a separate heap allocation, and iteration order is
random (cache-unfriendly).

**Expected impact:** O(1) access, better cache locality, no hashing overhead.
Matters for workloads with many concurrent streams (100+).

**How to prove:** Add a high-stream-count benchmark. Profile with `perf stat`
for cache miss rates before/after.

**Effort:** 1–2 days. Replace `FxHashMap` with `slab` crate, indexed by
stream ID.

---

### 6. Adaptive Idle Strategy (BackoffIdleStrategy)

**Aeron concept:** Progressive backoff state machine:
`NOT_IDLE → SPINNING (10 spins) → YIELDING (5 yields) → PARKING (1µs→1ms)`.
Reset on work detection.

**Current noq:** The endpoint driver returns `Poll::Pending` and relies on
Tokio's waker, which parks the task immediately. First packet after idle pays
full task wake-up cost.

**Expected impact:** Reduced p99/p99.9 latency for request-response workloads
with variable inter-request gaps.

**How to prove:** Extend the `perf` binary to capture p99.9 and p99.99
latency. Run request-response benchmark with varying inter-request gaps.

**Effort:** 0.5 days. Config knob on the endpoint driver: spin for N
iterations before yielding to the runtime.

---

### 7. Shared Connection Driver (Architectural)

**Aeron concept:** Single conductor thread processes all connections in a tight
loop, avoiding per-connection task scheduling overhead.

**Current noq:** Each connection spawns its own async task
(`ConnectionDriver`). At scale (thousands of connections), each task has its
own stack allocation, scheduling overhead, and waker management.

**Expected impact:** Significant at high connection counts (1000+). Reduces
Tokio runtime scheduling overhead.

**How to prove:** Connection-scaling benchmark measuring CPU per connection.

**Effort:** Very high. Major architectural change to the async wrapper layer.
Would not change the proto layer.

**Note:** This is a long-term consideration, not a near-term PR.

---

## Measurement Infrastructure Needed

To properly evaluate candidates 1–7, the project would benefit from:

- **High-packet-rate small-message benchmark:** Many small datagrams (100–200
  bytes) at maximum packet rate. The existing benchmarks lean toward bulk
  transfer. This is the scenario where per-datagram overhead dominates.
- **Connection-scaling benchmark:** 100, 1000, 10000 concurrent connections
  with modest traffic each. Tests dispatch and scheduling overhead.
- **Higher-percentile latency capture:** Extend `perf/src/stats.rs` to report
  p99.9 and p99.99, not just p90.

---

## References

- [Aeron Design Overview](https://github.com/aeron-io/aeron/wiki/Design-Overview)
- [Aeron Media Driver Operation](https://github.com/aeron-io/aeron/wiki/Media-Driver-Operation)
- [Aeron Flow and Congestion Control](https://github.com/aeron-io/aeron/wiki/Flow-and-Congestion-Control)
- [aeron-rs (Rust implementation)](https://github.com/UnitedTraders/aeron-rs)
