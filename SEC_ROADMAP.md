# noq Security Improvement Roadmap

Security audit findings for the noq QUIC implementation, focused on areas
reachable from untrusted network input.

---

## Critical

### 1. Unbounded ACK Range Processing (CPU Exhaustion)

**Location:** `noq-proto/src/frame.rs:1762`

`read_ack_blocks()` parses the ACK frame's `num_blocks` field as a VarInt
(max 2^62 − 1) and loops that many times with no upper bound check:

```rust
let num_blocks = buf.get_var()?;  // No bound check
for num_block in 0..num_blocks + 1 { ... }
```

A single ~100-byte malformed ACK frame can spin the connection for
effectively forever. This is a single-packet denial-of-service with
near-infinite amplification.

**Fix:** Add a cap, e.g. `if num_blocks > MAX_ACK_RANGES { return Err(...) }`.
RFC 9000 §19.3.1 does not mandate a specific limit, but implementations
commonly cap at 256–1024 ranges.

---

### 2. Packet Number Encoding Panic on Untrusted Input

**Location:** `noq-proto/src/packet.rs:751`

```rust
pub(crate) fn new(n: u64, largest_acked: u64) -> Self {
    let range = (n - largest_acked) * 2;
    // ...
    } else {
        panic!("packet number too large to encode")
    }
}
```

Packet numbers are derived from peer-supplied ACK information. If a peer
sends a crafted ACK that shifts `largest_acked` far from the actual packet
number, this panics and crashes the process.

**Fix:** Return `Result` or `Option` instead of panicking.

---

## High

### 3. No Key Zeroization on Drop

**Location:** `noq-proto/src/crypto/` (all key types)

No usage of the `zeroize` crate found. Cryptographic keys held in
`Box<dyn PacketKey>` and similar trait objects are freed normally — the
memory may persist on the heap, be swapped to disk, or appear in core
dumps.

While TLS 1.3 derives keys from ephemeral handshake material (limiting the
window), best practice is to zeroize on drop.

**Fix:** Add `zeroize` dependency. Implement `Zeroize` (or manual
`Drop` with `ptr::write_bytes`) for key-holding types.

---

### 4. CRYPTO Stream Gap-Based Memory Exhaustion

**Location:** `noq-proto/src/connection/mod.rs:3931`

The CRYPTO buffer size check validates `end - bytes_read` against
`crypto_buffer_size` (16 KiB default), but an attacker can send
sparse CRYPTO frames with large offset gaps:

- Send offset 0–100, then offset 10000–10100
- The assembler allocates for the gap
- Repeating this with increasing offsets exhausts memory during handshake

This is reachable before the handshake completes — no authentication
required.

**Fix:** Validate that CRYPTO frame offsets do not exceed `bytes_read +
crypto_buffer_size` (not just the total range). Alternatively, reject
CRYPTO frames with offsets far ahead of the read cursor.

---

### 5. Micro-Stream Accumulation (Open/Close Cycling)

**Location:** `noq-proto/src/connection/streams/state.rs:823`

QUIC enforces concurrent stream limits, but not cumulative stream counts.
An attacker can:
1. Open 100 streams (the concurrent limit)
2. Send 1 byte on each, FIN them
3. Open 100 new streams (different IDs)
4. Repeat

Each cycle allocates `Send` and `Recv` state in `FxHashMap`s (~1 KB per
stream). Over a 30-second connection this can accumulate 10,000+ entries
and ~10 MB of state.

**Fix:** Track total streams opened per connection lifetime (not just
concurrent) and enforce a configurable limit, or implement periodic cleanup
of fully-closed stream state.

---

### 6. Incoming Connection Buffer Limits Insufficient

**Location:** `noq-proto/src/endpoint.rs:696`, `config/mod.rs:230`

Defaults allow 65,536 half-open connections (`max_incoming`) with 10 MiB
per incoming and 100 MiB total. However, the `datagrams` vector within
each `IncomingBuffer` can grow unboundedly if packets arrive faster than
the application accepts connections.

**Fix:** Enforce a per-incoming datagram count limit (not just byte limit).
Consider reducing `max_incoming` default or documenting that production
deployments should tune this.

---

## Medium

### 7. Fuzzing Coverage Gaps

**Location:** `fuzz/fuzz_targets/`

Only four fuzz targets exist:
- `packet.rs` — PartialDecode parsing
- `params.rs` — TransportParameters decoding
- `streamid.rs` — StreamId construction
- `streams.rs` — Stream state machine

**Not fuzzed:**
- Frame parsing (ACK, STREAM, CRYPTO, etc.) — this is where finding #1 lives
- Encrypted packet processing (decrypt → parse → handle)
- Handshake state machine
- Stream data reassembly (Assembler)
- Key update transitions
- Multipath frame handling

No `fuzz/corpus/` directories found, suggesting fuzzing has not been run
recently or corpus was not committed.

**Fix:** Add fuzz targets for frame parsing (highest priority given finding
#1), assembler, and handshake processing. Run them and commit corpus.

---

### 8. Panics Reachable from Untrusted Input

Several `panic!`, `unwrap()`, and `unreachable!()` calls exist on paths
reachable from network input:

| Location | Trigger |
|---|---|
| `varint.rs:70` | `VarInt::size()` on value ≥ 2^62 |
| `endpoint.rs:492` | Non-initial packet in `handle_first_packet` |
| `frame.rs:941,981,1399` | `VarInt::from_u64(...).unwrap()` on `.len()` |
| `crypto/rustls.rs:237,249` | Header encrypt/decrypt failure |

Each is a potential process crash from a crafted packet.

**Fix:** Audit all `unwrap()` / `panic!` / `unreachable!` on code paths
reachable from `Endpoint::handle()`. Replace with `Result` propagation or
`TransportError`.

---

### 9. Assembler Defragmentation as Amplification

**Location:** `noq-proto/src/connection/assembler.rs:207`

The assembler triggers defragmentation when `over_allocation > threshold`.
An attacker sending many small out-of-order fragments can keep the
over-allocation just below the threshold, then push it over to trigger
expensive sort + merge + reallocation.

This is repeatable: the defragmentation resets the counters, so the
attacker can trigger it again.

**Fix:** Rate-limit defragmentation per stream, or limit the number of
non-contiguous fragments tracked per stream.

---

### 10. Retry Token Nonce Construction (Non-Standard)

**Location:** `noq-proto/src/crypto/ring_like.rs:28-70`

Uses HKDF to derive a per-token key from a random nonce, then encrypts
with a zero nonce (`[0u8; 12]`). The code comments acknowledge this is
non-standard:

> "I suspect the original authors of this code didn't like the fact
> that it limits you to ~2^32 safe encryptions"

The construction is sound IF the HKDF output is indistinguishable from
random, but it is unusual and has not been formally analyzed.

**Fix:** Consider switching to a standard nonce-based construction with
the random value as the nonce (not derived key), or document the security
argument formally.

---

### 11. No Explicit Path Count Limit (Multipath)

**Location:** `noq-proto/src/connection/mod.rs:281`

When multipath is enabled, `paths: BTreeMap<PathId, PathState>` can grow
without explicit limit. Each path allocates a congestion controller, RTT
estimator, and loss detection state (~1 KB).

**Fix:** Enforce a configurable maximum active path count. The
`initial_max_path_id` transport parameter provides a negotiated limit, but
verify it is enforced on the receive side.

---

### 12. Windows CMSG Bounds Check Off-by-One

**Location:** `noq-udp/src/cmsg/windows.rs:32`

```rust
if unsafe { next.offset(1) } as usize > max {
```

This checks if the *next* CMSG header past the current one would fit, but
should check if `next as usize >= max` to prevent reading at the exact
boundary. Could allow reading one CMSGHDR struct past the control message
buffer on Windows.

**Fix:** Change comparison to `next as usize >= max` or
`(next as usize) + mem::size_of::<CMSGHDR>() > max`.

---

## Low

### 13. Stateless Reset Parsing Not Rate-Limited

**Location:** `noq-proto/src/endpoint.rs:279`

The stateless reset *response* is rate-limited to one per 20ms, but every
invalid-CID packet is still fully parsed and CID-validated before the rate
limit check. An attacker flooding random CIDs still consumes parsing CPU.

---

### 14. Idle Timeout Can Be Disabled

**Location:** `noq-proto/src/config/transport.rs:547`

Applications can set `max_idle_timeout` to `None`, allowing connections to
persist indefinitely. Combined with occasional keep-alive packets, this
enables slowloris-style resource exhaustion.

---

## Positive Findings

The audit also identified several areas that are well-implemented:

- **Constant-time comparison** for stateless reset tokens (`constant_time.rs`)
- **Key rotation / key phase handling** with pre-generated next keys
  (`packet_crypto.rs`)
- **TLS 1.3 enforcement** — `with_protocol_versions(&[&TLS13])` everywhere
- **VarInt decoding** is properly bounded
- **ConnectionId length validation** checked before buffer access
- **Packet length validation** with proper bounds checking
- **Property testing** with proptest and 35+ recorded regressions
- **ECN/path validation** prevents off-path packet injection

---

## Suggested PR Order

| Priority | Finding | Effort | Risk if Unpatched |
|---|---|---|---|
| 1 | Cap ACK range count (#1) | 1 hour | Remote process hang |
| 2 | Packet number panic → Result (#2) | 1 hour | Remote process crash |
| 3 | Frame parsing fuzz target (#7) | 0.5 day | Unknown unknowns |
| 4 | CRYPTO offset validation (#4) | 0.5 day | Pre-auth memory exhaustion |
| 5 | Audit all unwrap/panic on net paths (#8) | 1 day | Multiple crash vectors |
| 6 | Micro-stream lifetime tracking (#5) | 1 day | Per-connection memory leak |
| 7 | Key zeroization (#3) | 0.5 day | Key material in memory |
| 8 | Assembler fragment limit (#9) | 0.5 day | CPU amplification |
| 9 | Incoming buffer tightening (#6) | 0.5 day | Endpoint memory pressure |
| 10 | Path count limit (#11) | 0.5 day | Multipath resource growth |
| 11 | Windows CMSG fix (#12) | 1 hour | Windows buffer over-read |
