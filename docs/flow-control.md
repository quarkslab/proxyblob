# Multiplexed stream flow control

Proxy and agent must be upgraded together. Version 3 retains NEW and ACK framing introduced in version 2:
a 4-byte big-endian version (`3`) and an 8-byte big-endian receive-window
size. Empty legacy handshakes and unsupported versions terminate the tunnel
with an explicit negotiation error in the log. Identity framing is unchanged.
No backward identity sniffing or aznet Driver/Transport change is involved.

Each endpoint reserves a fixed byte ring before advertising its window. All
outstanding grants, including unused credit on idle streams, count against the
aggregate reservation. Admission is limited by both stream count and aggregate
bytes. Excess **new** streams are refused; admitted slow streams pause without
reset or transfer timeout. Reservations are not dynamically borrowed between
streams, so one slow consumer cannot absorb another stream's allowance.

DATA consumes the sender's credit before encoding or queueing. CREDIT (command
6) carries an 8-byte big-endian cumulative count of bytes consumed by the peer.
The sender's remaining allowance is `window - (sent - consumed)`. Duplicate and
older counts are ignored; counts beyond bytes sent, malformed updates and
counter overflow fail the stream. Updates for disposed streams are ignored.
Stream UUIDs must never be reused within a tunnel (the proxy generates random
UUIDs). Counters never wrap.

Read copies bytes out of the reserved ring before returning credit. There is
no delivery worker or second receive queue. A sender at zero credit waits for
consumption, close, cancellation or handler draining. DATA beyond a reservation
or following EOF is a protocol violation. FIN/EOF is ordered after confirmed
DATA writes; the opposite direction remains usable. Full CLOSE bounds draining
and reports forced abort as an error, not clean EOF.

## Scheduling and limits

TCP forwarding pipelines socket reads while storage writes are pending. DATA
queues rotate round-robin between ready streams, preserving each stream's byte
order. Direct `ProtocolConn.Write` remains transport-confirmed. Forwarding
reports success only after its final DATA record is confirmed, before sending
EOF; a late upload failure interrupts even an idle source socket. Cancellation
discards unsent DATA before CLOSE. Control records use separate bounded capacity. Credit updates coalesce to one pending record per stream. Small reads accumulate
until half the window is released, or the advertised grant has been fully
received. The exhausted-grant case flushes even one released byte, both when
Read consumes data and when the last in-flight DATA arrives; a zero-credit
peer never waits for more consumption or a timer. Unadvertised consumption is
excluded from receive admission. At most eight controls are selected ahead of a
ready DATA frame. Batch collection stops at the byte limit or one millisecond;
there is no intentional coalescing delay. Short writes advance by the reported
count, while zero progress or an error terminates the tunnel without replay.
Transport stalls still delay all traffic: this policy prevents application
consumers from blocking dispatch, not a failed storage transport from blocking.

Both commands accept these environment variables. Embedders can use
`NewBaseHandlerWithConfig`, `NewProxyServerWithConfig`, or
`NewSocksHandlerWithConfig`. Byte/count values are decimal integers; durations
use Go syntax. Invalid or internally inconsistent limits are rejected.

| Variable | Default | Purpose |
| --- | ---: | --- |
| `PROXYBLOB_STREAM_WINDOW` | 524288 | Receive reservation and separate outgoing payload cap per stream |
| `PROXYBLOB_TUNNEL_WINDOW` | 67108864 | Receive budget and separate outgoing payload budget per endpoint |
| `PROXYBLOB_MAX_STREAMS` | 128 | Concurrent admitted streams |
| `PROXYBLOB_DATA_FRAME` | 32768 | Maximum outgoing DATA payload |
| `PROXYBLOB_BATCH_BYTES` | 524288 | Maximum encoded transport write batch |
| `PROXYBLOB_CONTROL_SLOTS` | 512 | Pending control records; at least four per stream |
| `PROXYBLOB_DRAIN_TIMEOUT` | 5s | Tunnel/full-close graceful drain, followed by abort |

The receive budget is not a total process heap cap. Outgoing DATA is separately
bounded by the stream and tunnel byte limits above, counting both queued and
in-flight records until transport completion. Outgoing capacity exhaustion
pauses admission. Metadata is additionally capped at 64 DATA records per stream
and `64 * MaxStreams` per tunnel, including cancelled streams still in flight.
Each active forwarding producer owns one scratch frame outside those budgets;
the writer owns one encoded batch. Thus the defaults allow 64 MiB of receive
rings and a separate 64 MiB of outstanding outgoing payload per endpoint, plus
at most 4 MiB of producer scratch, one 512-KiB batch and bounded record/header
metadata. Allocator rounding and unreclaimed garbage add heap overhead. Control
payloads are at most 259 bytes each with UDP setup records.
See [UDP tunneling](udp-tunnel.md) for its separate drop policy and additional
finite receive queues. Framing retains at most one incomplete
1-MiB record plus a 64-KiB read chunk and the current decoded record. Socket,
application, aznet, allocator and GC overhead are outside the ring budget.
A control flood that exhausts reserved control capacity aborts the tunnel;
ordinary zero-credit streams neither fill that queue nor cause resets.

## Earlier measurements (superseded defaults)

Native runtime measurements on darwin/arm64, Go 1.26.4, using two endpoints over
`net.Pipe`, with the committed published dependency. These are local multiplexing
measurements, not Azure throughput, billing, WASM performance, or an SLA.
`TestFlowWorkloads` is opt-in with `PROXYBLOB_MEASURE=1`; use
`PROXYBLOB_MEASURE_WINDOW` to repeat the 16/64/256-KiB comparison. Normal tests
assert deterministic bounds and progress rather than timing thresholds.

Workloads: idle reservations; 20 request/ack exchanges of 256 bytes per stream;
four 256-KiB bulk transfers per stream; and all but one consumer stalled while
the healthy stream completes 20 interactive exchanges. Peak heap is sampled at
1 ms plus an ending sample, relative to a GC baseline, across **both** endpoints
and test application buffers. It includes unreclaimed allocations and can miss
sub-millisecond peaks. Latency is application exchange latency, not packet RTT.

64-KiB window results with 32-KiB DATA frames and 128-KiB batches
(heap deltas in MiB; p95 shown with units):

| Streams | Idle heap | Interactive heap / p95 | Bulk heap / p95 | Slow heap / healthy p95 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3.03 | 5.02 / 376.167µs | 6.57 / 1.108875ms | 6.29 / 209.834µs |
| 16 | 7.68 | 9.72 / 768µs | 26.33 / 4.294375ms | 12.96 / 226.917µs |
| 64 | 14.44 | 20.24 / 1.1525ms | 92.18 / 13.293166ms | 34.19 / 757.459µs |
| 128 | 26.41 | 35.48 / 1.885458ms | 177.77 / 27.758084ms | 67.95 / 1.498917ms |

At 128 streams, the 16/64/256-KiB windows completed 128 MiB of bulk transfers in
115.437125ms/88.480083ms/88.318708ms, respectively.
Idle heap deltas were 12.37/26.41/81.53 MiB.
The initial 64-KiB receive reservation balanced window headroom with bounded memory;
128 streams reserve exactly 8 MiB per endpoint. Local CPU/heap results alone do
not establish performance under storage latency.

A user-reported Blob slowdown exposed that limitation. A real-TCP SOCKS transfer
of 530,000 bytes over a transport with 50 ms added per Write took about 1.29 s
before flow control, 2.26 s with the initial 16-KiB frames, and 1.38–1.42 s after
this correction. A normal 32-KiB io.Copy write no longer becomes two sequential
storage writes, and small SOCKS reads no longer each generate credit traffic.
The 64-KiB window still contains two DATA frames; zero-credit resume remains
covered for single-byte reads and delayed final DATA.

The batch comparison also models request cost: with 10 ms per transport Write,
16 concurrent 256-KiB streams completed in 1.439 s / 258 transport writes with
64-KiB batches, versus 0.507 s / 91 writes with 128-KiB batches. A 64-KiB encoded
batch cannot hold two 32-KiB payloads plus headers. The initial 128-KiB bound
fits several frames and control records without unbounded collection. The
single-stream result stayed near 100 ms in both cases. These are simulated
transport costs, not measured Azure request counts or billed operations.

Repeat the end-to-end latency probe with `PROXYBLOB_LATENCY_MS=50 go test -v
-run TestStorageLatencyTransfer ./pkg/proxy/server`, and the concurrent batch
comparison with `PROXYBLOB_MEASURE_LATENCY=1 go test -v -run
TestFlowStorageLatency ./pkg/protocol`. Native baseline/after logs remain in the
local ticket evidence. Actual Blob behavior still needs the user's retest.

512 control slots allow one coalesced credit and setup/EOF/close capacity per
stream. The 5-second drain allowance exceeds measured exchanges while bounding
cleanup; it remains configurable for real storage latency. These defaults
remain subject to separate storage measurement work.

## Historical throughput regression

The initial comparison above used main after PR #13. It therefore missed a
larger regression introduced by synchronous per-DATA transport confirmation.
A subsequent identical fixed-length SOCKS exchange (1 MiB request plus 1 MiB
response, verified byte-for-byte, 50 ms added per transport Write) measured:

| Revision | Elapsed | Transport writes |
| --- | ---: | ---: |
| Before receive-buffer reduction (`334be3f`) | 0.581 s | 11 |
| Before synchronous confirmation (`2cf9f29`) | 0.579 s | 11 |
| After confirmation (`af282d6`) | 3.644 s | 71 |
| Current flow-control data path (`952d741`) | 3.845 s | 137, including credit |

Before #13, a producer queued more data during an upload, allowing batches
approaching 1 MiB in this probe. After #13, each 32-KiB copy waits for its
transport write before admitting the next copy, preventing single-stream
batching. Disabling only that confirmation wait in a disposable `af282d6`
checkout restored 0.577 s / 11 writes; the diagnostic edit was then reverted.
The receive-buffer change showed no corresponding regression in this probe.
These results isolate a mechanism under simulated storage cost, not actual
Azure throughput or billing.

The bounded pipeline now corrects this serialization mechanism without removing
confirmation from direct Write or the forwarding completion barrier. The larger
finite windows below address the remaining credit round-trip limitation. This
does not promise the earlier unbounded implementation's throughput in every
workload or establish live Blob performance.

## Pipeline measurements and current defaults

`TestStorageFixedTransfer` checks a real TCP SOCKS flow with 1 MiB request and
1 MiB response, byte identity and an application acknowledgement. Set
`PROXYBLOB_LATENCY_MS=50` to model 50 ms per transport Write. On darwin/arm64,
Go 1.26.4 with the committed published dependency, the pre-pipeline revision
`6e7f6dc` took 3.840 s and 137 writes. The pipeline with a 512-KiB receive window
and 512-KiB batch took 1.148–1.203 s and 28–29 writes in three runs. A 1-MiB
window took 0.839–0.840 s and 18 writes, but doubled receive reservations.
These are transport calls, not Azure requests or billed operations. The
pre-#13 unbounded baseline was faster still (0.579 s); finite credit and batch
limits deliberately retain backpressure.

The 64-KiB window remained a bottleneck after pipelining. The 512-KiB choice
allows sixteen normal copy frames in flight; the 512-KiB encoded batch allows
multiple frames to share an upload while retaining bounded control latency.
The 64-MiB tunnel receive budget admits 128 such windows. Smaller deployments
can lower the tunnel budget and stream count; a smaller window reduces memory
but also throughput on transports with high request latency. The DATA frame
stays 32 KiB, preserving scheduling granularity.

`TestFlowWorkloads` now exercises the forwarding pipeline with finite in-memory
sources. With the current defaults, the same workload definitions and heap
sampling method described above produced (MiB across both endpoints and test
buffers; p95 is exchange latency):

| Streams | Idle heap | Interactive heap / p95 | Bulk heap / p95 | Slow heap / healthy p95 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3.15 | 3.71 / 256.667µs | 6.54 / 280.5µs | 3.83 / 50.042µs |
| 16 | 18.19 | 29.00 / 409.042µs | 60.22 / 4.192166ms | 59.26 / 444µs |
| 64 | 66.27 | 99.76 / 1.036375ms | 227.56 / 15.004791ms | 236.92 / 1.626541ms |
| 128 | 130.41 | 209.41 / 1.883375ms | 445.65 / 28.2925ms | 473.69 / 3.043709ms |

At 128 streams, 256/512/1024-KiB windows showed sampled slow-workload heap
of 236.85/473.69/862.20 MiB and healthy p95 of 1.43625ms/3.043709ms/9.673375ms.
The harness allocates both endpoints and 2-window source payloads for stalled
streams; these values are not per-agent retained memory. The 512-KiB choice
trades higher finite memory than the original defaults for substantially fewer
storage round trips, without the full memory cost of 1-MiB windows. Live Blob
throughput remains a rollout check; native in-memory and latency-model evidence
cannot substitute for it.

## Rollout and validation scope

Drain existing tunnels before deploying matching proxy/agent binaries. Mixed
versions are rejected on logical-stream setup. The committed aznet pin remains
unchanged: publishing and pinning the merged aznet integrity/buffering changes
is separate dependency integration work. Native flow regressions and WASM builds
do not establish production JS-host or live Azure behavior. UDP overload policy,
JS buffer/callback cleanup, organization, and final release validation remain
separate work.

### Explicit agent removal

`agent rm` clears the local selection even if session cleanup returns an error.
Protocol abort first interrupts transport I/O. Session cleanup waits for the
protocol writer to exit before replacing its expired write deadline with a
finite shutdown deadline, allowing aznet to submit FIN without resuming an
interrupted protocol write. Ordinary cancellation does not produce a drain
warning; actual drain deadlines and transport failures remain visible.

The removal regression uses the selected aznet implementation and a fake raw
storage driver, and checks decrypted FIN submission under concurrent close.
It does not establish remote process exit over Azure. Live Blob removal and
remote termination remain rollout checks; transport Close is bounded and may
still report genuine delivery or cleanup failure.
