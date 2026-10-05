# Multiplexed stream flow control

Proxy and agent must be upgraded together. Version 2 changes NEW and ACK to
carry a 4-byte big-endian version (`2`) and an 8-byte big-endian receive-window
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

A stream has at most one pending DATA frame. FIFO admission, followed by waiting
for the batch's transport result, places sustained producers behind other ready
streams. Control records use separate bounded capacity. Credits coalesce to one
pending update per stream; a zero-credit peer does not depend on a size threshold
or timer to receive its update. At most eight controls are selected ahead of a
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
| `PROXYBLOB_STREAM_WINDOW` | 65536 | Receive bytes reserved per stream/direction |
| `PROXYBLOB_TUNNEL_WINDOW` | 8388608 | Receive reservation budget per endpoint/tunnel |
| `PROXYBLOB_MAX_STREAMS` | 128 | Concurrent admitted streams |
| `PROXYBLOB_DATA_FRAME` | 16384 | Maximum outgoing DATA payload |
| `PROXYBLOB_BATCH_BYTES` | 65536 | Maximum encoded transport write batch |
| `PROXYBLOB_CONTROL_SLOTS` | 512 | Pending control records; at least four per stream |
| `PROXYBLOB_DRAIN_TIMEOUT` | 5s | Tunnel/full-close graceful drain, followed by abort |

The receive budget is not a total process heap cap. Outgoing DATA is separately
bounded by one frame per admitted producer and the writer's batch. Control
payloads are at most 12 bytes each. Framing retains at most one incomplete
1-MiB record plus a 64-KiB read chunk and the current decoded record. Socket,
application, aznet, allocator and GC overhead are outside the ring budget.
A control flood that exhausts reserved control capacity aborts the tunnel;
ordinary zero-credit streams neither fill that queue nor cause resets.

## Measurements behind the defaults

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

64-KiB window results (heap deltas in MiB, p95 in ms):

| Streams | Idle heap | Interactive heap / p95 | Bulk heap / p95 | Slow heap / healthy p95 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2.65 | 5.29 / 0.264 | 6.51 / 1.183 | 5.29 / 0.261 |
| 16 | 5.80 | 8.09 / 0.845 | 25.25 / 5.806 | 9.96 / 0.197 |
| 64 | 13.46 | 19.20 / 1.313 | 86.61 / 14.963 | 37.20 / 0.568 |
| 128 | 25.23 | 35.58 / 1.763 | 168.90 / 28.536 | 62.25 / 1.322 |

At 128 streams, the 16/64/256-KiB windows completed 128 MiB of bulk transfers in
118.5/99.4/101.5 ms, respectively. Idle heap deltas were 10.57/25.23/75.30 MiB;
slow-workload heap deltas were 17.19/62.25/210.27 MiB. The 64-KiB choice improved
bulk completion over 16 KiB without the 256-KiB memory cost; 256 KiB showed no
bulk benefit in this run. The 128-stream ceiling is the highest measured
concurrency, with 8 MiB of receive reservations per endpoint. Four 16-KiB credit
quanta per window and a 64-KiB batch limit preserve short scheduling turns.
512 controls allow one coalesced credit and setup/EOF/close capacity per stream.
The retained 5-second drain allowance greatly exceeds measured p95 exchanges
while bounding cleanup; it is configurable for real storage latency. These are
conservative finite defaults, subject to the separate storage measurement work.

## Rollout and validation scope

Drain existing tunnels before deploying matching proxy/agent binaries. Mixed
versions are rejected on logical-stream setup. The committed aznet pin remains
unchanged: publishing and pinning the merged aznet integrity/buffering changes
is separate dependency integration work. Native flow regressions and WASM builds
do not establish production JS-host or live Azure behavior. UDP overload policy,
JS buffer/callback cleanup, organization, and final release validation remain
separate work.
