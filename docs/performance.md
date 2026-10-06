# Measured workloads and finite defaults

Measurements below were taken on 2026-10-06. Proxy/workspace measurements use
Go 1.26.4 darwin/arm64; standalone aznet SDK measurements use its selected
Go 1.26.5 toolchain on the same host. They
separate in-process multiplexing, local Azurite 3.34.0, and real Azure. Process
heap samples include both endpoints and test payloads, not just tunnel-owned
memory. Allocated bytes measure churn; sampled peaks are not exact maxima.

## Multiplexing baseline

Three repeats per combination: receive windows 128 KiB/512 KiB/1 MiB, concurrency
1/16/64/128, idle/interactive/bulk/stalled-consumer workloads. Interactive sends
20 × 256 bytes per stream with a reply; bulk sends 4 × 256 KiB. In stalled mode all
but one consumer stop reading; the remaining stream must finish all 20 round trips.
No race instrumentation is used for these timings. Race regressions run separately.

At 128 streams, maximum observed heap delta and range of per-run p95 latency:

| Window | Idle heap MiB | Interactive p95 ms | Bulk heap MiB | Healthy p95 with stalled peers ms |
|---|---:|---:|---:|---:|
|128 KiB|34.50|1.86–2.12|240.35|1.74–2.34|
|512 KiB|130.45|1.68–1.72|450.94|1.92–2.90|
|1 MiB|258.50|1.61–1.66|679.64|5.23–9.34|

The stalled 512 KiB workload peaked at 473.77 MiB including test-owned source
payloads, both endpoints' rings/queues and transient allocations; this is not
an assertion that a 64 MiB tunnel budget bounds the whole process. Idle 128 streams
already reserve 64 MiB of receive memory at each of two endpoints. Operators
with lower memory limits should reduce both maximum streams and tunnel budget.

A separate fixed 10 ms-per-transport-write model transfers 256 KiB per stream.
At 16 streams, 64 KiB batches took 1.520–1.528 s / 146 writes, 128 KiB took 0.524–0.530 s / 53 writes,
and 512 KiB took 0.124–0.138 s / 12–16 writes. These are model writes, not Azure SDK
attempts or billed transactions. Single-stream 512 KiB batches took 22–35 ms.

Retain 512 KiB stream windows: doubling adds substantial memory without a clear
latency benefit, while 128 KiB raises bulk credit-turnover pressure. Retain 512 KiB
batches for bounded batching efficiency, 32 KiB DATA frames for scheduling granularity,
64 MiB per-endpoint RX reservation and a separate 64 MiB TX budget, 128 streams,
512 control slots and 5 s drain timeout. These are configurable operating limits,
not optimal settings for every deployment. Five seconds remains the finite
cleanup allowance validated by lifecycle tests, not a flow-control timeout.

UDP retains 256 KiB/64 complete queued packets and 64 resolved destinations per
association; association admission shares the stream limit. Saturation drops
whole UDP packets; TCP senders wait for credit. The live pressure case sends
100 × 32 KiB packets without reading while healthy traffic progresses; it does
not establish an exact drop rate. No buffer pooling change is justified by
these results alone.

## SDK request and emulator baseline

The existing aznet SDK workload measures all 3 drivers at 1/4/16 sessions, separating
setup/teardown from operation counters. With the directional Blob-lock candidate
`aa3fec86d61941ac5582c20d2122606551b6eaf7`, all 36 cases passed against Azurite.
At 16 sessions:

| Driver | Workload | MiB/s | p95 ms | Peak heap delta MiB | SDK attempts |
|---|---|---:|---:|---:|---:|
|Blob|idle|0|—|2.65|174|
|Blob|interactive|0.0536|45.129|7.59|1040|
|Blob|bulk|98.94|61.751|32.15|144|
|Blob|slow|9.60|59.896|7.72|144|
|Queue|idle|0|—|2.15|174|
|Queue|interactive|0.0432|51.289|6.47|1552|
|Queue|bulk|17.32|276.523|52.81|848|
|Queue|slow|9.65|72.379|18.77|208|
|Table|idle|0|—|2.31|171|
|Table|interactive|0.0985|33.012|7.67|1040|
|Table|bulk|32.00|283.039|82.23|144|
|Table|slow|10.30|66.161|30.70|144|

These are separate transport sessions, not multiplexed streams. The emulator
workload deliberately uses 1 ms fast / 10 ms idle polling; deployed defaults remain
10 ms / 500 ms. SDK attempts include empty polls and deletes but exclude session
setup/teardown for this table. Payload bytes are counted separately. Emulator
throughput is neither Azure throughput nor a price ranking.

## Reproduction and regression decisions

```sh
PROXYBLOB_MEASURE=1 GOWORK=off go test -mod=readonly ./pkg/protocol -run '^TestFlowWorkloads$' -count=3 -v
PROXYBLOB_MEASURE_LATENCY=1 GOWORK=off go test -mod=readonly ./pkg/protocol -run '^TestFlowStorageLatency$' -count=3 -v
```

Use PROXYBLOB_MEASURE_WINDOW for a decimal-byte alternate window. The workload
reports elapsed time, p50/p95, transferred bytes, sampled heap and allocation
counts. Reproduce SDK cases in aznet with AZNET_MEASURE=1 against Azurite.
Use live harness PROXYBLOB_LIVE_GOWORK=/absolute/go.work for an explicit candidate
workspace; default off still builds the committed published dependency readonly.
Always record exact revisions and separate those results.

Correctness gates remain byte identity, ordered EOF, healthy progress, exact
credit/aggregate bounds and cleanup. For a matching host/toolchain/workload,
use the observed upper ranges above as investigation thresholds: rerun at least
three times if exceeded, and compare distributions before changing defaults.
They are measured baseline envelopes, not user SLOs or brittle CI timing asserts.
Do not accept a speed gain that drops accepted TCP bytes, starves controls,
unbounds memory or increases SDK attempts without documenting the tradeoff.

All measured cases, including intermediate concurrency and allocation counts,
are retained in [baseline.json](../tests/performance/baseline.json).

## Same-account live Blob comparison

Alternated published aznet 9e6683d and candidate aa3fec8 twice against the same
Blob account, unchanged polling and flow limits. Each run completed 3 BIND
conversations, 3 public DNS queries and 45 UDP echo cases, then independent Azure
catalog checks found zero leftover resources. The client had no direct agent
or resolver access. The retained deterministic SDK test separately proves
that a held read no longer blocks an independent append, and vice versa.

| Round | Published echo median ms | Candidate echo median ms | Published DNS A/AAAA/NXDOMAIN ms | Candidate DNS ms |
|---|---:|---:|---|---|
|1|974|481|969/1132/1283|602/739/601|
|2|995|483|1165/1010/884|1047/1050/1044|

Echo latency approximately halved in both samples. DNS did not improve
consistently, so do not promise a uniform speedup: polling, service latency
and scheduling remain. These runs were not a bulk-download benchmark or
an HTTP-duration profile. The changed dependency is integrated separately;
workspace success alone is not evidence for a published release.

## Separate lock, SDK and polling profile

A further same-account pair used temporary atomic count/duration probes against
aznet `9e6683d` and merged `7abcd80a7a2820d69e55deb1fc62f8b5a601f4be`.
Both completed the same BIND/DNS/UDP matrix and independent zero-residue checks.
These are instrumented whole-run totals, including cleanup, with one process
per endpoint. Repeated close snapshots are counted once per endpoint. The probes
are diagnostic only and are not included in the shipped dependency.

| Measurement | Before proxy / agent | After proxy / agent |
|---|---|---|
| Read mutex wait, total s |22.759 / 24.854|0.000241 / 0.000255|
| Append mutex wait, total s |0.264 / 0.239|0.000039 / 0.000093|
| Body-offset mutex wait, total s |1.006 / 0.000038|0.000183 / 0.000085|
| DownloadStream headers, mean ms |126.70 / 119.62|129.53 / 125.55|
| AppendBlock SDK call, mean ms |214.11 / 222.62|228.79 / 216.61|
| Response-body reads, total s |0.381 / 0.243|0.264 / 0.394|
| Poll waits, total s |13.120 / 15.111|11.752 / 13.040|

Mutex probes bracket acquisition; SDK probes bracket the SDK call (including any
internal retry); body probes bracket each response-body read; polling probes
bracket `Conn.idleWait`. Counts and nanosecond totals are retained in baseline.json.
Direction and endpoint durations overlap: do not sum this table into request RTT
or infer a per-DNS latency allocation. This single diagnostic pair supports the
lock-contention explanation; SDK calls still cost roughly 120–230 ms each and
polling still contributes. It does not establish a general Azure latency SLO.

The first profile pilot completed traffic but exposed a harness cleanup omission:
`context.DeadlineExceeded` from bounded best-effort FIN was not accepted alongside
other expected close outcomes. The harness now accepts that specific leaf error;
other failures remain failures. The pilot is excluded from the accepted pair.
The separate published-dependency integration passed the full live matrix for
Blob, Queue and Table; see the dependency update PR #23.
