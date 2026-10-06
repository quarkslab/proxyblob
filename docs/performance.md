# Measured workloads and finite defaults

Measurements below were taken on 2026-10-06 with Go 1.26.4 darwin/arm64. They
separate in-process multiplexing, local Azurite 3.34.0, and real Azure. Process
heap samples include both endpoints and test payloads, not just tunnel-owned
memory. Allocated bytes measure churn; sampled peaks are not exact maxima.

## Multiplexing baseline

Three repeats per combination: receive windows 128KiB/512KiB/1MiB, concurrency
1/16/64/128, idle/interactive/bulk/stalled-consumer workloads. Interactive sends
20×256bytes per stream with a reply; bulk sends4×256KiB. In stalled mode all
but one consumer stop reading; the remaining stream must finish all 20 round trips.
No race instrumentation is used for these timings. Race regressions run separately.

At 128 streams, maximum observed heap delta and range of per-run p95 latency:

| Window | Idle heap MiB | Interactive p95 ms | Bulk heap MiB | Healthy p95 with stalled peers ms |
|---|---:|---:|---:|---:|
|128KiB|34.50|1.86–2.12|240.35|1.74–2.34|
|512KiB|130.45|1.68–1.72|450.94|1.92–2.90|
|1MiB|258.50|1.61–1.66|679.64|5.23–9.34|

The stalled512KiB workload peaked at473.77MiB including test-owned source
payloads, both endpoints' rings/queues and transient allocations; this is not
an assertion that a64MiB tunnel budget bounds the whole process. Idle128streams
already reserve64MiB of receive memory at each of two endpoints. Operators
with lower memory limits should reduce both maximum streams and tunnel budget.

A separate fixed10ms-per-transport-write model transfers256KiB per stream.
At 16 streams,64KiB batches took1.520–1.528s/146writes,128KiB took0.524–0.530s/53writes,
and512KiB took0.124–0.138s/12–16writes. These are model writes, not Azure SDK
attempts or billed transactions. Single-stream512KiB batches took22–35ms.

Retain512KiB stream windows: doubling adds substantial memory without a clear
latency benefit, while128KiB raises bulk credit-turnover pressure. Retain512KiB
batches for bounded batching efficiency,32KiB DATA frames for scheduling granularity,
64MiB per-endpoint RX reservation and a separate64MiB TX budget,128streams,
512control slots and5s drain timeout. These are configurable operating limits,
not optimal settings for every deployment. Five seconds remains the finite
cleanup allowance validated by lifecycle tests, not a flow-control timeout.

UDP retains256KiB/64complete queued packets and64resolved destinations per
association; association admission shares the stream limit. Saturation drops
whole UDP packets; TCP senders wait for credit. The live pressure case sends
100×32KiB packets without reading while healthy traffic progresses; it does
not establish an exact drop rate. No buffer pooling change is justified by
these results alone.

## SDK request and emulator baseline

The existing aznet SDK workload measures all 3 drivers at1/4/16sessions, separating
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
workload deliberately uses1ms fast/10ms idle polling; deployed defaults remain
10ms/500ms. SDK attempts include empty polls and deletes but exclude session
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

Alternated published aznet9e6683d and candidateaa3fec8 twice against the same
Blob account, unchanged polling and flow limits. Each run completed3BIND
conversations,3publicDNSqueries and45UDPecho cases, then independent Azure
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
