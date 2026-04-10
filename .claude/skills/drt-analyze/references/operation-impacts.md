# Operation-to-Expected-Impact Mapping

Use this table during temporal correlation (Step 3a) to determine whether a
metric finding is an expected side-effect of a disruptive operation.

A finding is "expected" only if:
1. Its START time falls within [op_start, recovery_end]
2. Its metric appears in the operation's expected impact set below
3. It recovered within the recovery window

If the metric is NOT in the expected set, do not attribute it to the operation.

## Impact Table

| Operation | Expected Metric Impacts |
|-----------|------------------------|
| `network-partition/*` | ranges.unavailable, ranges.underreplicated, heartbeatlatency, heartbeatfailures, sql.service.latency, sql.failure, txn.restarts, requests.slow.raft, rpc.connection.unhealthy, kv.replica_circuit_breaker.num_tripped_replicas, storage.wal.fsync.latency, changefeed.commit.latency |
| `disk-stall/dmsetup` | storage.write.stalls, sql.service.latency, sql.failure, txn.restarts, disk.iopsinprogress, storage.wal.fsync.latency, admission.io.overload, requests.slow.raft, l0-sublevels |
| `license-throttle` | sql.service.latency spike, sql.failure, txn.restarts, QPS drop |
| `resize/*` | ranges.underreplicated, livenodes, kv.prober.write.failures, kv.prober.read.failures, sys.goroutines, heartbeatlatency, sys.uptime |
| `backup-restore/*` | jobs.backup.currently_running change, jobs.restore.currently_running change, capacity.used growth, storage.wal.fsync.latency spikes, disk.iopsinprogress, AmbiguousResultError in logs |
| `add-column`, `add-index` | jobs.schema_change.currently_running, schema_change.currently_paused, sql.service.latency (minor), command-too-large errors on wide tables |
| `*-changefeed-job` | changefeed.* metric changes |
| `pause-job/*` | jobs.*.currently_running drop, currently_paused increase |
| `cancel-job/*` | jobs.*.currently_running drop |
| `manual-compaction` | disk.iopsinprogress spike, rocksdb.read.amplification, sql.service.latency (can cause near-total SQL drop), l0-sublevels, io.overload |
| `cluster-settings/scheduled/*` | varies |
