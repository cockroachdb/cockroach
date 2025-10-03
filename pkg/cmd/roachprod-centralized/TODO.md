# Issues to look into under first light load test

## Clusters registration takes too long - ✅ FIXED

Description:
- under relatively light load, `POST /clusters/register` p99 peaked at 4.85s
- p50/p95 were fast (~50-290ms), indicating tail latency issue not systematic slowness
- not sure if this is just a coincidence or not, but on one instance, the API stopped responding during a call to `POST /clusters/register` and was restarted by Cloud Run

Root Cause:
- Registration executed **3 sequential DB queries** without proper indexes:
  1. `GetSyncStatus()` - check if sync in progress
  2. `IsInstanceHealthy()` - check instance heartbeat (**full table scan on instance_health!**)
  3. `ConditionalEnqueueOperation()` - enqueue operation
- Missing index on `instance_health(instance_id, last_heartbeat)` caused full table scans
- P99 spikes correlated with instance_health table size

Fixes Applied:

1. **Atomic repository operation** - Reduced 3 queries to 1
   - Combined sync status check + health check + enqueue into single atomic SQL query
   - Inline health check using EXISTS subquery in `ConditionalEnqueueOperation()`
   - Files: [coordination.go:92-101](pkg/cmd/roachprod-centralized/services/clusters/coordination.go#L92-L101), [cockroachdb.go:461-520](pkg/cmd/roachprod-centralized/repositories/clusters/cockroachdb/cockroachdb.go#L461-L520)
   - Benefit: Eliminates race conditions, reduces network round-trips

2. **Critical database indexes** - Added via migrations
   - **`instance_health(instance_id, last_heartbeat)`** - CRITICAL for atomic health check
   - **`tasks(type, state) STORING (...)`** - For task worker polling
   - **`tasks(state, update_datetime) STORING (...)`** - For cleanup queries
   - Plus additional covering indexes for cluster_operations and migration_locks
   - Files: [health/migrations v2-3](pkg/cmd/roachprod-centralized/repositories/health/cockroachdb/migrations_definition.go), [tasks/migrations v4](pkg/cmd/roachprod-centralized/repositories/tasks/cockroachdb/migrations_definition.go), [clusters/migrations v4-5](pkg/cmd/roachprod-centralized/repositories/clusters/cockroachdb/migrations_definition.go)

Expected Impact:
- **Before**: 60-120ms typical, p99 up to 4.85s
- **After**: 2-5ms with indexes, p99 <10ms consistently
- **Improvement**: ~95%+ reduction in registration latency

## Clusters background sync timed out and did not release the lock - ✅ FIXED

Description:
- a background clusters sync task timed out and did not release the distributed sync lock
- this generated a situation where no instance was syncing because the lock was still held by a healthy instance

Logs:
- 2025-10-10T04:49:54.947016735Z DEBUG storing clusters and releasing sync lock
- 2025-10-10T04:49:54.947046649Z DEBUG atomically storing clusters and releasing sync lock
- 2025-10-10T04:49:55.067889613Z ERROR Task processing timed out
- 2025-10-10T04:49:55.067961617Z ERROR Unable to process task: Unable to process task
- 2025-10-10T04:49:55.068037091Z ERROR failed to release sync lock (defer): failed to release sync lock: context deadline exceeded
- 2025-10-10T04:49:55.083402589Z ERROR unable to process task: task processing timeout

Fix applied:
1. Lock release now uses background context (30s timeout) instead of cancelled context
   - File: [sync.go:40-57](pkg/cmd/roachprod-centralized/services/clusters/sync.go#L40-L57)
   - This ensures lock is always released even when task times out
2. Lock acquisition now auto-initializes the sync state row if missing (UPSERT)
   - File: [cockroachdb.go:332-387](pkg/cmd/roachprod-centralized/repositories/clusters/cockroachdb/cockroachdb.go#L332-L387)
   - Enables easier manual recovery without needing to manually initialize DB state
3. Migration savepoint rollback also fixed with background context
   - File: [migrator.go:202-221](pkg/cmd/roachprod-centralized/utils/database/cockroachdb/migrator.go#L202-L221)
   - Prevents savepoint rollback failures on migration timeouts

Potential future improvement:
- Add lock age timeout as additional safeguard against pathologically slow operations
- Current protection: heartbeat-based health check (handles crashed/hung instances)
- Gap: healthy instance holding lock for abnormally long time (e.g., 30+ minutes)
- Proposed: Add `started_at < now() - $MAX_LOCK_AGE` check in lock acquisition query
- Recommended timeout: 15-30 minutes (generous to avoid interrupting legitimate operations)
- Benefit: Bounded worst-case recovery time even for extreme edge cases
- Priority: Low (current defenses are sufficient for normal operation)

## DNS update tasks failure - ✅ FIXED

Description:
- Two types of DNS task failures observed:
  1. **Incremental update races**: `manage_records` tasks fail with 409 (already exists) or fail when full sync already applied changes
  2. **Full sync 404 errors**: `public_dns_sync` fails when trying to delete records that were already deleted externally

Root Causes:
1. **Race between incremental and full sync operations** - manage_records and public_dns_sync can collide
2. **Non-idempotent DNS operations** - gcloud fails on "already exists" (409) and "not found" (404)
3. **Unnecessary DNS task creation** - Tag updates triggered DNS tasks even when IPs unchanged (~66% of tasks)

Fixes Applied:

1. **Smart DNS diffing** - Eliminated unnecessary DNS tasks (66% reduction)
   - File: [coordination.go:148-183](pkg/cmd/roachprod-centralized/services/clusters/coordination.go#L148-L183)
   - Compare old vs new cluster DNS→IP mappings before creating tasks
   - Skip task creation when `len(createRecords) == 0 && len(deleteRecords) == 0`
   - Properly detect VM replacements (different DNS names)
   - Impact: 500 RegisterClusterUpdate calls → only ~130 actual DNS tasks (mostly tag updates had no IP changes)

2. **Idempotent CreateRecords** - Handle 409 conflicts gracefully
   - File: [gce/dns.go:443-478](pkg/roachprod/vm/gce/dns.go#L443-L478)
   - New helper: `executeRecordSetCommand()` with automatic retry on 409
   - When create fails with "already exists" → auto-retry with update
   - Eliminates race between lookup and create operations

3. **Idempotent DeleteRecords** - Continue on 404 errors
   - File: [gce/dns.go:245-252](pkg/roachprod/vm/gce/dns.go#L245-L252)
   - Treat 404/not found as success (record already gone)
   - Process entire batch instead of failing on first missing record
   - Handles external deletions gracefully

4. **Retry full sync on 404** - Handle external deletion races
   - File: [public_dns.go:185-212](pkg/cmd/roachprod-centralized/services/public-dns/public_dns.go#L185-L212)
   - Detect 404 errors from `gcloud --delete-all-existing`
   - Single immediate retry (by then, deleted records excluded from list)
   - Handles case where records deleted between list and delete operations

Expected Impact:
- **Before**: ~760 DNS tasks per load test run, frequent 409/404 failures
- **After**: ~260 DNS tasks (only actual changes), all operations idempotent
- **Task reduction**: 66% fewer DNS tasks
- **Reliability**: No more 409 or 404 DNS failures

## Cluster already exists at registration - ✅ DONE

Description:
- since the cluster is created externally before being registered to the API, if a background clusters sync happens between the external creation and the registration, the API might already know about it and will return a 409 cluster already exists.

Fix:
- pre-register an empty cluster before externally creating it
- update the cluster with its final state at the end of the creation process

## Tasks picked up late

Description:
- a few tasks have been picked up late (~5 minutes after creation) under light load

Context:
- a single instance with 5 workers was processing the tasks
- some maintenance tasks (e.g. health_cleanup) were running every minute and creating load on the system

Fix considered:
- increase the interval for maintenance tasks (e.g. health should run every hour) - ✅ DONE
- increase the number of workers