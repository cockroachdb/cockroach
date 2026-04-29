# Tenant-level branching in CockroachDB

## Overview

Tenant-level branching lets you create a virtual cluster that starts as a copy
of another virtual cluster's data, without copying anything. The new cluster
sees all of the parent's data at the moment of branching and accumulates its
own writes from there. The parent doesn't know about the branch and is
unaffected by it.

The SQL surface looks like:

```sql
CREATE VIRTUAL CLUSTER prod;
ALTER VIRTUAL CLUSTER prod START SERVICE SHARED;
-- populate prod...

CREATE VIRTUAL CLUSTER dev BRANCH FROM prod;
ALTER VIRTUAL CLUSTER dev START SERVICE SHARED;
-- dev now sees prod's data; writes against dev are isolated.
```

Branch creation is O(1) and takes well under a second regardless of how much
data the parent holds. Subsequent writes against the branch land in the
branch's own keyspace; reads against the branch see a merged view of the
branch's writes layered over the parent's pre-branch state.

This is a working PoC, not a production design. In scope:

- One level of branching (a branch from a regular tenant).
- Shared-process tenants only.
- Read and write against user tables that already exist in the parent.
- Single-node correctness on the demo path.

Out of scope (and explicitly broken in some cases):

- Branch of a branch.
- Branch deletion.
- Multi-region or multi-node correctness.
- Anything called out in "What doesn't work" below.

The rest of this document explains how it works, what trade-offs are baked in,
and where the corners are.

## Two concerns

The work splits into two pieces that should be reasoned about independently.

The first piece is the tenant itself: what metadata identifies a branch, how
tenant creation differs from a normal tenant, what the branch's keyspace
contains on day one. This piece would exist in any branching design.

The second piece is how reads on the branch find the parent's data when the
branch hasn't written anything for a key. We need this because the storage
engine doesn't expose snapshots or file-level copy-on-write. With a storage
backend that did, this whole piece would shrink to a metadata pointer ("branch
shares parent's state at fork_ts") and the read paths would unify naturally.
The PoC works around the missing capability at the kv.Sender layer, with two
RPCs per read and a request-level merge.

The point of keeping these separate is that swapping the storage backend
changes everything in the second piece without touching the first.

## Tenant layer

The tenant layer is mostly metadata and wiring.

### What a branch is

A branch is a regular tenant with two extra metadata fields and an empty
keyspace. The metadata records the parent tenant ID and the timestamp at
which the branch was created (fork_ts). Anything that reads tenant metadata,
like the controller deciding whether a tenant exists or what service mode
it's in, sees a normal tenant.

The branch starts with a totally empty keyspace. No descriptors, no system
tables, no sequence state, no zone configs of its own. Anything the SQL pod
needs to read at boot, including the catalog, comes from the parent through
the CoW layer. This is what makes creation O(1) and is the source of several
limitations that show up later.

### Metadata

The two fields live in the existing `ProtoInfo` proto stored in the `info`
column of `system.tenants`:

- `branch_parent_id`: the tenant ID of the parent. Set only on branches.
- `branch_timestamp`: the fork timestamp.

This could also be stored in a new table like `system.tenant_branches`. It was
rejected for the PoC because the proto column is the canonical metadata bag for
a tenant; adding two fields is the smallest surface area. A new table would
force a new migration, new read paths in server controller startup, and a join
everywhere the tenant record is loaded. None of that pays for itself when the
only consumer of the data is the branch wiring path.

A small helper `IsBranch()` returns true when `branch_parent_id` is set.
Everywhere else in the system that needs to know "is this a branch" goes
through this helper.

### Creation

The `CREATE VIRTUAL CLUSTER ... BRANCH FROM <parent>` path does the
following, in order, in a single transaction:

1. Resolve the parent tenant. Reject if the parent is itself a branch.
2. Snapshot the cluster clock. This timestamp becomes both the
   `branch_timestamp` metadata and the protection floor handed to the CoW
   layer.
3. Allocate a tenant ID and write a tenant record with branch metadata set,
   `DataState = Ready`, `ServiceMode = None`.
4. Hand control to the CoW layer to set up its protection record (see "The
   PTS record" below).
5. Pre-create one range at the branch span boundary so the branch's first
   write has somewhere to land.

Two choices in this list deserve calling out.

`DataState = Ready` means we skip the `Add` to `Ready` bootstrap that a
normal `CREATE VIRTUAL CLUSTER` goes through. The bootstrap exists to
populate system tables and seed initial ranges. A branch doesn't need
either: the CoW layer will satisfy reads against the parent's
already-bootstrapped state, and the `AdminSplit` in step 5 gives writes a
landing place. Skipping bootstrap is what makes creation O(1).

The `AdminSplit` at the branch span boundary is needed because, without it,
the empty branch keyspace would be glued onto whatever range happens to
cover the address space adjacent to the parent. The first write on the
branch would either land in a range that spans multiple tenants (rare today
but possible) or fail to find a leaseholder cleanly. The split is cheap and
obvious.

### What the branch starts with

Nothing. Every system table the SQL pod expects to find is missing from the
branch's keyspace. The branch boots only because the CoW layer makes the
parent's catalog visible at fork_ts. Any descriptor lookup, any zone config
read, any setting fetch goes through the CoW path on its first occurrence
and returns the parent's value as it stood at the fork.

This works for reads but creates a sharp edge for writes that touch the
catalog. See "What doesn't work" for the specific failure modes.

### Wiring metadata to the SQL pod

When a shared-process tenant starts, the server controller reads its tenant
record. If the record is a branch, the controller builds a small
`TenantBranchInfo` struct (parent ID and fork timestamp) and plumbs it
through the SQL pod's `SQLConfig`. Presence of that struct flips on the CoW
interceptor in the pod's KV stack.

The struct deliberately doesn't carry the full tenant record. The startup
path needs exactly the parent ID and the timestamp, nothing else. Keeping
the struct narrow makes it easy to change the metadata without touching the
wiring.

### How each subsystem behaves on a branch

User tables are the happy path: reads return a merged view, writes land
in the branch's keyspace, parent stays untouched. Everything else lives
either inside the tenant's own keyspace (system tables, sequences,
descriptors) or in host-side metadata (`system.tenants` columns), and
each subsystem composes with branching differently. The table below
covers the rest.

Status is one of: **Works**, **Broken** (known failure mode),
**Limited** (works but with a caveat that changes the semantics from
what a user would expect), or **Untested**.

| Subsystem                          | Behavior on a branch                                                                                                                                                                                          | Status                                    |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| User tables and rows               | Reads return branch writes layered over parent at fork_ts; writes land in the branch's keyspace and stay isolated.                                                                                            | Works                                     |
| Schema (descriptors)               | Reads see parent's descriptors at fork_ts via fallthrough. `ALTER TABLE` issues a CPut whose expected bytes came from the parent read; `BranchSender` sets `AllowIfDoesNotExist = true` on the CPut so the first write against the empty branch keyspace succeeds. Subsequent CPuts on the same descriptor see the branch's value and use the normal conditional check. | Works                                     |
| `CREATE TABLE` on a branch         | Descriptor-ID allocation issues an `Increment` that `BranchSender` seeds with the parent's current value at fork_ts before dispatch. The branch's allocated IDs are strictly above what the parent had at fork_ts; no collision with parent descriptors. | Works                                     |
| Sequences (user)                   | Same `Increment` seeding mechanism as descriptor IDs. Branch's sequence values continue from parent's value at fork_ts. Branch and parent diverge after the fork; uniqueness is per-tenant, not across the pair. | Works                                     |
| Cluster settings                   | Reads see parent's settings as of fork_ts via fallthrough. Branch-side overrides are `Put`-based, so they should land in the branch's keyspace, but post-fork setting changes on the parent do not propagate. | Untested                                  |
| Jobs system                        | Job rows live in `system.jobs`. Reads fall through to parent, so a branch sees parent's job history. Job claims and updates use CPut-style operations and likely fail for the same reason `ALTER` does.       | Untested                                  |
| Privileges and roles               | Stored on descriptors and in `system.users` / `system.role_members`. Reads see parent's. Mutations use CPut and would fail.                                                                                   | Untested                                  |
| Table statistics                   | Reads see parent's stats at fork_ts. Recomputation on the branch (which writes back to `system.table_statistics`) is untested.                                                                                | Untested                                  |
| Tenant capabilities                | Live in `system.tenants` host-side, not in the tenant's keyspace. The branch has its own; nothing inherited from the parent.                                                                                  | Works                                     |
| Zone configs                       | Branch gets a fresh zone config at create time. Parent's customizations do not carry over.                                                                                                                    | Limited (no inheritance)                  |
| `SELECT FOR UPDATE` on parent rows | Locks would be acquired on the parent's keyspace via the non-txn fallback `kv.DB`, not transferable into the branch's transaction.                                                                            | Broken                                    |
| AOST queries on a branch           | Branch reads pin to fork_ts. Combining that with an explicit AOST is unspecified.                                                                                                                             | Untested                                  |
| Rangefeeds on a branch             | The rangefeed `dbAdapter` `Unwrap`s past `BranchSender`. Rangefeeds see only the branch's own writes; parent's data is invisible.                                                                             | Limited (parent-blind)                    |
| Changefeeds on a branch            | Built on rangefeeds, so they would see only the branch's own writes.                                                                                                                                          | Untested but expected to be parent-blind  |
| Backup of a branch                 | Backup walks ranges in the branch's keyspace and would capture only the branch's own writes, not the merged view.                                                                                             | Untested but expected to miss parent data |
| PCR / LDR with a branch            | PCR's tenant model assumes a self-contained tenant keyspace; a branch isn't.                                                                                                                                  | Untested                                  |

## Copy-on-write layer

### Why this layer exists

The branch's keyspace is empty at creation and stays empty for any key the
user hasn't written. A read on the branch must reach the parent's data at
fork_ts to return anything.

If the storage layer exposed snapshots or file-level copy-on-write, the
branch would just be a metadata pointer at the parent's snapshot, and the
read path would unify across the two without any per-request work. Pebble
doesn't expose either today (shared SSTs and virtual SSTs are experimental
and not wired through the SQL stack), so we work around the missing
capability at the request layer.

This means everything in this section is, in some sense, a workaround for a
missing storage capability. A future migration to a snapshotting backend
would replace most of it.

### Where the CoW could go

Four candidate layers, deepest to shallowest.

**Storage engine.** Native snapshots or shared/virtual SSTs at the Pebble
level. The branch is a metadata pointer at the parent's snapshot; reads
naturally walk both. Out of reach today; this entire section disappears if
we ever get there.

**MVCC scanner.** A scanner that knows about parent fallthrough and walks
both tenants' keyspaces in a single pass. Cleanest in-process design.
Requires teaching the MVCC iterator about cross-tenant reads, intent
resolution that crosses tenant boundaries, and pushing branch metadata into
the storage layer. Big change.

**KV server (batcheval).** Move the fallthrough into the server-side
handlers for `Get` and `Scan`. Cross-cuts leaseholders, range boundaries,
replicas, quota pools. Touches the hot path for every tenant.

**kv.Sender wrapper in the SQL pod.** Intercept reads above the wire, send
a separate batch to the parent's keyspace, merge in-process. Lives in one
place, easy to remove, requires no protocol changes. Two RPCs per read; no
cross-tenant transactionality.

The PoC picks the last. It's quickest to implement and is easy to revert if we
move CoW into the storage layer later. The cost of that choice (latency, format
conversions, no atomicity across tenants) is the source of most gotchas in this
section.

The interceptor is named `BranchSender`.

### The PTS record

The CoW layer reads the parent's keyspace at fork_ts every time the branch
needs to fall through. Without protection, MVCC GC would eventually reclaim
the data the branch needs. The first thing CoW initialization does at
branch creation time is write a protected timestamp record on the parent.

The record is held for the life of the branch and never released today.
Branch deletion is not implemented. A failed branch creation that errors
after writing the PTS leaks the record.

### Where BranchSender sits

In a branch tenant's SQL pod, the kv.Sender stack is:

```
kv.DB -> TxnCoordSender -> BranchSender -> DistSender -> wire
```

`BranchSender` sits below `TxnCoordSender` (TCS) and above `DistSender`.
Both placements are forced by the design.

Below TCS: the branch's transaction tracks intents and refresh spans for
keys in the branch's keyspace only. The fallback to the parent reads keys
in a different keyspace that the branch's transaction can't and shouldn't
enroll. If `BranchSender` sat above TCS, it would receive batches with
transaction metadata and would have to either hand the parent's keys to TCS
(which then tries to track intents in a tenant the branch isn't authorized
to write) or strip the transaction off the fallback (which reintroduces the
same problem we're solving). Below TCS, `BranchSender` sees raw requests and
can issue the fallback as a separate non-transactional batch with no
consequence to the branch's transaction.

Above DistSender: `BranchSender` needs to see the request before it's split
across ranges and dispatched. A wrapper below DistSender would have to
coordinate fallback at the range level, where the request has already been
fragmented.

The consequence of "below TCS" is that the fallback to the parent is not
transactional. There is no cross-tenant atomicity, no shared refresh, no
shared locks. This shapes several of the limitations in the next two
sections.

### Writes

Most writes pass through unmodified. `BranchSender` forwards `Put`,
`Delete`, and similar requests directly to `DistSender`; the branch's
keyspace is empty for any key the branch hasn't written, so a `Put`
just lands.

Two write types have read-modify-write semantics built into the request
and need first-write CoW so they produce the right outcome on a branch.
`BranchSender` interposes targeted logic for them before forwarding:

- `Increment` requests (sequence keys: descriptor IDs, role IDs,
  user-defined sequences). `MVCCIncrement` starts from 0 if the key has
  no prior value. On a fresh branch the sequence keys are empty, so an
  unmodified `Increment` would allocate IDs that collide with what the
  parent already issued. Before dispatch, `BranchSender` scans the batch
  for `Increment`s and, for any whose target key has no value on the
  branch, copies the parent's current value at fork_ts into the branch
  via a non-transactional `Put`. The `Increment` then proceeds normally
  and produces parent_value + delta.
- `ConditionalPut` (`CPut`) requests with non-nil expected bytes. The
  schema changer reads a descriptor (gets the parent's via fallthrough)
  and follows up with a `CPut` whose expected value is what it read.
  Without intervention, the `CPut` targets the empty branch keyspace and
  the conditional check fails. `BranchSender` sets
  `AllowIfDoesNotExist = true` on every such `CPut`. The flag, which
  already existed in `kvpb` for unrelated reasons, makes the `CPut`
  succeed if the expected bytes match OR the key does not exist. The
  "key does not exist" branch is exactly the first-write case; the
  "key exists but doesn't match" failure mode is preserved.

These two interceptions are narrow on purpose. Other write types don't
read prior state, so they pass through. See the more-detailed write-up
under "First-write CoW for `Increment` and `CPut`" in "More details"
below.

The "copy" in copy-on-write is still mostly lazy and read-side: a key
materializes in the branch's keyspace on first write (or first read
where seeding kicks in for sequence keys), and subsequent reads see the
branch's value and skip the fallback.

### The three-state read problem

For each key the branch's read path encounters, three states matter:

- The branch wrote a value. Use it.
- The branch deleted the key. Suppress the parent fallthrough.
- The branch never touched the key. Fall through to the parent.

The middle state is the trap. Without an explicit signal for "the branch
has deleted this key", a `DELETE` on the branch silently un-does itself:
the row appears missing in the branch's keyspace, fallthrough returns the
parent's row, and from the user's perspective the `DELETE` never happened.
The PoC depends on having a way to distinguish "branch deleted" from
"branch never wrote".

The signal we picked is the MVCC tombstone, surfaced through a new flag.

### IncludeTombstones

An MVCC tombstone is a storage-layer construct: an MVCC version with empty
value bytes. Pebble writes one whenever batcheval handles a `DeleteRequest`.
By default, MVCC scans and gets filter tombstones out before returning, so
a key that has only a tombstone looks like a missing key to the SQL layer.

The PoC adds an `IncludeTombstones` flag to `GetRequest` and `ScanRequest`
in `kvpb`. The batcheval handlers for `Get` and `Scan` pass the flag through
to `MVCCGetOptions.Tombstones` / `MVCCScanOptions.Tombstones`, which the
storage layer already supports. The result is that `BranchSender` can ask
the branch's keyspace for tombstones and learn which keys the branch has
deleted.

A few alternatives were considered.

A sentinel value written by `BranchSender` on `DELETE`. The wrapper would
intercept deletes and write a `Put` with a known marker value instead. Reads
would check for the marker and suppress fallthrough. This decouples the
deletion signal from MVCC GC entirely (the marker is a regular value and
won't get collected as a tombstone would). The cost is that every system
that reads the data needs to learn the marker: rangefeeds, backup,
replication, anything that walks raw KV. Not worth the blast radius for a
PoC.

A per-version "branch-owned" bit at the MVCC level. The cleanest fix in
principle: tag every MVCC version written by the branch (including
tombstones) so GC and reads can both treat them specially. The deepest
change of any of the alternatives. Out of scope.

The `IncludeTombstones` flag is the smallest change that makes the
three-state distinction available. It comes with a downstream gotcha
(tombstone GC) that we don't fix today; see "Tombstone GC and the
regression" below.

### Scan format handling

SQL pods normally request scan results in `BATCH_RESPONSE` format (or
`COL_BATCH_RESPONSE` for the vectorized engine). Both encode keys into
opaque byte buffers that the SQL pod decodes with a table descriptor.

`BranchSender` can't work with those formats. The merge step needs explicit
per-row keys to dedupe by, suppress shadowed parent rows by, and rewrite
tenant prefixes on. Decoding the opaque buffers without a table descriptor
is not feasible.

So both the branch read and the parent fallback are forced to `KEY_VALUES`
format (which returns `[]roachpb.KeyValue` with the keys exposed). After the
merge, the response is re-encoded back to whatever format the caller asked
for.

`COL_BATCH_RESPONSE` is collapsed to `BATCH_RESPONSE` during re-encoding.
The columnar encoder needs an `IndexFetchSpec` the wrapper doesn't have, and
it would have to rewrite tenant prefixes inside the spec. The SQL pod's KV
fetcher accepts `BATCH_RESPONSE` regardless of the original `ScanFormat`
(it checks for non-empty `BatchResponses` first), so the substitution is
invisible to the caller.

### The fallback batch

When `BranchSender` returns the branch's response and finds keys that need
fallback, it builds a fallback batch against the parent's keyspace.

For Gets: include in the fallback if and only if the branch returned no
value and no tombstone. A value or a tombstone means the branch has a
definitive answer; only a true absence triggers fallback.

For Scans: always include in the fallback. The branch may have only a
subset of the rows in the scan range, and the parent may have additional
rows the branch hasn't shadowed. The merge handles deduplication and
tombstone suppression; we let it do the work.

The keys in the fallback batch are the original request keys with the
branch's tenant prefix stripped and the parent's prefix prepended. So
`/Tenant/dev/Table/.../row` in the original becomes `/Tenant/prod/Table/.../row`
in the fallback.

The fallback batch has no transaction attached. The batch header pins
`Timestamp = fork_ts` and `ReadConsistency = CONSISTENT`. The intent is a
snapshot read of parent's state at fork_ts.

### Fallback execution

The fallback is sent through a separate `kv.DB` rooted on the raw
`DistSender`, not through `BranchSender`'s own `kv.DB`.

Two reasons for the separate `kv.DB`.

The branch tenant's main `kv.DB` has `BranchSender` in its sender stack.
Sending the fallback through the main `kv.DB` would route the fallback
back through `BranchSender`, which would then try to fall back again on its
own fallback. Building a separate stack rooted on raw `DistSender` breaks
the loop.

The fallback is non-transactional and frequently needs to span multiple
ranges (any nontrivial parent-side scan). `DistSender` doesn't auto-wrap
non-transactional multi-range batches; the call would error. The
`kv.DB`'s `NonTransactionalSender` includes a `CrossRangeTxnWrapperSender`
that auto-wraps such batches in a transaction. Using the fallback `kv.DB`'s
`NonTransactionalSender` gives us the auto-wrap.

There's a real correctness gap here. The `CrossRangeTxnWrapperSender` drops
the AOST timestamp when it auto-wraps. The fallback batch went in with
`Header.Timestamp = fork_ts`; after the wrap, it executes at the txn's
timestamp, which is the cluster's current time. Single-range fallbacks
bypass the wrapper and preserve fork_ts. Multi-range fallbacks observe the
parent's current state.

The PTS guarantees the fork_ts data is reachable. In practice, for keys
the branch hasn't shadowed, a current-time read returns the same row as a
fork_ts read would in many cases, but any post-fork parent write to a
key the branch hasn't shadowed will leak into the branch's view. The PoC
tests pass because the test tables fit in a single range and the wrapper
isn't hit. A larger table that spans ranges would expose the gap.

### The merge

After the fallback returns, `BranchSender` merges the two responses into a
single key-sorted result.

The merge:

- Builds a set of branch keys that includes both branch-written values and
  branch-written tombstones.
- Emits branch values that have non-empty bytes (skips tombstones).
- For each parent row, rewrites the key from the parent prefix to the
  branch prefix. Drops the row if the key is in the branch set (branch
  wins, or branch tombstoned).
- Sorts the combined output by key.

Today the merge is materialized: both responses are slices, and we build a
third slice and sort it. A streaming merge from two sorted iterators would
be O(n) instead of O(n log n), but the responses are already materialized
in memory, so the algorithmic constant doesn't matter at PoC scale.

### Tombstone forms

SQL `DELETE` produces point tombstones, one per row. The SQL planner reads
to find the rows to delete and issues a `DeleteRequest` per row; batcheval
writes one MVCC tombstone per request.

Point tombstones work on the branch even when the underlying key never
existed in the branch's keyspace. `MVCCDelete` has no precondition on prior
state; Pebble writes a standalone tombstone version over nothing. So
`DELETE FROM accounts WHERE id = 2` on the branch (where row 2 lives only
in the parent) writes a tombstone at the branch's key for `id = 2`. The
branch's read path returns the tombstone; the merge suppresses parent's
row 2.

Range tombstones (`MVCCRangeTombstone`, written by `DeleteRangeRequest`
with `UseRangeTombstone = true`) are not handled. The merge walks per-row
`KeyValue` lists and never inspects the range tombstone channel. SQL
`DELETE` doesn't produce range tombstones in any normal path, so this is
not a problem in practice. It is a hazard if anyone ever wires a
`DeleteRangeRequest` into a write path that targets a branch: the deletion
would land in the branch's keyspace as a range tombstone, the merge would
ignore it, and the parent's rows in the affected range would leak.

### Tombstone GC and the regression

Point tombstones are subject to MVCC GC. A point tombstone becomes eligible
for collection once it's older than the `gc.ttlseconds` setting on the
surrounding zone config and is the latest version of its key (it's always
the latest in our case). Once collected, the branch's keyspace has nothing
at that key, `BranchSender`'s read sees neither value nor tombstone, the
merge falls through to the parent, and the deleted row reappears.

Deletions on a long-lived branch silently un-do themselves after one GC
TTL. The PoC does not address this.

Three possible mitigations, listed by where the trade-off sits.

PTS on the branch's own keyspace at fork_ts. Targeted, symmetric with the
PTS we already write on the parent. Pins all branch state including
tombstones for the life of the branch. Cheap. The natural production
answer.

Bump `gc.ttlseconds` on the branch's zone config. Simpler than a PTS; one
cluster setting per branch. Preserves all MVCC versions on the branch, not
just tombstones, so write churn accumulates as MVCC garbage proportional
to write rate times branch lifetime. Fine for short-lived branches, bad
for long-lived ones with heavy update traffic.

Sentinel values instead of tombstones. Decouples deletion durability from
GC entirely. Costs are the same as in "IncludeTombstones": every reader
has to learn the marker.

### Rangefeed integration

Rangefeeds operate on raw KV state and have no notion of branching. They
expect to talk to the underlying `DistSender` directly.

The rangefeed `dbAdapter` walks the kv.Sender chain via `Unwrap()` to find
the underlying `DistSender`. `BranchSender` implements `Unwrap()` to return
its inner sender, so the `dbAdapter` steps through `BranchSender` to reach
`DistSender`. Rangefeeds on the branch see the branch's own keyspace only;
the parent's data is not merged into the rangefeed stream.

This is the right behavior. A merged rangefeed would have to fabricate
events for the parent's pre-fork state (not how rangefeeds work) and decide
what to do about post-fork parent writes the branch doesn't see (which the
branch isn't supposed to observe anyway). The branch's rangefeed shows what
the branch did, which is a coherent answer.

The implication is that any subsystem that consumes rangefeeds on a branch
(changefeeds, replication, span configs) operates only on branch-originated
changes. None of these are tested.

## What doesn't work

Each item names the symptom, the root cause, and the section that explains
it.

**Branch of a branch.** `BranchSender` does a single fallback hop, and the
metadata only carries one parent ID. Recursive fallback or a chain isn't
wired. The `CREATE` path explicitly rejects branching from a branch. See
"What a branch is" and "Where BranchSender sits."

**Branch deletion.** Not built. The PTS on the parent leaks. The branch's
keyspace is left around. See "The PTS record."

**`SELECT FOR UPDATE` and other locking reads on parent rows.** The
fallback runs through a separate `kv.DB` and is non-transactional. Locks
acquired during the fallback would be on the parent's keyspace and not
transferable into the branch's transaction. See "Where BranchSender sits."

**Range tombstones in the branch.** The merge walks per-row `KeyValue`
lists and doesn't read the range tombstone channel. SQL `DELETE` doesn't
produce range tombstones, so this only matters if someone wires a
`DeleteRangeRequest` into a branch write path. See "Tombstone forms."

**Long-lived deletions silently regressing.** Tombstone GC reclaims point
tombstones once they age past `gc.ttlseconds`. Once collected, the branch's
read path falls through to the parent and the deleted row reappears. See
"Tombstone GC and the regression."

**Multi-range parent fallback at fork_ts.** When the fallback batch spans
ranges, `CrossRangeTxnWrapperSender` auto-wraps it into a transaction and
drops the AOST timestamp. The fallback then reads parent's current state
rather than fork_ts. Single-range fallbacks preserve fork_ts. See
"Fallback execution."

**AOST queries on a branch, changefeeds, backups.** Untested. Each
interacts with the CoW layer in ways the PoC hasn't validated.

## Gotchas

Things that work today but matter for anyone touching this.

Two RPCs per read on the branch's hot path. Latency cost compounds for
plans with many small Gets.

Forcing `KEY_VALUES` on every branch and fallback read defeats encoding
optimizations the SQL pod relies on. The post-merge re-encoding cost grows
with scan size.

The fallback's AOST drift on multi-range batches. Single-range scans
preserve fork_ts; multi-range scans observe the parent's current state.
Cross-ref "Fallback execution."

PTS retention has no release path. A failed branch create that errors after
writing the PTS leaks the record. The PoC does not retry-safe the PTS
write.

Rangefeeds on a branch see only the branch's own writes. Any consumer of
rangefeed events (changefeeds, replication, span config maintenance) sees a
partial picture.

Any new sender wrapper inserted between TCS and DistSender in a branch
tenant must implement `Unwrap`, or the rangefeed `dbAdapter` will fail to
find `DistSender` and rangefeed setup will error.

## More details

This section collects deeper dives that came out of follow-up questions while
reading the design above. Each subsection assumes you've read the relevant
part of the main design and answers a "but why" that the main text glossed
over.

### Why AdminSplit at the branch span boundary

The short answer: without the split, the branch's first write lands in a
range that already belongs to something else, and the branch's keyspace ends
up sharing a range with another tenant.

Ranges in CockroachDB cover contiguous slices of the global keyspace, and
they're created lazily. Before the branch exists, the address space where
the branch's keyspace will live (`/Tenant/<branchID>/...`) is just sitting
inside whatever range happens to cover that part of the keyspace. Most
likely that's the trailing range of the previous tenant, or some catch-all
range that runs from "end of last allocated tenant" to "end of keyspace."
There's no range that starts at `/Tenant/<branchID>`.

When the branch issues its first write, KV has to find the range that
covers the write's key. It finds the range that already owns that slice of
address space, and the write lands there. Several things go wrong from that
point.

**Tenant isolation at the range level breaks.** A core invariant of
multi-tenancy in CRDB is that a single range belongs to one tenant. Lots
of code relies on this: per-tenant rate limiting, span configs, zone
configs, tenant capabilities, leaseholder accounting. A range that holds
keys from two tenants is a state the system is not designed to handle
gracefully. It often works in practice, but it's an invariant violation,
and bugs show up in places you wouldn't expect.

**Zone config and span config dispatch breaks.** Zone configs (replication
factor, GC TTL, locality preferences) are applied at the range level,
sourced from the span config that covers the range. The span config
subsystem walks ranges and assigns configs based on which tenant's
keyspace the range falls in. A range that straddles two tenants' keyspaces
gets one config or the other, not both. The branch's own zone config
silently doesn't apply.

**Future range operations get dangerous.** When branch deletion is
eventually built, the natural way to nuke a branch's data is `ClearRange`
over the branch's span. `ClearRange` operates at the range level. If the
branch shares a range with another tenant, `ClearRange` either corrupts
the other tenant's data or refuses to run cleanly. Splitting up front is
what makes the branch's keyspace separable from its neighbors.

**Leaseholder and write contention.** The first writes from the branch
funnel into a range whose leaseholder was placed for a different workload.
The branch's writes compete with whatever else lives there.

The `AdminSplit` at the branch's span start key (`/Tenant/<branchID>`)
carves a range boundary so that the range covering the start of the
branch's keyspace is dedicated to the branch. Subsequent splits as the
branch grows are handled by the normal split queue, but the first one we
have to do explicitly because nothing else is going to do it.

A normal `CREATE VIRTUAL CLUSTER` doesn't need this step explicitly
because the bootstrap path it goes through (the one we skip) does a bunch
of pre-splits for system tables and for the tenant boundary. We skipped
bootstrap to make creation O(1), so we have to put back the one piece of
bootstrap that's actually load-bearing for correctness. The split is cheap
(a single AdminSplit RPC), so paying for it is the right trade.

One nit on the implementation in the PoC: the split is done with a
one-hour expiration. That's enough time for the branch's first writes to
land and for the normal split queue to take over, which prevents the
manual split from sticking around forever. If the branch sees zero traffic
in the first hour, the split expires and the keyspace re-merges with the
neighboring range. That's fine for the demo but is a footgun for a
hibernating branch.

### Why deletions on a branch don't need first-write CoW

When a user runs `DELETE FROM accounts WHERE id = 2` against a branch
where row 2 lives only in the parent's keyspace, the SQL planner does its
usual two-step: scan to find the row, then issue a `DeleteRequest` for it.
The scan goes through `BranchSender`'s fallthrough and returns parent's
row 2. The `DeleteRequest`, however, goes straight through to the branch's
keyspace, where the key for `id = 2` doesn't exist.

This works because `MVCCDelete` has no precondition on prior state. Pebble
writes a tombstone version at the branch's key whether or not anything
was there before. The next read by the branch returns a tombstone for
that key, the merge suppresses parent's row 2, and from the user's
perspective the row is gone.

This is the load-bearing property that makes deletions work without a
first-write CoW step. Without it, the branch would need to read the
parent on every delete, copy the value into its own keyspace, and then
write the tombstone over its own copy. We get that behavior for free
from MVCC. The two write types that genuinely need first-write CoW
(`Increment` and `CPut`) get explicit interception instead; see
"First-write CoW for `Increment` and `CPut`" below.

### What PROTECT_AFTER actually preserves for the branch

The PTS keeps the branch's snapshot of the parent alive regardless of
what the parent does after fork_ts. The mechanism is worth pinning down.

`PROTECT_AFTER(T)` preserves any MVCC version whose live window touches
`T` or later. For the branch, this means every version of every key that
was the latest visible value at fork_ts is pinned, even after the parent
overwrites it.

Walk through it. Parent has `(id = 1, balance = 100)` at fork_ts.
Post-fork, parent runs `UPDATE balance = 9999 WHERE id = 1`. Two MVCC
versions now exist. Without protection, the older `balance = 100` would
be GC'd once it aged past `gc.ttlseconds` (it's no longer the latest
version at any current read timestamp). The PTS keeps it alive: it was
the live version at fork_ts, and `PROTECT_AFTER(fork_ts)` covers it.

The PTS does not pin parent's history forward. If parent then updates
`balance` to 7777, the intermediate `9999` is still GC-eligible once it
ages out. The protection is for the fork_ts snapshot, not for everything
the parent writes from then on.

This is what makes the parent independently usable while a branch is
open: parent can write freely, and the branch's view at fork_ts stays
intact until the PTS is released.

### PTS on the branch vs raising the branch's GC TTL

Both are valid ways to keep branch tombstones from being GC'd, and they
sit at different points in the trade-off.

A PTS on the branch's keyspace at fork_ts is targeted: it pins exactly
the snapshot the branch needs. Branch's own post-fork updates are still
GC-eligible once their newer versions age out. Storage footprint scales
with the branch's live state plus the versions that were live at
fork_ts.

Bumping `gc.ttlseconds` on the branch's zone config is blanket: every
MVCC version on the branch is preserved for the TTL window, including
non-tombstone churn from regular updates. Storage footprint scales with
write rate times TTL. Fine for a hibernating branch with little write
traffic; expensive for an active branch with heavy update churn (the
MVCC iterator skips more dead versions on every read, scans get slower).

The same trick is not available on the parent side. Bumping the parent's
`gc.ttlseconds` to keep parent's pre-fork versions alive would mean every
tenant that happens to have a branch forces GC decisions onto its parent.
That is cross-tenant coupling: a branch's existence shapes GC behavior in
a different tenant. The PTS we put on the parent today exists precisely
to keep that coupling narrow, scoped to a single record the parent's GC
queue consults, rather than spreading branch-related policy into the
parent's zone config.

### How range scans behave when the branch deletes a sub-range

This is what `mergeScanRows` is doing in practice. Walk through
`DELETE FROM accounts WHERE id BETWEEN 5 AND 8` on the branch followed
by `SELECT * FROM accounts WHERE id BETWEEN 1 AND 10`:

1. The DELETE: SQL plans a per-row delete. The scan to find rows 5..8
   falls through to the parent and returns four rows. SQL issues four
   `DeleteRequest`s, one per row. The branch's keyspace gains four point
   tombstones at keys for `id = 5, 6, 7, 8`.
2. The SELECT: `BranchSender` forces `IncludeTombstones = true` and runs
   the scan against the branch's keyspace. Returns the four tombstones
   (no values, since the branch never wrote any of these rows).
3. `BranchSender` builds a fallback `ScanRequest` against the parent and
   gets back rows 1..10.
4. `mergeScanRows`:
   - Branch keys (values plus tombstones): {5, 6, 7, 8}.
   - Branch values to emit: none (tombstones don't emit).
   - Parent rows: drop any whose key is in the branch set. So 5, 6, 7, 8
     are dropped.
   - Result, sorted: 1, 2, 3, 4, 9, 10.

The merge handles arbitrary interleavings of "branch wrote", "branch
deleted", and "branch never touched" cleanly. The `scan_merge` testdata
file exercises this with branch updates at ids 3, 7, 10, deletions at
ids 2, 5, 8, and inserts at 0, 11, 12.

The whole approach depends on point tombstones. If anything in the write
path produced an MVCC range tombstone for the deleted sub-range instead,
the merge would not see it (it walks per-row `KeyValue` lists and never
reads the range tombstone channel) and parent's rows in the affected
range would leak through. SQL `DELETE` only produces point tombstones,
so this is theoretical today. It becomes real if anyone wires a
`DeleteRangeRequest` with `UseRangeTombstone = true` into a branch's
write path.

### Why BranchSender sits between TCS and DistSender

The kv.Sender stack in a SQL pod is:

```
*kv.DB  ->  TxnCoordSender  ->  DistSender  ->  gRPC  ->  KV nodes
```

For a transaction, the user's `*kv.Txn` is built on its own TCS instance;
the rest of the stack is shared.

Each layer in this stack does one job. TCS owns the lifecycle of a single
transaction: it tags outgoing BatchRequests with txn metadata, tracks
intents the txn has written, accumulates refresh spans for the keys the
txn has read, heartbeats the txn record, and handles refresh on commit-time
conflicts. DistSender takes the BatchRequest, looks up the ranges that
cover the keys, splits the batch by range, routes each piece to the
appropriate leaseholder, retries on stale range descriptor cache, and
reassembles the response.

`BranchSender`'s placement options are constrained by what these two layers
do.

**Why not above TCS.** If `BranchSender` wrapped the user-facing `kv.DB` /
`kv.Txn` API, it would intercept high-level operations before TCS has
added any transaction metadata. The branch's own read against the branch's
keyspace would have to be re-injected into the txn somehow, and the parent
fallback would still need to be peeled off as a separate non-txn read.

The second part (build a separate `kv.DB` for the parent fallback and call
into it) is feasible regardless of where `BranchSender` sits. The first
part is the hard one. If the branch's own read doesn't go through TCS, the
read isn't part of the txn, and the txn loses intent tracking, refresh
spans, and the rest of the txn machinery for the branch's own keys.
Rebuilding any of that above TCS would amount to reimplementing TCS.

By sitting below TCS, the branch's own read goes through TCS naturally:
TCS adds txn metadata, `BranchSender` forwards the BatchRequest unchanged
to DistSender, the response comes back through `BranchSender` to TCS, and
TCS records the txn's read activity normally. The only special thing
`BranchSender` does is build a side request (the parent fallback) on a
separate sender chain that has no TCS.

**Why the parent fallback can't be transactional.** The parent fallback
could in principle stay inside the branch's transaction. We don't do that
for three reasons.

The branch's tenant capabilities don't authorize writes (or write intents)
on the parent's keyspace. If the branch's txn enrolled parent's keys, any
subsequent intent-resolution path that thinks "this txn touched these
keys, let me act on them" might attempt operations the tenant isn't
allowed to perform.

Refresh on parent's keyspace is meaningless. TCS refreshes a serializable
txn by re-reading the keys at a new timestamp and verifying nothing
changed. The parent has its own writers; refresh would constantly fail
because the parent is moving forward independently of the branch.

Locks on parent's keyspace are unwanted. The branch's txn would block
parent writers. Tenant isolation requires that one tenant's transactions
cannot lock another tenant's keys.

So the fallback must be non-transactional, and to issue a non-transactional
batch `BranchSender` needs a sender chain that doesn't have TCS in it. The
simplest way is a separate `kv.DB` rooted on raw DistSender. That's what
the PoC does.

**Why not below DistSender.** DistSender's job is to take a logical
BatchRequest, fragment it across the ranges that cover the keys, and
dispatch each fragment to the right leaseholder. By the time a
BatchRequest has been through DistSender's split logic, it isn't a logical
user operation anymore; it's a per-range chunk.

If `BranchSender` sat below DistSender, it would see chunks: "the part of
the user's scan that targets range R-103." The parent fallback for that
chunk doesn't have a clean shape. The parent's keyspace might be split
into completely different ranges, and a chunk of branch reads doesn't
correspond to any obvious chunk of parent reads. Multi-range scans would
multiply into N branch reads and N parent reads, with N merges, all
coordinated below DistSender.

Sitting above DistSender means `BranchSender` sees the user's logical
request once. It issues one branch read and one parent fallback for the
whole logical operation, merges once, returns once. DistSender does its
range-splitting work twice (for the branch read and for the parent read)
but the rest of the design stays simple.

There's also a practical reason: there is no kv.Sender slot below
DistSender. DistSender is the boundary between the kv client and the
network. A wrapper below DistSender would have to live inside the gRPC
transport layer, which is the wrong shape for a kv.Sender.

### First-write CoW for `Increment` and `CPut`

The original PoC implemented copy-on-write on the read side only. Two
write paths needed targeted first-write CoW to work correctly on a
branch: `Increment` (used for descriptor IDs, role IDs, and user
sequences) and `CPut` (used by the schema changer and several other
catalog mutators). Both have read-modify-write semantics built into the
request, so the branch's empty keyspace produces the wrong outcome
without intervention.

**Increment seeding.** Sequence keys live in the tenant's keyspace.
`MVCCIncrement` adds a delta to the existing value, or starts at 0 if
the key is unwritten. On a fresh branch every sequence key is unwritten,
so the first `Increment` would return delta and allocate values that
collide with what the parent already issued (descriptors with the same
numeric IDs as the parent's, etc.).

`BranchSender.seedIncrements` scans every batch for `IncrementRequest`s.
For each one whose target key has no value on the branch, it reads the
parent's value at fork_ts and writes it to the branch via a
non-transactional `Put`. The `Increment` then proceeds normally and
produces parent_value + delta. Subsequent `Increment`s on the same key
see the seeded value on the branch and skip the seeding step.

The seeding writes are non-transactional and idempotent on purpose. The
parent's value at fork_ts is fixed, so two concurrent seeders write the
same bytes; the second write is wasteful but harmless. The seeding has
to be visible to all subsequent readers (not just the calling txn),
otherwise concurrent `CREATE TABLE` statements on the branch would race
and allocate the same descriptor ID.

There's a per-batch overhead: every batch with at least one `Increment`
incurs an extra `Get` against the branch to check whether the key needs
seeding. After the branch has been touched the `Get` returns the seeded
value and seeding is skipped, but the `Get` itself still happens. A
"this key has been seeded" cache would eliminate the lookup on the hot
path.

We considered seeding all known sequence keys at branch creation time
instead. Rejected: the set of `Increment`-driven keys is open
(`DescIDSequence`, `RoleIDSequence`, every user-defined sequence) and
fragile to maintain as new ones are added. Per-`Increment` seeding is
general.

**CPut weakening.** `ConditionalPut` writes a new value if the key's
current value matches the expected bytes (or the key is missing and
`AllowIfDoesNotExist` is set). The schema changer uses `CPut` on
descriptors with the descriptor's prior bytes as the expected value:
"write the new descriptor only if it hasn't been changed since I read
it." On a branch, the read fell through to the parent, so the expected
bytes are parent's descriptor bytes; the `CPut` then targets the branch
keyspace, where the descriptor key is empty on the first write.
`CPut(expBytes = parent's, actual = nil)` fails the conditional check.

The fix is one line: `BranchSender.rewriteCPutsForBranch` sets
`AllowIfDoesNotExist = true` on every `CPut` with non-nil expected
bytes. The flag, which already existed in `kvpb` (`api.proto:369`) for
unrelated reasons, makes the `CPut` succeed if the expected bytes match
OR if the key does not exist. The "key does not exist" branch is the
first-write case. The "key exists but doesn't match expected" failure
mode is preserved, so genuine concurrent-modification conflicts on the
branch still fail correctly.

The relaxation is narrow but real. A `CPut` against a key that was
deleted on the branch (current value is nil because of a tombstone,
expected bytes is non-nil) will now succeed where it would have failed
on a non-branch tenant. For schema-change `CPut`s on descriptors this
is fine in practice: descriptors aren't deleted out from under live
mutations. For other `CPut` callers on a branch, the semantics are
slightly looser than on a regular tenant.

We considered intercepting `CPut` by rewriting it to a `Put` when the
expected bytes match the parent's value. Rejected: that needs an extra
`Get` against the parent on every `CPut` to verify the match, and it
ends up encoding the same idea as `AllowIfDoesNotExist` with more code.

**Why these two and not a general first-write CoW.**

A general first-write CoW would intercept every write, read the parent
to materialize the prior value into the branch, and then dispatch the
write. That's an extra RPC per write on every key. The two cases above
are the only built-in request types that have read-modify-write
semantics; for everything else (`Put` overwrites, `Delete` writes a
standalone tombstone, etc.) the parent's value doesn't need to be
locally present for the write to behave correctly. Targeted
interception keeps the cost on the writes that need it.
