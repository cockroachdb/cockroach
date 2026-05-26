# Continuous Backup / Rangefeed-to-S3

## Context / Motivation

CockroachDB today supports point-in-time backup/restore via discrete
full and incremental backups, with the "revision history" flavor of
incrementals capturing all MVCC versions in the window via polling
`ExportRequest(MVCCFilter_All)`. Recovery point objective (RPO) is
bounded below by how often you schedule incrementals, and the
operational cost of frequent incrementals is real.

Separately, rangefeed consumers (CDC, LDR, PCR, etc.) protect history
via per-job PTS records so their catch-up scans don't see GC errors.
PTS records can be scoped (per-table or per-database), so the issue
isn't keyspace breadth — it's *duration*. When the consuming job
stalls or falls behind for hours/days, MVCC garbage accumulates
within the protected scope and serving traffic on those same tables
pays the cost. The other awkwardness: catch-up is key-ordered (driven
by `MVCCIncrementalIterator`) rather than time-ordered.

This project introduces a **continuous backup** job that opens
rangefeeds across the cluster and streams events to external storage
(S3-style). The same external log then serves two purposes:

1. **Sub-minute RPO restore.** Restore replays the suffix of the log
   over the most recent discrete backup up to a chosen restore time.
   Inc backups themselves remain — they are load-bearing for RTO as
   the restore base on which the log is replayed — but the operational
   pressure to schedule them more frequently than ~hourly is relieved.
   Deprecates the "revision history" flavor of inc backup.
2. **External catch-up for rangefeed consumers.** A rangefeed client
   wrapper serves the catch-up phase from the log instead of from KV's
   MVCC history, transitioning to a live KV subscription once close to
   the present. Consumers covered by a continuous backup can then opt
   out of PTS, letting GC proceed on its normal schedule and shifting
   catch-up scans into time order (the natural order of the log).

---

## 1. Backup job / log production

### Concerns and constraints

- **Flush durability on the order of 5–10 seconds.** This is the
  RPO floor — events must be in durable external storage and visible
  to readers within that window of the write actually happening.
- **Resolved-timestamp / completeness markers are first-class.**
  Many readers can only operate on a time slice once they know it is
  *complete* — every change that occurred in the slice has been
  flushed. The log emits these as a primitive readers gate on, not
  as a derivation a reader has to compute.
- **Output organized so a reader can pick a subset.** Even if the
  job is logging an entire cluster, a reader rarely wants
  *everything* — it wants some subset of spans (could be all, could
  be one table's spans, could be a single index range) over some
  time slice. The two retrieval dimensions:
  - **Span subset.** Locate events for an arbitrary subset of spans
    (per-table restore "fish out widgets as of 30m ago after a bad
    UPDATE"; per-table changefeed catch-up; whole-cluster restore)
    without reading data for spans outside the subset.
  - **Time slice.** Locate a short window (e.g. 1h–6h) inside a
    long retention period (e.g. up to a year) without scanning the
    year.
- **The span set evolves under us.** A "cluster backup" today is a
  backup of every user table's every *index* span individually;
  nothing tells the backup job when tables or indexes are
  created/dropped. The log job watches the schema descriptor system
  table via its own rangefeed and replans on relevant changes —
  see "Span coverage tracking" below for the full mechanism. The
  earlier attempt to widen this to per-table spans hit a DistSQL
  planning snag (adjacent spans where `span1.EndKey ==
  span2.StartKey` get merged), so we stay at per-index span
  granularity.
- **PTS coordination across the cluster.** The log writer
  needs an MVCC floor to prevent GC from reaping data it
  hasn't yet flushed. Rather than each component installing
  its own redundant PTS, the cluster coordinates:
  - **Inc backup**, which already installs and advances a
    PTS, advances it to
    `min(its_end_time, log_resolved_time)` instead of
    `its_end_time` alone — so the inc backup chain's PTS
    doesn't race ahead of data the log hasn't yet captured.
  - **Rangefeed consumers** (CDC, LDR, PCR) consult a
    cluster registry of active log jobs and **skip
    installing their own PTS** when a log covers their
    span — they can serve catch-up from the log instead,
    relieving the long-running per-consumer PTS pressure
    that motivated this project in the first place.

  Both depend on a cluster-wide registry ("is a log job
  configured for this scope, and what's its current
  flushed frontier?") that the existing protected-timestamp
  cache or a small new system table can host. The log
  writer itself does not own a PTS in this scheme — the inc
  backup's PTS, advanced to the log's frontier minus a
  small slack window, is sufficient.

  Until that registry exists, an implementation may install
  a self-managed log-job PTS at `frontier - 10min` as a
  bring-up shortcut. This is a stopgap, not the design;
  see the implementation TODO in `pkg/revlog/revlogjob`.
- **Tiny files are expensive.** Cloud providers bill against a
  per-object size floor, and small objects also dominate the
  compute / IO / RPC cost of listing, opening, and reading — for
  both producers and downstream readers. Production is
  parallelized for throughput capacity, which means during
  low-traffic periods on a large cluster we risk producing an
  outsized number of tiny files. The job must avoid this; specific
  mitigation (batching across producers, idle-collapse, larger
  segments at the cost of latency, etc.) TBD in the file-
  organization session.
- **Storage is WORM / immutable.** Assume we cannot delete or
  rewrite files (anti-ransomware / compliance protection). The
  layout, segment sizing, and any post-hoc compaction strategy
  must work under this assumption — no after-the-fact merging of
  tiny files, no garbage collection of superseded segments.
- **No new persisted job type.** A flag check at the top of the
  existing BACKUP resumer (`pkg/backup/backup_job.go`) dispatches
  to the log-job branch. Same job table row, same job ID space.

### Design

#### File organization

This subsection stipulates the **shape and properties** of the
log's on-disk organization and the access-pattern reasoning
behind them. Exact path layouts, marker proto schemas,
encoding choices, integrity wrapping, and the discovery walk
live in `pkg/revlog/revlog-format.md` alongside the code.

##### Shape

- **Tick.** Wall-clock-aligned fixed-width time slice
  (working number 10s, may go to 5s for tighter RPO). Every
  change is assigned to exactly one tick by its MVCC
  timestamp; every artifact is associated with exactly one
  tick.
- **Data files.** Internally key-sorted. A tick contains
  zero or more data files. There is **no global key
  ordering across files** — multiple files appear because
  multiple producers contribute concurrently and because a
  single producer may flush multiple files mid-tick. Each
  file is one PUT (no append).
- **Tick close marker.** Per closed tick. Asserts the tick
  is complete (every change for it has been durably
  flushed) and authoritatively lists the tick's data files
  in per-producer flush order. Readers gate on a marker's
  existence to know a slice is complete, and use its file
  list to know what to read.

##### Why this shape

- **Span-subset reads need in-file key sort.** Readers want
  events for arbitrary subsets of the keyspace (one index,
  one table, the whole cluster) — and the choice is the
  reader's, not the writer's. The number of distinct
  subspans is unbounded (a cluster can have millions of
  tables × indexes), so pre-bucketing files by key doesn't
  scale. A file must hold many subspans, and to find one
  efficiently the file must be internally key-sorted —
  which in turn rules out append. One PUT per file.
- **Time-windowed reads need time bucketing.** Once files
  contain mixed key ranges, time is the only other axis a
  reader can use to narrow the working set. Ticks provide
  it: a time-range read enumerates ticks in the window and
  reads the files they reference.
- **Completeness is a first-class primitive.** Many readers
  (restore, rangefeed catch-up serving downstream
  consumers) can only operate on a slice once *every*
  change committed in it has been flushed. The tick close
  marker is what readers gate on to know that.

##### Ordering within a tick

KV rangefeed's per-key guarantee is precise but limited: an
*initial* delivery of an older event for a key never follows
the initial delivery of a newer event for the same key.
Replays may be out of order; first-sight is monotonic in
commit time. No cross-key ordering at all.

A log-served catch-up must preserve the same property. Two
invariants make this work without a sort-merge across the
whole tick:

- **Disjoint producer spans at any given flushorder.** At
  any instant the active producers cover disjoint span
  subsets, so files written by *different* producers at the
  same flushorder cannot contain the same key. Readers
  parallelize freely across producers within a flushorder
  bucket.
- **Flush-order is read-order within a single producer.** A
  producer may write multiple files for the same tick (size
  threshold), and the same key may appear in more than one
  of them. The producer never reorders its rangefeed inputs,
  so reading those files in flush-order preserves whatever
  ordering the rangefeed delivered.

The close marker records each file's flushorder so readers
can apply both invariants. After a crash + rebalance the
same span may be served by a different producer mid-tick;
the resume protocol bumps every producer's flushorder to
`max(prior) + 1` for any open tick (see "Progress
checkpointing"), so prior files sit at strictly lower
flushorder and are read first. Per-key ordering survives
the rebalance via the same mechanism, no special case.

##### Detailed format

`pkg/revlog/revlog-format.md` is the spec for: marker and
data-file naming and prefix structure, the `Manifest` /
`Coverage` / `Schema` / `ValueFrame` proto schemas,
SSTable encoding (Pebble + comparator), the integrity
wrapping (CRC32C placement), the `WithDiff` policy, the
discovery walk (LIST delimiters, lookup-by-time), and the
WORM-consistency rules. The RFC stipulates the *shape* and
*properties*; the format spec stipulates the *encoding*.

#### DistSQL flow / production pipeline

The artifacts in "File organization" describe what is
written. This subsection covers the pipeline that produces
them — who buffers, who sorts, who flushes, and how an idle
cluster avoids producing swarms of tiny files.

A large cluster (hundreds of nodes) cannot assume any single
node has the capacity to log all of its changes; production
is distributed across many producers. Region-pinning of logs
(for data sovereignty) further argues for keeping production
work near the data being logged.

Roles in the flow:

- **Producers (one per node).** The job coordinator calls
  partition-spans against the job's span set to assign each
  producer its own subset. Each producer opens a
  `kvclient/rangefeed` subscription (`WithDiff`) over its
  assigned spans, buffers KV events and checkpoints, and on
  each flush trigger (buffer fills — working number 32MB — or
  rangefeed frontier crosses a tick boundary) sorts the
  buffer by `(key, ts)`, groups by tick, and either:
  - if a per-tick group is large enough to justify its own
    file, PUTs a key-sorted SSTable directly into the tick;
  - if too small, forwards the sorted group to the regional
    sub-coordinator for coalescing.

  After the flush, the producer sends a message to the
  coordinator carrying any directly-flushed file IDs, any
  forwarded sorted buffers, and any rangefeed checkpoints
  observed since the last flush.
- **Regional sub-coordinator (one per region).** Receives
  forwarded buffers from low-volume producers in its region,
  merges them across producers into one coalesced key-sorted
  file per tick, and PUTs that file in-region. If the
  coalesced result is itself small enough, hands it to the
  tick closer for inclusion as the close marker's inline
  tail rather than emitting a separate data file. Only
  metadata (file list, checkpoints) flows from sub-coordinators
  to the job coordinator.
- **Tick closer / metadata writer (job coordinator).**
  Maintains the cluster-wide flushed frontier from incoming
  checkpoints. A tick T becomes closable when
  `min(flushed_frontier) > T.end` across all producers AND
  every contributed data file (producer-direct or coalesced)
  is durably PUT. Then it writes the close marker — the
  tick's data file list in per-producer flush order, plus
  any inline tail. Implicit FIFO of the DistSQL channel
  gives flush order; a per-producer monotonic sequence
  number is cheap insurance against reordering.

This pipeline lets an idle cluster collapse to a single
artifact per tick (the close marker, with inline tail),
while giving each node independence to flush its own files
when it has enough volume to justify them. Producers with
steady traffic write directly; quiet producers piggy-back
on a shared coalesced file or marker tail.

The processor → coordinator message deliberately carries no
per-file key bounds — in steady state each producer's spans
interleave across the keyspace (well-spread replicas), so
bounds collapse to ~(start-of-keyspace, end-of-keyspace)
and buy nothing for span-pruning.

Two coupling notes:

- **Producer durability handshake.** "I forwarded my buffer
  to the coordinator" is *not* "the data is durable." A
  producer may only release a tick's buffer once it observes
  the tick's durable close marker exists (or some equivalent
  ack guaranteeing the coordinator's PUT has happened). If
  the coordinator dies mid-coalesce, producers must be able
  to re-supply.
- **Tick close gating on the descriptor frontier.** Even
  when every data producer has reported past the tick end,
  the job coordinator waits for its descriptor rangefeed
  (see "Span coverage tracking") to also pass that point
  before writing the close marker. Otherwise a late
  descriptor change inside the tick would leave the recorded
  coverage wrong for some events.

TODO(dt): exact "small enough to forward" and "small enough
to inline" thresholds; producer ↔ coordinator wire format
proto; resilience model (producer/coordinator failure
recovery, exactly-once-via-idempotent-PUT story); admission
control.

#### Implementation sketch / package layout

A first cut at where the code lives. Three packages:

- **`revlog`** — library for interacting with the revision log
  in external storage. Used by the backup-side writer (§1),
  the restore-side reader (§2), and the rangefeed-client
  wrapper (§3); it is the shared "S3 API" surface for the log.
  Reader and writer surfaces (exact API TBD):
  - **Reader side:**
    - List closed-tick markers in a time window (the
      time-prefix LIST walk specified in the format doc).
    - GET and parse a close marker.
    - Open a data file as an iterator of decoded events.
  - **Writer side** — a thin wrapper around `sstable.Writer`
    that handles the log's key/value encoding and minimal
    bookkeeping. All buffer / sort / multi-tick / when-to-flush
    logic belongs to the processor; the writer just encodes
    one file. Sketch:
    ```go
    package revlog

    type Writer struct{ /* sstable.Writer + count */ }

    func NewWriter(w io.Writer) *Writer
    func (w *Writer) Add(key roachpb.Key, ts hlc.Timestamp,
                         value, prevValue *roachpb.Value) error
    func (w *Writer) Count() int
    func (w *Writer) Close() error  // finalizes sstable
    ```
    Tests can use `Writer` directly with an in-memory
    `io.Writer`, no DistSQL needed.
  - **Marker writer** — a function the coordinator calls with
    the tick id, file list (in per-producer flush order), and
    optional inline tail. Writes the close marker.
- **`revlog/revlogpb`** — proto definitions. The tick close
  marker (file list, per-producer flush order, optional inline
  tail) and the thin value-frame proto for the SSTable value
  (see §1 "Data file format").
- **`revlog/revlogjob`** — the DistSQL flow described in
  "DistSQL flow / production pipeline" above. Built on
  `revlog`. Not a standalone job — entry point is invoked
  from the top-level fork in the existing BACKUP resumer.

##### Progress checkpointing

The coordinator holds two pieces of in-memory state:

- **Flushed frontier.** The min-across-producers timestamp up to
  which the coordinator has durably recorded data. Advances as
  producer checkpoint events arrive.
- **Open-tick file lists**, one per not-yet-closed tick. Each
  entry is `{filename, flushorder}`. Filename is whatever random
  identifier the producer chose (no need for predictability).
  Flushorder is normally a **per-producer-per-tick** monotonic
  counter — each producer increments its own counter as it
  flushes, so two producers can both have files at flushorder=0
  in the same tick. That's fine: they have disjoint spans by
  construction, so the files are spatially separable and
  interchangeable within the bucket. Readers consume all
  flushorder=0 files (in any interleaved order), then all
  flushorder=1 files, etc.

When the flushed frontier crosses a tick's end the coordinator
**closes the tick**: writes the close marker (file list in
flushorder) and drops the tick from in-memory state.

Every ~1 minute the coordinator **persists** both items to the
job's progress payload:

- The flushed frontier — so producers can resume their
  rangefeed subscriptions from the right point on restart.
- The open-tick file lists — so the eventual close marker
  includes files written before the crash, even if they were
  written by producers no longer on the new plan.

Closed ticks need nothing in the checkpoint — their markers are
already durable on S3.

**On resume:**

1. Read the job's checkpoint.
2. Re-partition spans (the new plan may differ if nodes have
   changed).
3. Hand each producer:
   - Its assigned spans.
   - The flushed frontier (rangefeed subscription start time).
   - For each open tick, a **starting flushorder** = max
     persisted flushorder for that tick + 1 (across all
     producers). Every resumed producer starts its own
     per-tick counter at this value instead of 0, so any new
     flush is at a flushorder strictly above any prior
     incarnation's flushes — even if the post-resume span
     partitioning gives a span to a different producer than
     before.

Events flushed between the persisted frontier and the crash get
re-derived from the resumed rangefeed; the new files have new
random names and are recorded fresh. If the coordinator wrote a
close marker but died before persisting that fact, the tick is
re-closed on resume with the re-derived data — the overwritten
manifest is correct because the catch-up scan re-delivers every
event past the persisted frontier. The pre-crash data files for
the tick become orphaned on storage, but orphaned files are an
inevitable consequence of any write-then-record design and are
harmless (readers only consult files listed in the manifest).
No resume-time LIST walk is needed.

##### Job creation

User-facing syntax: `BACKUP ... WITH REVISION STREAM` (the
SQL keyword is `STREAM` rather than `LOG` to avoid introducing
a new unreserved keyword; the internal subsystem is named
`revlog` regardless). Captured on the BACKUP job's details
proto as a `CreateRevlogJob bool`. The BACKUP runs as it
normally would (taking the inc backup the user asked for). At
some point during execution it also checks whether a log job
needs to exist for this destination:

1. **Look for an existing log job marker** at the
   destination via `LIST(prefix="log/job.latest-", limit=1)`
   on the destination's external storage. The presence of
   any such file means a log job has already been
   established here.
2. **If present, no-op.** BACKUP skips log-job creation and
   continues with its own work. This makes
   `WITH REVISION STREAM` idempotent across repeated /
   scheduled BACKUPs.
3. **If absent, create the log job.** Build a sibling job
   that's a copy of this BACKUP's details, with two
   modifications:
   - Flip the flags: `CreateRevlogJob=false`,
     `RevLogJob=true`. The resumer fork (next subsection)
     keys off `RevLogJob`.
   - **Clear PTS state from the copy.** The parent BACKUP
     may have its own `ProtectedTimestampRecord` set; if
     the sibling inherits it, the sibling's
     `OnFailOrCancel` would `Release` the parent BACKUP's
     PTS by ID — orphaning the parent. (PTS coordination
     between the log job and other PTS-installing jobs is
     spelled out in §1 Concerns "PTS coordination across
     the cluster".)

   After allocating the new job's ID, write the ID into a
   marker file under the destination's `log/` prefix so
   subsequent BACKUPs find it on the existence-check LIST.

The marker name encodes a chronological ordering, so
multiple log-job generations over the destination's
lifetime sort newest-first under a `LIST(prefix=...,
limit=1)`. On the existence check, the BACKUP reads the
latest entry, parses the embedded job ID, and consults the
job's status:

- A **live** prior generation (running / paused) — no-op,
  the existing log already covers this destination.
- A **dead** prior generation (canceled / failed) —
  takeover is allowed: a fresh log job is created and a
  new, lex-greater marker is written. The dead generation's
  artifacts on disk remain (WORM doesn't allow deletion)
  but are inert: they're never referenced from any
  manifest written by the new generation, and random file
  IDs / non-overlapping tick boundaries (the new job's
  frontier starts at the parent BACKUP's end time, after
  any prior run's frontier) prevent collision with new
  writes.

**Multiple destinations**: log jobs are independent per
destination. Two BACKUPs to two different destinations,
both with `WITH REVISION STREAM`, produce two independent
log jobs. The marker dance enforces "at most one live log
job per destination" but imposes no global cluster-wide
limit.

**Where `log/` lives**: at the **backup collection root** —
the same URI the user supplied to `BACKUP ... INTO`, *not*
nested under any individual full-backup directory inside
the collection. A single, well-known location across the
collection's lifetime so rangefeed consumers (CDC, etc.)
can find the log without knowing about any particular full
backup, and so log playback that crosses full-backup
boundaries (e.g. CDC catching up across a fortnight that
spans two full backups) doesn't have to hop directories.

**No "log folder" to speak of**: object storage has no
folders, only files. The job-marker file *is* the "log
exists" indicator; the rest of `log/<prefix>/...` is just
naming convention.

A small race exists if two BACKUPs concurrently see no
marker and both create log jobs: both PUTs to the marker
file succeed (object stores allow PUT-overwrite by default;
WORM buckets configured with object-lock would fail one).
Resolved by serializing the create-or-noop check via a
CockroachDB-side lock (e.g. a system-table row keyed by the
destination URI).

##### Resumer fork

The existing BACKUP resumer in `pkg/backup/backup_job.go` gains
a small fork at the top: if the job's payload says it's a log
job (the `RevLogJob` details flag set in the previous
subsection), hand off to `revlogjob`; otherwise the existing
incremental/full path runs unchanged. No new persisted job
type, no new system-table changes.

##### Lifecycle: pause / resume / replan / crash recovery

All non-running states (pause, resume, replan after a node
blip, full crash recovery) share a single mechanism: **resume
from the persisted frontier**. The Coordinator's
`Progress checkpointing` subsection covers what's persisted;
this one covers how the resumer uses it.

- **First run.** No persisted frontier yet. The Coordinator
  initializes the frontier to **the parent BACKUP's end
  time** — i.e. the timestamp the inc/full BACKUP that
  triggered `WITH REVISION STREAM` finished at. Restore from
  any time T ≥ parent.end_time is then served by
  `parent backup + log replay over (parent.end_time, T]`.
- **Subsequent runs.** Frontier comes from the most recent
  persisted checkpoint, exactly as documented in
  `Progress checkpointing`. Producers re-subscribe their
  rangefeeds from this frontier; events with ts ≤ frontier
  are not re-delivered, events with ts > frontier are
  (re-)captured. **No hole** — every MVCC timestamp in
  `(parent.end_time, current_frontier]` is durably in the
  log.
- **Coordinator retry policy.** Modeled on PCR / other
  long-running jobs: the Coordinator does *not* fail the
  job on transient errors. Instead it retries while making
  progress (frontier is advancing), and pauses the job if
  it's stuck (no progress for some threshold). The job sits
  in paused state until a user / cluster operator
  intervenes; resume proceeds via the standard resume-from-
  frontier path above.
- **Pause is a normal pause.** No special semantics — the
  job suspends, no rangefeeds are open, no PTS advances,
  the persisted frontier stays where it was. Resume picks
  up at that frontier. The window between pause and resume
  is *not* a hole in the log because the catch-up scan on
  resume will deliver every event with ts > frontier from
  KV, including all events committed during the paused
  period (assuming KV still has them — which the
  self-managed PTS at `frontier - 10min` is supposed to
  ensure, but a long pause beyond that 10-minute slack
  risks GC removing data the resume would need).
- **Resume avoids conflicting writes by construction, not by
  PUT semantics.** Conditional PUT (`If-None-Match: *`) is
  not portable across the cloud providers we target, so we
  don't rely on it. Instead: tick close markers are only
  written once the aggregate frontier crosses the tick end,
  the persisted checkpoint records which ticks have already
  been closed, and a resumed coordinator starts from the
  persisted frontier — so it never tries to re-close a tick
  whose marker is already on disk. Data files use random IDs
  so independent collisions are practically impossible. A
  duplicate write that does happen anyway (true coordinator
  race / bug) silently overwrites with byte-identical content
  in the common case; in the pathological case (different
  content) it's a bug we'd catch via downstream failures, not
  via the storage layer.

##### Telemetry / metrics

The log job exposes the standard set an operator needs to
reason about its health:

- `flushed_frontier_lag` — wall-clock minus aggregate
  flushed frontier; the operational measure of "how far
  behind real time is the log right now."
- `descriptor_frontier_lag` — same shape for the
  descriptor rangefeed; an alert here signals that ticks
  are about to start blocking on it.
- `pts_lag` — flushed frontier minus the protected
  timestamp the log relies on; a runaway value here means
  GC is closing in on data the log still needs.
- Per-tick file count and byte size — for capacity
  planning and to spot pathological producer behavior.

TODO(dt): metric definitions live in `pkg/revlog/...`
once an implementation team wires them up.

##### Package path

TBD: `pkg/backup/revlog/` keeps it adjacent to the existing
backup code; `pkg/revlog/` reflects that the package is
consumed by non-backup callers too (§2 restore, §3
rangefeed-client wrapper). Lean toward the latter so the
dependency direction stays clean.

#### Scope and coverage tracking

The log job watches a *scope* — the set of descriptors whose
KV traffic must end up in the log. Scope membership is fixed
in identity at job creation; the resolved span set the
producers actually subscribe to evolves underneath as schema
changes happen within those identities. Three pieces make
this work: a small `Scope` interface the writer holds, a
control-plane signal (the schema descriptor rangefeed at the
coordinator), and an artifact that publishes the resolved
spans (the coverage manifest).

##### Scope: identity-based, sourced from BACKUP

The revlog job is a sibling of a parent BACKUP — it doesn't
exist in isolation. Its scope is therefore exactly what that
parent BACKUP resolved its targets to, captured by ID at job
creation:

- For `BACKUP DATABASE foo`: `ResolvedCompleteDbs = {foo's
  database descriptor ID at parent-BACKUP time}` plus
  `ResolvedTargets` for any tables the planner enumerated.
- For `BACKUP TABLE foo.bar, foo.baz`:
  `ResolvedTargets = {bar.id, baz.id}`, no ExpandedDbs.
- For `BACKUP` (cluster): a `FullCluster` flag. The
  curated system-table allowlist
  (`GetSystemTablesToIncludeInClusterBackup`) is also part of
  the scope — RESTORE needs the system rows, the BACKUP
  includes them, the log has to track them.

The scope is **not** the SQL target string. We never
re-resolve "DATABASE foo" by name; if `foo` is dropped and a
new database is created with the same name, the new
database has a different descriptor ID, is a different
scope, and would need a different revlog. This is a
deliberate divergence from how scheduled BACKUP thinks about
scope (re-resolve by name each run) and aligns the revlog
with how PCR / LDR think about identity.

The predicate is small: a descriptor change matters to the
scope iff
`desc.ID ∈ ResolvedTargets`,
or `desc.ParentID ∈ ResolvedCompleteDbs`,
or (`FullCluster` and `desc.ID` is not the system DB and not
in the cluster-backup exclusion list). The rangefeed
callback runs the predicate; non-matching events are
ignored. The coordinator never imports BACKUP's
target-syntax machinery; that lives entirely behind the
`Scope` interface, whose concrete implementation in
`pkg/backup` calls `spansForAllTableIndexes` (the same
merger BACKUP uses for its own export-spans / PTS) so we
don't duplicate span derivation.

DROP narrows the watched span set. Once a descriptor enters
DROP state, its span exits the resolved set: a RESTORE at a
time after the DROP wouldn't include the table anyway (the
schema state at the restore HLC has it marked dropped), so
the GC tombstones that are the only remaining KV traffic on
the span are pure cost. The descriptor rangefeed keeps
watching the system.descriptor row so we still record the
DROP transition in the schema-delta stream and so we
eventually observe the descriptor's removal from KV.

##### Schema descriptor rangefeed (control plane)

The coordinator subscribes one rangefeed at startHLC to the
`system.descriptor` table span. Descriptor change values
are buffered as they arrive and processed in HLC order on
each rangefeed checkpoint; the per-checkpoint batch drives
all of:

1. **Schema-delta write** for each matching descriptor
   change in the batch — `log/schema/descs/<HLC>/<desc-id>.pb`
   with the new descriptor body, or a nil-payload
   tombstone for deletions (per `revlog-format.md` §3 / §6).
2. **Coverage manifest write** for each distinct scope
   transition in the batch — one coverage entry per
   transition at the transition's `T_change`, so reader
   lookups at any AOST in the batch's window get the right
   span set. Pure metadata changes (column rename, etc.)
   produce schema deltas but no coverage entries.
3. **Replan signal** sent at most once per batch if any
   change widened the resolved span set. Coalescing churn
   per checkpoint avoids a long migration causing one
   replan per descriptor write while still preserving
   per-transition coverage entries.
4. **Tick-close gating.** Each rangefeed checkpoint also
   forwards the coordinator's descriptor frontier; the
   tick close loop only seals ticks whose end is below
   `min(data frontier, descriptor frontier)`. Without this
   gate a late-delivered descriptor change inside an
   already-closed tick would mean the recorded coverage /
   schema state was wrong for some events.
5. **Termination check.** After processing the batch, if
   the resolved span set is empty at the checkpoint ts,
   `Scope.Terminated` is consulted. On true, the writer's
   outer loop exits successfully (see "Termination" below).

##### Coverage manifest (artifact)

A separate object is written whenever the resolved span set
changes — initially at job launch with the starting span
set, then again at each relevant descriptor change. Each
captures the scope spec (e.g. "cluster",
"targets:db:5,desc:7") and the resolved span set effective
from a given HLC forward. The writer may also re-emit the
current coverage unchanged to refresh against bucket TTL on
long-lived logs (the latest entry must remain accessible to
readers; otherwise a coverage gap would appear after the
TTL window).

Readers answer "which spans were covered at time T?" by
looking up the latest entry whose effective HLC ≤ T.
Lookups let consumers distinguish "absent because no event
happened" from "absent because we weren't watching."

Path layout, lookup protocol, and proto schema: see
`pkg/revlog/revlog-format.md` §5.

##### Mid-job span widening: flow restart

A producer's rangefeed subscription is fixed at processor
startup. New spans created after that — a CREATE TABLE in a
covered DB, a CREATE INDEX on a covered table — fall outside
every running producer's subscription, so their writes never
reach the log unless we do something about it. (Tables, not
databases, own keyspace in CRDB; a database is a naming
relationship, not a keyspace allocation, so there's no "wide
DB-level subscription" trick — a CREATE TABLE inside a
covered DB allocates a fresh table ID from the cluster's
counter and its `/Table/<new_id>/...` keyspace lives nowhere
near any existing producer's subscription.)

When the descfeed's per-checkpoint batch contains a
widening change, the descfeed:

1. Adds the newly-introduced spans to the manager's
   per-span frontier at zero so they're visible to the
   producer's per-span bookkeeping for the next flow.
2. Computes the restart start time as
   `min(currentDataFrontier, T_change)` where `T_change`
   is the earliest widening event in the batch.
3. Signals the outer flow loop with the new full span set
   and the restart start time.

The outer loop cancels the running inner flow and re-plans
with the new spans. The new flow's producers open their
rangefeed with `WithInitialScan` and a pre-populated
frontier built from `ResumeStateForPartition`:

- Old spans (already at or beyond the rangefeed's start
  time per the resume state) are skipped by the rangefeed
  library's per-span init-scan gate — `runInitialScan`
  walks the frontier and only scans spans whose ts is
  empty or strictly below the rangefeed's start ts.
- Newly-introduced spans (at zero in the resume state) are
  scanned at the rangefeed's start ts; the scan output
  materializes their pre-existing state, and subsequent
  events stream in via the normal rangefeed path.
- The first flow uses the same machinery, with all
  original-scope spans pre-forwarded to `startHLC` in the
  resume state so the initial scan is a no-op for them
  (otherwise we'd init-scan the entire backed-up keyspace
  and re-export everything).

Per-tick flushorder for any in-flight tick is bumped to
`max(prior flushorder for the tick) + 1` — same machinery
as crash-recovery resume (see "Progress checkpointing").
The new flow's files for the in-flight tick land at
strictly higher flushorder than the old flow's
contributions; per-key revision ordering survives the
restart.

The TickManager and persister are shared across iterations,
so closed ticks and the high-water keep advancing.
Open-tick file lists from the prior incarnation get
included in the eventual close marker through the
rehydrate path.

We do **not** wait for the data frontier to pass
`T_change` before restarting. The initial scan is the
mechanism that bridges the gap: any pre-`T_change` state
of a newly-in-scope span is captured by the scan; events
streamed thereafter cover the rest.

##### Pre-publication captures

A consequence of capturing newly-in-scope spans via initial
scan at `min(currentDataFrontier, T_change)` — which can be
strictly less than `T_change` itself — is that the log's
data files for a newly-introduced span may contain events
at MVCC timestamps earlier than the coverage entry that
brought the span into scope. Concretely, a schema change
that publishes a descriptor at `T_change` may have written
intermediate KV state during a backfill window before
`T_change`; rangefeed catch-up after the initial scan
delivers those pre-`T_change` MVCC versions to the writer,
and they land in the log under the span's keyspace.

This is correct because:

- At the **catalog layer**, SQL invariants (cross-index
  consistency, FK validity, table publicness) only hold
  after the descriptor publication transaction commits at
  `T_change`. Intermediate KV state during a backfill,
  IMPORT, or any other "write KV first, publish desc
  atomically" operation is physically present at the KV
  layer but not exposed via SQL.
- The **sum** of all MVCC revisions of the span through
  `T_change` is, by construction, a valid SQL-ready state:
  the schema-change machinery is responsible for ensuring
  that publishing the descriptor coincides with a coherent
  KV state.
- Restore is **all-or-nothing** per span. The coverage
  manifest is a step function: at AOST `T < T_change`, the
  span is excluded entirely (the prior coverage entry
  doesn't include it) — the log's pre-`T_change` events
  for it are never read. At AOST `T ≥ T_change`, the span
  is included and **every** event in its data files is
  replayed. The sum produces the SQL-valid state at
  `T_change`, and subsequent events bring it forward to
  AOST. There is no AOST that produces a half-published
  state visible to the user.

This invariant — that a downstream consumer can observe
raw KV writes including pre-publication backfill state and,
by replaying through the publication ts, arrive at a
SQL-valid state — is the same one that **PCR / LDR**
relies on. PCR's logical replication subscribes a
rangefeed to source spans, replicates pre-publication
backfill writes verbatim, and trusts that replay through
the descriptor's commit ts produces a consistent
destination state. The continuous backup writer uses the
same mechanism (rangefeed + initial scan) and the same
invariant.

A secondary precedent is **BACKUP introduced spans**: when
an incremental backup discovers a span that came into
scope between full and incremental, it does a
revision-history export over `(0, BackupStartTime]` for
that span, capturing the full MVCC history including any
pre-publication backfill writes. Restore replays the full
set; the same invariant produces a SQL-valid state. The
mechanism differs (bulk export vs. live rangefeed) but
the underlying property is identical.

Reader contract: see `pkg/revlog/revlog-format.md` §5
("Coverage vs. data files"). Readers must gate "is this
span in scope at AOST?" via coverage but must **not**
clip events for an in-scope span by the HLC of its
introducing coverage entry.

##### Termination: scope dissolved

The log job exits successfully when its scope dissolves —
every entry in `ResolvedTargets` is in DROP state or absent
from KV, *and* every entry in `ResolvedCompleteDbs` is in
DROP / absent. Empty resolved spans alone is not enough: a
live database with no tables is a perfectly valid scope
that should keep the descriptor rangefeed watching for a
future CREATE TABLE.

Cluster mode (`FullCluster=true`) effectively never
terminates. The system-table allowlist guarantees a
non-empty span set as long as the tenant exists; the only
path to termination is the tenant itself going away, which
takes the job out via a different mechanism (tenant
teardown stops the resumer).

The check fires after each in-scope descriptor event (the
only point at which scope state can change). On a positive
result the descfeed returns `ErrScopeTerminated`, the
coordinator cancels the flow, and the resumer reports
success.

##### Edge case: chronically slow descriptor rangefeed

If the descriptor rangefeed lags behind the data
rangefeeds, ticks cannot close until it catches up — even
when data is fully flushed. This throttles RPO without
bounding it. A metric on `descriptor_frontier_lag` and an
alert if it exceeds a threshold is enough; remediation
(cancel + restart the descriptor sub, fall back to
alternative detection) is out of scope here.

##### Lifecycle coupling to BACKUP runs

A revlog's scope is anchored to one specific parent BACKUP
run. The job creation dance described above ("Job creation")
runs at the parent BACKUP's execution time and snapshots
*that* run's resolved targets into the new revlog's
details. The revlog then carries those identities for its
entire lifetime. It does not chase the SQL meaning of the
target string into future BACKUPs.

This produces a natural coupling between the revlog's
lifecycle and the BACKUP chain it serves:

- **Initial creation.** First `BACKUP ... WITH REVISION
  STREAM` for a destination resolves its targets, creates
  the parent BACKUP, and (if no live revlog marker exists)
  creates the sibling revlog with roots = those resolved
  IDs.
- **Subsequent runs that match.** Later BACKUPs to the same
  destination find the marker and no-op. As long as the
  revlog's roots are still alive, this is correct: the
  revlog continues covering the same identities the new
  BACKUP would have covered, since name resolution
  produces the same IDs.
- **Subsequent runs that don't match.** If a DROP +
  CREATE has reassigned the target name to a new
  descriptor ID, the prior revlog's roots will eventually
  dissolve (when the old IDs are fully gone) and the
  revlog will terminate successfully. The next BACKUP run
  for that name discovers the marker is terminal (job
  state = succeeded) and creates a fresh revlog with the
  new resolved IDs. Multi-generation marker rotation
  (descending-`now()` suffix; see TODO in the existing
  marker code) is the mechanism that makes this discovery
  possible.

Two consequences worth calling out:

- **Coverage gap between BACKUP runs across an identity
  swap.** Between when a DROP + CREATE swaps the target's
  identity and when the next BACKUP run creates a fresh
  revlog with the new IDs, the new descriptor's data is
  being written but no revlog is watching it. Restore to a
  time in that window is served only by the most-recent
  BACKUP, not by log-replay extending it. The RPO property
  RESTORE provides on top of a BACKUP+log chain is
  "sub-minute RPO continuously, *modulo* the granularity at
  which BACKUPs themselves anchor scope." Customers running
  schedules at hourly cadence (or finer) get hourly-modulo
  RPO across identity swaps.
- **Scheduled BACKUPs are the natural driver.** A
  scheduled `BACKUP DATABASE foo WITH REVISION STREAM`
  running every N minutes both keeps the BACKUP chain
  fresh and re-discovers any scope dissolution
  promptly — each scheduled run is the chance to create a
  successor revlog if the previous one terminated.

#### Schema descriptor capture

Coverage tracking (above) tells a reader **which key spans
were covered at time T**. That's enough for rangefeed
consumers — they only need to know which spans of KV pairs to
expect from the log. RESTORE needs more: to interpret the
encoded KV bytes back into rows, it needs the **table
descriptors** that were in effect when those bytes were
written.

This is a separate artifact stream from coverage, written by
the same control plane (the schema descriptor rangefeed) but
serving a different reader.

##### Schema manifest (artifact)

One object is written **per descriptor per change** —
per-descriptor deltas, not per-event full snapshots. The
base full backup supplies the starting descriptor catalog;
each entry in this stream is a delta applied on top during
log replay.

- For a descriptor change (CREATE, ALTER, schema mutation,
  state transition into DROP): the new descriptor body is
  written verbatim.
- For a descriptor deletion (the system.descriptor row is
  gone from KV): a nil-payload tombstone is written
  (matching the format spec's tombstone framing — the
  4-byte magic-only object).

A single DDL transaction touching multiple descriptors
writes one object per descriptor under the same HLC folder.

Restore walks `log/schema/descs/` in HLC order between the
base backup time and the restore AOST, applying each delta
to the running descriptor set. Rangefeed consumers don't
read this — only RESTORE does.

Path layout, lookup protocol, and proto schema: see
`pkg/revlog/revlog-format.md` §6.

The earlier RFC sketched per-event full snapshots of the
in-scope descriptor set. Deltas are smaller, simpler to
write, and naturally let RESTORE replay descriptor changes
in lockstep with KV events at the same HLC.

##### Relationship to span coverage

Both manifests are written in response to descriptor
changes by the same coordinator, but they're independent
in cadence:

- Coverage entries are written only when the resolved span
  set actually changes. Schema-only changes (column
  rename, default value change) don't move spans and
  produce no coverage entry.
- Schema entries are written for every in-scope descriptor
  change. Schema events are therefore a strict superset of
  coverage events.

Keeping them in separate artifact streams preserves
single-responsibility: rangefeed consumers consult coverage
only; restore consults both. Folding schema into coverage
would force every rangefeed consumer to read descriptors it
never uses.

##### Joint concern with §2 "Schema"

The §2 Restore subsection's "Schema" subsubsection consumes
the schema artifact described here. Must stay coordinated:
the encoding chosen here determines the read API there.

#### TODO(dt): production polish

- Processor count and span partitioning policy.
- Processor restart behavior vs. already-flushed segments.
- The durable handshake between "S3 write succeeded" and
  "advance job's resolved time."
- Admission control / pacing.

---

## 2. Restore job and UX

### Concerns and constraints

- **Restorability check.** The chosen AOST must be ≤ the log's
  resolved time and the prior backup chain + log together must form
  a contiguous cover over the requested spans. Restore planning
  fails clearly otherwise.
- **Locate the base.** Find the most recent prior incremental (or
  full) backup whose end time is ≤ the requested AOST. Restore the
  base via the existing path (online restore is the expected
  default), then replay the log over `[base.end_time, AOST]`.
- **Time-ordered → key-ordered ingest.** The log is time-ordered;
  ingest needs key-ordered SSTs. Volume to rotate is **assumed >
  memory** — design must spill / stream, not buffer in RAM.
  DistMerge-style precedents: IMPORT (`pkg/sql/importer/`), index
  backfill (`pkg/sql/backfill/`).
- **Per-span lifecycle in the window.** A table that existed for
  only part of `[base.end, AOST]` must restore correctly. The log's
  coverage manifest entered-at / left-at metadata flows into restore
  planning so this is handled, not ignored.
- **`SHOW BACKUP` extended.** Surfaces the log's resolved time and
  the restorability window the log adds on top of the discrete
  backup chain. Operators need to be able to inspect "what is
  restorable right now."
- **Idempotency mid-replay.** A restore that fails part-way through
  log replay must be resumable / retryable without corrupting the
  partially-restored state.

### Design

#### Schema

During restore planning, the set of descriptors available for
restore target resolution is constructed by starting with the
descriptors in the base backup and then playing back the changes
recorded in `log/schema/descs/` up to the requested AOST. This
playback modifies, deletes, and adds descriptors chronologically,
producing the actual descriptor set as of the restore time. This
is necessary because descriptors may have been introduced only
in the revision log — e.g., a table created after the last
backup but before AOST — and those must be resolvable as restore
targets.

The resulting descriptor set is what restore planning uses when
resolving the user's `RESTORE` target expressions (database
names, table names, etc.) to concrete descriptor IDs.

Must coordinate with §1 "Schema descriptor capture" — the
encoding chosen there determines the read API here.

#### Log replay pipeline

Restore from a continuous backup is a two-step process: restore
the base backup via the existing path (online restore is the
expected default), then replay the suffix of the log that covers
`(backup.end_time, AOST]`. The log is time-ordered (events are
bucketed into 10-second ticks by MVCC timestamp); KV ingest
requires key-ordered SSTs. The volume of revisions in the replay
window is assumed to exceed available memory, so a streaming /
spilling approach is required. The solution is a three-phase
distributed merge pipeline, reusing the same architectural
pattern as the index backfill distributed merge
(`pkg/sql/bulkmerge`).

##### Phase 1: Tick assignment (planning)

The coordinator discovers all closed ticks in
`(backup.end_time, AOST]` via `LogReader.Ticks()` — the
time-prefix LIST walk specified in the format doc. Ticks are
**shuffled** (deterministic shuffle seeded by the job ID) and
assigned to SQL instances via round-robin. The shuffle is
critical: without it, contiguous ticks would be assigned to the
same node, and traffic spikes during certain time periods would
create skew — one node gets most of the data-heavy ticks while
others sit idle. Shuffling spreads hot ticks uniformly across the
cluster.

Each node receives a `RevlogLocalMergeSpec` proto listing its
assigned ticks, the revlog external-storage URI, a
`nodelocal://` output prefix, and the target AOST.

##### Phase 2: Local merge (per-node)

Each node reads its assigned ticks from cloud storage via the
`revlog` reader API (`GetTickReader`, `Events()`). Events from
multiple ticks are fed into a k-way merge. The merge key is
`(user_key ASC, mvcc_ts ASC)` — the same ordering used within
revlog data files.

**Deduplication.** For each user key, only the latest MVCC
revision with `ts ≤ AOST` is kept — including if that revision
is a tombstone, since we may need to shadow a key present in
the restored backup. Earlier revisions of the same key are
discarded. This is correct because restore wants the state
*as of* AOST, not the full revision history.

**Key prefix rewriting.** Phase 2 also performs source→target
table-ID prefix rewriting using `KeyRewriter` from
`pkg/backup/key_rewriter.go`. Since Phase 2 already allocates
keys to convert from revlog encoding to MVCC `EngineKey`
encoding (the log stores MVCC timestamps in ascending order;
engine keys require descending), key prefix rewriting is done
at the same time with no additional cost. This establishes a
clean boundary: Phase 2 output is entirely in the
**target-keyspace**. Phase 3 can therefore work exclusively in
target-keyspace for span assignment, partitioning, and
ingest — no rewriting logic is needed in the final merge path.

The merged, deduplicated, rewritten output is written as
standard MVCC-encoded SSTs (CockroachDB `EngineKey` encoding,
not revlog encoding) to
`nodelocal://<instance>/crdb-revlog-merge/<job_id>/`. These SSTs
are directly compatible with `bulkmerge.Merge()` and
`AddSSTable` — no re-encoding is needed in Phase 3.

**Key sampling.** Each node collects row samples from its
output stream (reservoir sampling) and returns them alongside
SST metadata (URI, key span, size, key count) to the
coordinator. These samples are used in Phase 3 to identify
spans introduced by the revision log that were not part of
the original restore. Sampling also enables more intelligent
split-point decisions when write volume is heavily skewed —
e.g., one table received a burst of writes dwarfing all
others. Without sampling, split points would be derived from
local SST file boundaries, which reflect time-partitioned
assignment rather than actual key-space density.

##### Phase 3: Final merge and ingest

The coordinator collects all intermediate SST manifests and
key samples from Phase 2. Because Phase 2 performs key prefix
rewriting, all intermediate SSTs are already in the
**target-keyspace** — span assignment and partitioning operate
entirely in the target-keyspace with no further rewriting
needed.

**New span discovery.** The revision log may contain spans
that were not part of the original restore's split-and-scatter
— for example, if a user dropped all tables in a database and
recreated them within the backup chain, the recreated tables
have new span ranges. The coordinator identifies these by
subtracting the spans that were restored in the regular restore
from the key samples returned by Phase 2. The remaining spans
are new — the coordinator chunks them and performs
split-and-scatter so that every span the revision log will
ingest has been assigned to a node.

With all spans accounted for, the coordinator uses
`PartitionSpans` to obtain a partitioning and node assignment
from the existing range layout, then calls
`bulkmerge.Merge()` with this partitioning.

`bulkmerge.Merge()` is **reused as-is** — no fork, no
modification. The intermediate SSTs from Phase 2 are standard
MVCC-encoded SSTs on `nodelocal://`, which is exactly the input
format `bulkmerge` expects. The existing infrastructure handles:

- DistSQL flow planning (MergeLoopback → BulkMerge →
  MergeCoordinator)
- Remote blob streaming via gRPC
- Task assignment with row-sample-based split points
- KV ingestion via `kvStorageWriter` → `SSTBatcher` →
  `AddSSTable`

The only configuration differences from the index backfill
caller: `EnforceUniqueness` is false (the revlog legitimately
contains overwrites of the same key across ticks), and
`WriteTimestamp` is set to `Clock().Now()` (see "Timestamp
handling" below).

##### Why time-partitioned assignment

With N nodes and T ticks, each tick containing F files
(typically 1–10):

- **Time-partitioned** (each node gets T/N complete ticks):
  each node reads all files in its assigned ticks
  sequentially. Total reads across the cluster: T × F. Each
  read is a full GET of the file.
- **Key-partitioned** (each node handles a slice of the
  keyspace across all ticks): every node must open every
  tick's files and seek to its key range. Total reads across
  the cluster: N × T × F. Each read is a partial-file read
  (seek + bounded scan), which on object storage translates
  to a range-GET that is no cheaper than a full GET for small
  files.

For realistic numbers (T = 100K ticks for a ~12-day window at
10s ticks, F = 5 avg files/tick, N = 10 nodes):
time-partitioned = 500K full reads; key-partitioned = 5M
partial reads. The 10× difference is entirely in cloud API
calls, which dominate both cost and latency.

#### Timestamp handling

##### MVCC timestamp rewriting

MVCC timestamp rewriting **must happen in Phase 3**, not Phase 2.
Because work is time-partitioned, a key modified in two different
ticks will be assigned to different nodes. Each node's local
deduplication only sees its own assigned ticks, so both revisions
survive Phase 2 — node A holds the key at `ts=100` and node B
holds it at `ts=200`. During Phase 3's cross-node merge, the
original MVCC timestamps are needed to determine which revision
is newer and should win. If Phase 2 rewrote timestamps to
`Clock().Now()`, all revisions would carry the same timestamp
and the merge could not distinguish them.

Phase 3 (`bulkmerge`'s `kvStorageWriter`) already rewrites MVCC
timestamps — it sets `key.Timestamp = writeTS` for every key
before passing it to `SSTBatcher`. This happens *after* the
merge iterator has resolved cross-node key conflicts using the
original timestamps, so no information is lost.

##### Write timestamp ordering

The existing restore path writes all ingested data at a timestamp
obtained from `Clock().Now()` at the time of ingest (via
`SSTBatcher`). The revlog replay phase runs *after* the main
restore completes. By construction, `Clock().Now()` at revlog
ingest time is later than `Clock().Now()` at main restore ingest
time. Since MVCC resolution picks the latest writer for a given
key, revlog data naturally supersedes backup data for any key that
was modified during the replay window. No special timestamp
coordination, write barriers, or multi-version reasoning is
needed.

The AOST does *not* become the write timestamp — it controls which
revlog events are **included** (the deduplication filter in Phase
2 discards events with `ts > AOST`), not the MVCC timestamp at
which data is written into KV. This matches the existing restore's
timestamp strategy exactly.

#### Integration with existing restore flow

The revlog replay is called at the end of `doResume()` in the
restore job. The feature is gated behind `buildutil.CrdbTestBuild`
and the `RevisionLogTimestamp` field in `RestoreDetails`; when
empty, the replay phase is a no-op and the restore proceeds
exactly as it does today.

#### Syntax and UX

User-facing syntax:
`RESTORE ... FROM ... AS OF SYSTEM TIME <ts>`.
No special option is needed — restore planning detects the
presence of a revision log at the backup location automatically.
`AS OF SYSTEM TIME` is **mandatory** when a revision log is
present (restoring "as of latest" is ill-defined for a live
log).

TODO — planning-time validation steps and `SHOW BACKUP`
extension to surface the log's resolved time and restorability
window.

#### Resumability

TODO — resumability state, on-disk staging area lifecycle, and
idempotency strategy for partial replay.

#### Implementation sketch / key files

| File | Role |
|------|------|
| `pkg/revlog/reader.go` | `LogReader`, `TickReader`, `Event` API |
| `pkg/sql/sem/tree/backup.go` | `RestoreOptions` struct |
| `pkg/jobs/jobspb/jobs.proto` | `RestoreDetails` (`revision_log_timestamp`) |
| `pkg/backup/restore_planning.go` | Restore planning + validation |
| `pkg/backup/restore_revision_log.go` | Orchestration (new file) |
| `pkg/backup/restore_job.go` | `doResume()` call site |
| `pkg/sql/bulkmerge/merge.go` | `bulkmerge.Merge()` (reused as-is) |

---

## 3. Rangefeed client catch-up extension

### Concerns and constraints

- **Status quo cost.** Today's rangefeed consumers (CDC, LDR, PCR)
  install per-job PTS records to protect their catch-up window.
  When a job stalls or falls behind, MVCC garbage accumulates in
  the protected scope and serving traffic on those same tables
  pays the cost.
- **Order awkwardness.** Catch-up today is key-ordered (driven by
  `MVCCIncrementalIterator` in
  `pkg/kv/kvserver/rangefeed/catchup_scan.go`). The log enables
  time-ordered catch-up, which is more natural for replay-style
  consumers and lets GC proceed sooner.
- **Drop-in API surface.** The wrapper must look like
  `pkg/kv/kvclient/rangefeed` to consumers — same event callback,
  same frontier, same resolved-timestamp semantics. Consumers
  shouldn't need consumer-specific changes beyond opt-in.
- **Two-phase with a clean seam.** Phase 1 reads from the log;
  phase 2 hands off to a live KV subscription. The seam must
  produce neither a gap nor a duplicate at the boundary.
- **Coverage gaps.** The wrapper must handle the case where the
  log doesn't cover the requested span or doesn't go back far
  enough to the requested start time. Either fail clearly or fall
  back to the existing PTS-protected path — choice deferred.
- **PTS opt-out.** Initially an opt-in flag (per-job option or
  cluster setting) on the consumer job. Only safe when the log
  demonstrably covers the consumer's span back far enough; opt-out
  is the consumer's assertion that the log will be there for it.
  Eventually this could be automatic; out of scope here.

### Design

#### (TODO — per-component session)

Phase-1 (log replay) ↔ phase-2 (live KV) wiring and exact handoff
criterion (lag threshold? caught-up signal from the log?); overlap
policy at the seam; coverage-gap handling and fallback policy;
schema / system-table event flow through the wrapper; knob shape
for the PTS opt-out (per-job CREATE-time option vs. cluster
setting vs. both); wiring at the `Manager.Protect` call sites in
CDC (`pkg/ccl/changefeedccl/`), LDR (`pkg/crosscluster/logical/`),
and PCR (`pkg/crosscluster/physical/`).
