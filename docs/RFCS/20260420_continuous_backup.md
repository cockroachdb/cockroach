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
already durable on S3 and discoverable via the LIST walk.

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
random names and are recorded fresh. The narrow race where the
coordinator wrote a close marker but died before persisting
that fact is handled by the resume-time LIST walk — markers
that already exist on S3 win, and the coordinator skips
re-opening those ticks.

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

#### Span coverage tracking

The log's covered spans evolve over time as schema changes
add and drop tables or indexes. Three pieces make this work:
an artifact (the coverage manifest), a control-plane signal
(the schema descriptor rangefeed), and a mid-tick transition
protocol that reuses the crash-recovery flushorder bump.

##### Coverage manifest (artifact)

A separate object is written whenever the covered spans
change — initially at job-launch with the starting span
set, then again at each relevant descriptor change. Each
captures the scope spec (e.g. "cluster", "database:foo")
and the expanded span set effective from a given HLC
forward. Readers answer "which spans were covered at time
T?" by looking up the latest entry whose effective HLC ≤
T. Lookups let consumers distinguish "absent because no
event happened" from "absent because we weren't watching."

Path layout, lookup protocol, and proto schema: see
`pkg/revlog/revlog-format.md` §5.

##### Schema descriptor rangefeed (control plane)

The log job's coordinator subscribes its own rangefeed to the
schema descriptor system table (or the relevant subset for
the log's scope). This rangefeed serves two purposes:

1. **Tick close gating.** A tick can only be sealed once the
   descriptor rangefeed's frontier has advanced past the
   tick's end. Even if every data producer's frontier is past
   the tick end, the coordinator must have evidence that no
   schema change happened during the tick before writing the
   close marker — otherwise a late-delivered descriptor
   change inside the tick would mean the recorded coverage
   was wrong for some events in the tick.
2. **Span-change detection.** When the descriptor rangefeed
   delivers any descriptor change at `T_change`, the
   coordinator **re-resolves the job's target spec** (cluster
   / database / table) into a fresh span set and **compares
   to the currently-covered span set**. If they differ,
   spans changed at `T_change` and the mid-tick transition
   protocol below kicks in. If they match (a descriptor
   mutation that doesn't affect the resolved spans — e.g. a
   column rename, an unrelated table elsewhere in the
   cluster), no replan is needed; the coordinator just
   records that the descriptor frontier has advanced past
   `T_change` and continues. Re-resolve-and-compare is
   simpler and more robust than maintaining a per-scope
   filter that tries to guess which descriptor mutations
   matter.

##### Mid-tick span change protocol

When a span change at `T_change` falls inside an open tick,
the coordinator orchestrates a transition that keeps the
wall-clock-aligned tick model intact:

1. Wait for the data producers' frontier to pass `T_change` —
   so all events with MVCC ts < `T_change` for the *old* span
   set are durably flushed and reported.
2. Cancel the existing flow.
3. Write a new coverage manifest entry effective at
   `T_change` with the new span set.
4. Launch a new flow on the new spans, starting at
   `T_change`. Per-tick flushorder for any in-flight tick is
   bumped to `max(prior flushorder for the tick) + 1` —
   exactly the same machinery as crash-recovery resume (see
   "Progress checkpointing"). The new flow's files for the
   in-flight tick land at strictly higher flushorder than
   the old flow's contributions.
5. The tick eventually closes normally at its wall-clock
   boundary. Its marker contains both flows' files in the
   right order; per-key revision ordering is preserved by
   the standard read rule (lower flushorder first).

The reader has no special case: it sees a normal tick whose
marker happens to list files from two flows. Coverage is
looked up via the coverage manifest, not derived from tick
contents.

The alternative — closing a "stub" tick at `T_change` and
starting a new tick mid-second — would break the
wall-clock-aligned tick invariant, complicate marker
discovery with variable-length ticks, and duplicate
machinery already provided by the resume protocol. Rejected.

##### Edge case: chronically slow descriptor rangefeed

If the descriptor rangefeed lags behind the data rangefeeds,
ticks cannot close until it catches up — even when data is
fully flushed. This throttles RPO without bounding it. A
metric on `descriptor_frontier_lag` and an alert if it
exceeds a threshold is enough; remediation (cancel + restart
the descriptor sub, fall back to alternative detection) is
out of scope here.

#### Schema descriptor capture

Span coverage tracking (above) tells a reader **which key
spans were covered at time T**. That's enough for rangefeed
consumers — they only need to know which spans of KV pairs to
expect from the log. RESTORE needs more: to interpret the
encoded KV bytes back into rows, it needs the **table
descriptors** that were in effect when those bytes were
written.

This is a separate artifact stream from coverage, written by
the same control plane (the schema descriptor rangefeed) but
serving a different reader.

##### Schema manifest (artifact)

A separate object is written every time the descriptor
rangefeed delivers a change. Each captures a snapshot of
the descriptor set in scope at a given HLC — the table /
type / etc. descriptors the log job is covering at that
moment. Restore reads these to interpret KV bytes back into
rows correctly across DDL boundaries during log replay;
rangefeed consumers don't read this — only RESTORE does.

Each entry is a full snapshot of the in-scope descriptor
set — typically a few KB to low MB depending on cluster
size, written infrequently (only on DDL events). A delta
encoding (changed descriptors only, periodic full
snapshots) is a possible later optimization if size
becomes a concern.

Path layout, lookup protocol, and proto schema: see
`pkg/revlog/revlog-format.md` §6.

##### Relationship to span coverage

Both manifests are written in response to descriptor
changes by the same coordinator, but they're independent
in cadence:

- Spans always change as a consequence of descriptor
  changes (spans are derived from descriptors), so a span
  change always coincides with a descriptor change.
- Descriptors can change without spans changing (e.g.
  column rename, default value change, type change on an
  existing column). In that case only the schema manifest
  gets a new entry; the coverage manifest does not (its
  content would be identical to the prior entry).

Keeping them in separate artifact streams preserves
single-responsibility: rangefeed consumers consult
coverage only; restore consults both. Folding schema into
coverage would force every rangefeed consumer to read
descriptors it never uses.

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

Replay reads descriptor revisions from the log and applies them at
the right times across the replay window so that data ingested
between two DDL events is interpreted under the correct descriptor.
Must coordinate with §1 "Schema descriptor capture" — the encoding
chosen there determines the read API here.

TODO — separate session.

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
revision with `ts ≤ AOST` is kept. Earlier revisions of the
same key are discarded. This is correct because restore wants
the state *as of* AOST, not the full revision history.

The merged, deduplicated output is written as standard
MVCC-encoded SSTs (CockroachDB `EngineKey` encoding, not revlog
encoding) to `nodelocal://<instance>/crdb-revlog-merge/<job_id>/`.
These SSTs are directly compatible with `bulkmerge.Merge()` and
`AddSSTable` — no re-encoding is needed in Phase 3.

During the merge, the processor collects **row samples**
(reservoir sampling from the output key stream) for split-point
computation in Phase 3. This comes essentially for free — the
keys are already flowing through the iterator.

Each node returns its SST metadata (URI, key span, size, key
count) and collected row samples to the coordinator.

##### Phase 3: Final merge and ingest

The coordinator collects all intermediate SST manifests and row
samples from Phase 2. It computes key-space split points from the
combined row samples to divide the keyspace into balanced tasks,
then calls `bulkmerge.Merge()` directly.

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

Adding a local merge phase to key-partitioning doesn't help —
each node still has to open all T tick files and seek to its key
range to perform even the local merge. The expensive
many-small-reads-from-S3 pattern moves into the local merge
instead of being eliminated. Time-partitioning moves the sort to
the right place (after the cloud read, before the ingest) rather
than trying to push it down into the cloud read pattern.

#### Timestamp handling

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

After both phases complete, for any user key K:

- If K was not modified in `(backup.end_time, AOST]`, only the
  backup's version exists, written at some `Now_1`.
- If K was modified, the revlog replay wrote a version at
  `Now_2 > Now_1`. The revlog's version is the latest revision of
  K with `ts ≤ AOST` (per Phase 2 deduplication). A reader of the
  restored table sees the revlog's version, which is the correct
  state as of AOST.

#### Integration with existing restore flow

The revlog replay is called at the end of `doResume()` in the
restore job. The feature is gated behind `buildutil.CrdbTestBuild`
and the `RevisionLogTimestamp` field in `RestoreDetails`; when
empty, the replay phase is a no-op and the restore proceeds
exactly as it does today. `AS OF SYSTEM TIME` is required when
`WITH REVISION STREAM` is specified.

TODO — exact placement relative to other restore phases
(preData, mainData, publishDescriptors) and interaction with
online restore.

#### Syntax and UX

User-facing syntax:
`RESTORE ... FROM ... AS OF SYSTEM TIME <ts> WITH REVISION STREAM`.
`REVISION STREAM` is added to `restore_options_list` in the
parser (`sql.y`), with a corresponding `RevisionLog bool` on
`RestoreOptions`. `AS OF SYSTEM TIME` is **mandatory** when
`WITH REVISION STREAM` is specified.

TODO — planning-time validation steps, per-table point-in-time
restore UX, and `SHOW BACKUP` extension to surface the log's
resolved time and restorability window.

#### Resumability

TODO — resumability state, on-disk staging area lifecycle, and
idempotency strategy for partial replay.

#### Implementation sketch / key files

| File | Role |
|------|------|
| `pkg/revlog/reader.go` | `LogReader`, `TickReader`, `Event` API |
| `pkg/sql/parser/sql.y` | `RESTORE` grammar (`REVISION STREAM` option) |
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
