# revlog: on-disk format and layout

This document is the format specification for the **revlog** —
the continuous, externally-stored revision log of cluster changes.
It describes what is written to external storage (S3-style object
storage), how those artifacts are named and organized, and the
protocol readers use to consume them.

---

## 1. What the revlog is

The revlog is a continuous record of all KV changes occurring in
some configured set of spans (a cluster, a database, a table),
streamed to external storage as those changes happen. It is
written by a long-running backup-flavored job; it is read by:

- **Restore** — replays the suffix of the log over the most
  recent inc backup to restore at sub-minute RPO.
- **Rangefeed catch-up** (CDC, LDR, PCR, etc.) — serves the
  catch-up phase from the log instead of from the cluster's
  MVCC history.

Both readers expect to:

1. Locate events for an arbitrary subset of spans over a time
   slice (typically a short window inside a long retention
   period).
2. Determine whether a chosen time slice is *complete* — every
   change committed in that slice has been durably flushed —
   before consuming it.
3. Receive per-key revisions in a per-key initial-delivery
   monotonic order matching what KV's rangefeed natively
   provides (older events never arrive *for the first time*
   after newer events for the same key; replays may be out of
   order).
4. Receive events, and in particular close out time slices of
   events, if _roughly_ time-order rather than key order eg
   get all events for a minute before advancing to next minute
   rather than getting all events for key A before key B which
   would be the order emitted by a KV-powered catch-up scan.


External storage is assumed **WORM**: objects can be created
and read but not modified or deleted. Retention is enforced
by bucket-level TTL (e.g. 60 days), not by application-level
deletion.

---

## 2. Time bucketing: ticks

Wall-clock time is sliced into fixed-width **ticks**. The
working tick width is 10s. Boundaries are aligned to the wall clock (
e.g. for 10s ticks: `...20s, 30s, 40s...`), so they are deterministic 
and collision-free without needing an HLC tail in names.

Every change is assigned to exactly one tick by its MVCC
timestamp. Every artifact is associated with exactly one tick.

A tick is identified by its **end time** (the wall-clock instant
when it closes), formatted as the path-fragment
`YYYY-MM-DD/HH-MM.SS` in UTC. Example: `2026-04-20/15-30.10` is
the tick whose right boundary is 2026-04-20 15:30:10 UTC.

A tick covers events with MVCC timestamp in
`(tick_start, tick_end]` — exclusive lower, inclusive upper.
**Both bounds are recorded explicitly in the manifest** (see
§4), so a reader can interpret any tick in isolation without
chasing the prior tick to learn its lower bound. The interval
matches rangefeed frontier semantics: when the data frontier
reaches `tick_end`, every event with ts ≤ `tick_end` is
durably accounted for.

For most ticks, `tick_start` equals the prior tick's
`tick_end` and the two ticks form a contiguous boundary. The
exception is the very first tick a log produces, where
`tick_start` is the job's launch HLC (almost never on a tick
boundary). Recording `tick_start` always — instead of only
when it differs from the prior `tick_end` — keeps readers
stateless: no need to fetch the prior marker just to know how
far back the current one starts. The cost is one HLC per
manifest, dwarfed by the file list it sits next to.

Two properties make the format work as a discovery key:

- **Lex sort = chronological sort.** Ordinary string ordering on
  the marker basenames matches time order, so a sorted LIST is
  already in time order.
- **Self-narrowing common prefix.** A reader for a window
  `(start, end]` LISTs the longest path prefix shared by
  `MarkerPath(start.Next)` and `MarkerPath(end)`. The shared
  prefix degrades smoothly: a one-minute window LISTs
  `log/resolved/2026-04-20/15-30.`, a one-hour window LISTs
  `log/resolved/2026-04-20/15-`, a one-day window LISTs
  `log/resolved/2026-04-20/`, and only a multi-year window
  degenerates to LISTing `log/resolved/` entirely.

---

## 3. Folder layout

`log/` lives at the **backup collection root** — the URI the
user passed to `BACKUP ... INTO`. It is *not* nested under
any individual full-backup directory inside the collection.
A single, well-known location across the collection's
lifetime so rangefeed consumers can find the log without
knowing about any particular full backup, and so log playback
that crosses full-backup boundaries doesn't have to hop
directories.

```
log/
  data/
    <tick-end YYYY-MM-DD/HH-MM.SS>/
      <file_id>.sst
      <file_id>.sst
      ...
  resolved/
    <tick-end YYYY-MM-DD/HH-MM.SS>.pb
    <tick-end YYYY-MM-DD/HH-MM.SS>.pb
    ...
  coverage/
    <effective-from-HLC>
    <effective-from-HLC>
    ...
  schema/
    <effective-from-HLC>
    <effective-from-HLC>
    ...
```

- `log/data/<tick-end>/<file_id>.sst` — data files contributed
  to a tick. `file_id` is an opaque `int64` (random or from
  the cluster's unique-int generator); the marker references it
  by ID, never by full path.
- `log/resolved/<tick-end>.pb` — the close marker for one tick.
  One object per closed tick. The tick-end's embedded `/` makes
  the day half a real directory under `resolved/`.
- `log/coverage/<HLC>` — the span set the log was covering
  starting at the given HLC. One file per coverage epoch
  (initial coverage at job launch, plus one per subsequent
  change that affects the resolved span set). See §5.
- `log/schema/<HLC>` — the descriptor set in scope for the
  log starting at the given HLC, for restore to interpret KV
  bytes back into rows during log replay. Written every time
  the descriptor rangefeed delivers a relevant change (more
  often than coverage, since not every descriptor change
  affects spans). Rangefeed consumers don't read this — only
  RESTORE does. See §6.

Splitting `data/`, `resolved/`, `coverage/`, and `schema/`
into separate trees keeps discovery LISTs (which only walk
the relevant subtree) free of unrelated noise.

Discovery cost scales with the window: a one-minute window
LISTs only that minute's markers; a one-day window LISTs only
that day; a multi-year window LISTs all of `resolved/`. See §2
for the common-prefix mechanism.

---

## 4. Tick close marker

Written once per tick, when the tick is sealed. Located at
`log/resolved/<tick-end>.pb`.

A close marker existing at this path is the authoritative
statement that the tick is complete (every change for the tick
has been durably flushed somewhere) and that the marker's file
list is the full set of contributing data files.

Format: `revlogpb.Manifest` proto, conceptually:

```proto
message Manifest {
  // The tick this marker closes. Redundant with the marker's
  // path but useful when parsing the proto in isolation.
  // Encoded as the wall-clock end time, e.g. as an HLC or
  // unix-nanos. Prevents an empty file manifests for ticks with
  // no flushed files.
  ... tick_end = 1;

  // Data files contributed to this tick, with the per-tick
  // flush-order each was assigned at write time. Readers
  // process files in ascending flushorder; ties are
  // interchangeable (see §9).
  repeated File files = 2;

  // The tick's exclusive lower bound. The tick covers events
  // with MVCC ts in (tick_start, tick_end]. Always set, so
  // readers can interpret a tick in isolation. For most
  // ticks this equals the prior tick's tick_end; for the
  // first tick a log produces it equals the job's launch
  // HLC. See §2.
  ... tick_start = 3;
}

message File {
  int64  file_id    = 1;  // identifies log/data/<tick-end>/<file_id>.sst
  int32  flush_order = 2;  // see §9
}
```

`File` deliberately stores only the file ID, not a full path.
Readers reconstruct the path from `(tick-end, file_id)`.

### File integrity: leading checksum

The marker object on disk is a **4-byte little-endian CRC32C
checksum of the marshaled proto**, followed by the marshaled
`Manifest` proto:

```
[ crc32c (4 bytes) ] [ marshaled Manifest bytes ]
```

Readers GET the object, read the first 4 bytes as the checksum,
treat the remainder as the proto, and verify before trusting the
contents. The leading position avoids a separate `Size()` /
`HEAD` round-trip that a trailing checksum would require to
locate the trailer. (A streaming-marshal scheme that computed
the checksum incrementally as the body was written would justify
trailing placement; the manifest is small and we already
buffer-marshal, so leading is strictly simpler.)

Two reasons the wrapping is worth the 4 bytes:

- **Detects corruption and truncation.** A file that was
  silently truncated, partially uploaded, or otherwise mangled
  fails the checksum and is rejected rather than being parsed
  as a valid (but wrong) manifest.
- **Disambiguates "empty content" from "missing/corrupted
  content."** Combined with `Manifest.tick_end` always being
  set, a valid manifest is never a zero-byte file — any
  shorter-than-`4 + len(tick_end_field)` object is
  unambiguously corruption or a partial upload, never a
  deliberate empty-tick marker.

Data files don't add an outer wrapper of their own: Pebble
SSTables have block-level checksums and a footer with its own
checksum already, so corruption / truncation in a data file is
caught by the SSTable reader.

### Why the marker carries an explicit file list

- **Straggler safety under WORM.** A buggy or re-running
  producer that PUTs a file under `log/data/<tick-end>/` after
  the marker is written cannot be deleted; an explicit list
  makes such files inert (the reader never sees them).
- **One GET per tick on the read path.** Readers don't pay
  for a second LIST inside a tick.
- **Coalescing makes the marker authoritative anyway.** A
  tick can have zero data files (data inline), one file, or
  many; only the marker can authoritatively report which.

### Why the marker carries no per-file key bounds

In the steady state, every producer's spans interleave across
the keyspace (well-spread replicas), so per-file
`[min_key, max_key]` collapse to ~(start-of-keyspace,
end-of-keyspace) and pay no span-pruning rent. A reader for a
span-subset query opens every file in the relevant tick(s) and
seeks within each. Per-file bounds may be revisited later if
profiling shows a reason.

---

## 5. Coverage manifest

The set of spans the log covers can change over the log's
lifetime — a new table or index appearing extends coverage; a
dropped table contracts it. Readers need to be able to ask
"which spans were in scope at time T?" so they can distinguish
"absent from the log because no event happened" from "absent
because we weren't watching it."

### Layout

```
log/coverage/<HLC>
```

One object per **coverage epoch**. The object name is the HLC
at which this span set took effect. One file is written when
the log job first launches (the initial coverage); subsequent
files are written whenever the log's covered spans change.
Lex-sort of HLCs is chronological, so a flat
`LIST(prefix="log/coverage/")` returns coverage epochs in
time order.

### Lookup

To find the coverage effective at time T:

1. LIST `log/coverage/`.
2. Find the largest entry whose HLC ≤ T.
3. GET it.

For long-lived logs, a reader may cache the LIST result and
re-LIST only when consuming a tick later than what the cache
covered.

### Format

`revlogpb.Coverage` proto, conceptually:

```proto
message Coverage {
  // The HLC at which this coverage took effect. Redundant
  // with the file name but useful when parsing in isolation,
  // and (per §4) keeps the file from being a 0-byte object.
  ... effective_from = 1;

  // The scope spec the log job was created with — useful
  // for human inspection. (E.g. "cluster", "database:foo",
  // "table:bar".)
  string scope = 2;

  // The expanded span set effective at effective_from.
  repeated roachpb.Span spans = 3;
}
```

The same `[ crc32c (4 bytes) ] [ marshaled proto bytes ]`
leading-checksum wrapping as the tick close marker (§4)
applies, for the same reasons.

### Why a separate folder, not inlined per tick

Coverage typically doesn't change for hours or days at a
stretch; inlining it into every tick close marker would
duplicate it across thousands of markers needlessly. A
separate folder normalizes it.

---

## 6. Schema manifest

The coverage manifest (§5) tells a reader **which key spans
were covered** at time T. RESTORE needs more: to interpret
the encoded KV bytes back into rows, it needs the **table
descriptors** that were in effect when those bytes were
written. Rangefeed consumers don't need this — they only
care about the spans.

### Layout

```
log/schema/<HLC>
```

One object per **schema epoch** — the HLC at which this
descriptor set took effect. Written by the log job's
coordinator every time the descriptor rangefeed delivers a
relevant change. Schema epochs are more frequent than
coverage epochs: every descriptor change writes one, even
ones that don't change the resolved span set (e.g. column
rename on an existing table). Lex sort of HLCs is
chronological, so a flat `LIST(prefix="log/schema/")` returns
schema epochs in time order.

### Lookup

To find the descriptor set effective at time T:

1. LIST `log/schema/`.
2. Find the largest entry whose HLC ≤ T.
3. GET it.

For restore, the natural pattern is to read all schema
entries up to the restore time once at the start of replay
into an HLC → descriptor-set map, then consult that map for
every event during replay.

### Format

`revlogpb.Schema` proto, conceptually:

```proto
message Schema {
  // The HLC at which this schema took effect. Redundant
  // with the file name but useful when parsing in isolation,
  // and (per §4) keeps the file from being a 0-byte object.
  util.hlc.Timestamp effective_from = 1;

  // The full set of descriptors in scope for the log job at
  // effective_from — every table, type, etc. covered. Each
  // entry is a full snapshot; a delta encoding (only changed
  // descriptors with periodic full snapshots) is a possible
  // later optimization if size becomes a problem.
  repeated descpb.Descriptor descriptors = 2;
}
```

The same `[ crc32c (4 bytes) ] [ marshaled proto bytes ]`
leading-checksum wrapping as the tick close marker (§4)
applies, for the same reasons.

### Why separate from coverage

Coverage is read by every consumer of the log (rangefeed
catch-up, restore). Schema is read only by RESTORE. Folding
schema into the Coverage proto would force every rangefeed
consumer to fetch and parse descriptors it never uses.

The two artifacts share a triggering signal (descriptor
rangefeed delivers a change) but a coverage entry is only
written when the resolved span set actually changes, while
a schema entry is written on every descriptor change. The
HLC of the latest schema entry is therefore typically
ahead of (or equal to) the HLC of the latest coverage
entry.

---

## 7. Data file format

Data files are **Pebble SSTables**, written via `sstable.Writer`
with a custom comparator.

### Key encoding

`(user_key, ascending mvcc_ts)`.

- `user_key` is the raw KV-layer key, byte-compared
  lexicographically.
- `mvcc_ts` is the MVCC timestamp, encoded such that a forward
  scan across a single user_key emits its revisions
  oldest-first.

This deviates from CockroachDB's standard `EngineKey` encoding,
which sorts MVCC timestamps *descending* (so `SeekGE(key, ts)`
finds the latest revision at or before `ts` — the natural KV
read pattern). The revlog's read pattern is the opposite:
stream per-key revisions in commit-time order to a downstream
consumer. Ascending encoding makes a forward iterator naturally
emit them in that order, with no per-key buffer-and-reverse.

The trade-off is a custom Pebble comparator and tooling that
doesn't speak Cockroach's standard MVCC encoding out of the
box; both are mild given these files are standalone, never
loaded into the KV store.

### Value encoding

The SSTable value is a serialized `revlogpb.ValueFrame`,
conceptually:

```proto
message ValueFrame {
  // The new value at (user_key, mvcc_ts), as a roachpb.Value
  // (the underlying MVCCValue including header).
  bytes value = 1;

  // The previous value, populated when the producer's
  // rangefeed subscription has WithDiff set and the row
  // existed prior. Absent (zero-length) for inserts and
  // for non-WithDiff producers.
  bytes prev_value = 2;
}
```

The user key is already in the SSTable key, so the frame
deliberately omits it — distinct from existing related shapes
like `kvpb.RangeFeedValue` (which carries the key) and
`streampb.StreamEvent.KV` (which carries `KeyValue`).

### `WithDiff` policy

The log job's producers subscribe rangefeeds with `WithDiff`
unconditionally, so the log is always usable for any downstream
`WithDiff` consumer (most importantly CDC). Storing
`prev_value` inline avoids unbounded cross-tick lookback during
catch-up — a rarely-updated key's prior version could otherwise
sit many ticks back.

A "no diff, smaller files" mode is a possible later tunable.

---

## 8. Read protocol

A reader catching up on a `(t0, t1]` window over some span
subset — start exclusive (the caller already has state at t0,
so events at t0 are not needed), end inclusive (events that
bring state from t0 to t1 live in some tick whose coverage
contains t1). Chained windows `(t0, t1]` then `(t1, t2]`
don't double-count any tick.

Each tick's coverage `(tick_start, tick_end]` is recorded
explicitly in its manifest (see §4), so the **caller doesn't
need to know the tick width**: they say what time window they
want, and the reader yields whichever ticks have any overlap
with it.

Concretely, with 10s ticks at boundaries `t10, t20, t30, t40,
…`, a query for `(t8, t22]` returns `t10, t20, t30` — `t10`
contributes events in `(t8, t10]`, `t20` contributes
`(t10, t20]`, `t30` contributes `(t20, t22]`. `t40` is not
returned: its coverage `(t30, t40]` starts after `t22`.

1. **Discover ticks**: compute the longest common path prefix
   of `MarkerPath(t0.Next)` and `MarkerPath(t1 + maxTickWidth)`
   (see §2) and LIST that prefix flat. The `maxTickWidth`
   widening on the upper side ensures the LIST contains the
   tick whose coverage interval includes `t1`. Sort the
   results lexicographically (= chronologically); for each
   tick whose `tick_end > t0`, GET its manifest and yield it
   if `tick_start < t1` (overlap), or stop iteration if
   `tick_start ≥ t1` (sorted entries that follow can only
   start later).
2. **GET each marker** of interest, verifying its leading
   CRC32C checksum (§4).
3. **Read each tick's files**: for each manifest, `GET` each
   `log/data/<tick-end>/<file_id>.sst`. Process files in
   ascending flushorder; within a single flushorder bucket,
   files may be processed in any interleaved order (see §9).
4. **Within a file**: open as a Pebble SSTable with the revlog
   comparator. Seek to the requested span subset's start key
   and scan forward; per-key revisions emerge in commit-time
   order (oldest-first) due to the ascending-ts encoding.

We can't avoid LIST under WORM — there is no overwriteable
manifest of "all currently-closed ticks" — but the
common-prefix narrowing bounds it.

### Reader API sketch

The `revlog` package exposes the read path as two layers,
matching the discovery vs. consumption split above:

```go
// LogReader discovers closed ticks in the log.
type LogReader struct{ /* external storage handle */ }

// NewLogReader opens the log at the given external storage
// prefix (the `log/` root described in §3).
func NewLogReader(es cloud.ExternalStorage) *LogReader

// Ticks enumerates closed ticks whose end time falls within
// (start, end] (start exclusive, end inclusive — replay
// semantics), in chronological order. Performs the LIST walk
// described above (steps 1–3) using the multi-separator
// delimiter pattern from §2 and parses each marker (verifying
// its leading checksum, §4).
func (lr *LogReader) Ticks(
    ctx context.Context, start, end hlc.Timestamp,
) iter.Seq2[Tick, error]

// Tick is a discovered closed tick — its end time plus the
// parsed manifest (file list with flush_orders).
type Tick struct {
    EndTime  hlc.Timestamp
    Manifest revlogpb.Manifest
}

// GetTickReader opens a reader for one tick, restricted to
// events whose keys fall in any of the requested spans.
// Internally: opens the tick's files in ascending flush_order,
// seeks each to spans[0].Start, advances through the spans
// (skipping keys that fall outside any of them), and merges
// across files at the same flush_order in any order (per §9).
func (lr *LogReader) GetTickReader(
    ctx context.Context, t Tick, spans []roachpb.Span,
) *TickReader

// TickReader iterates the events in one tick, in per-key
// revision order (per §9), restricted to the configured spans.
type TickReader struct{ /* ... */ }

func (tr *TickReader) Events(
    ctx context.Context,
) iter.Seq2[Event, error]

// Event is a decoded change: one entry from a data file's
// (key, mvcc_ts) -> ValueFrame mapping.
type Event struct {
    Key       roachpb.Key
    Timestamp hlc.Timestamp
    Value     roachpb.Value
    PrevValue roachpb.Value  // zero-value if no diff
}
```

A "sharded TickReader" — partitioning a single tick's spans
across multiple consumers for parallel restore / catch-up —
is a natural extension when scale demands it.

---

## 9. Per-key revision ordering

KV rangefeed's per-key guarantee, which the revlog must
preserve: an **initial delivery** of an older event for a key
never follows the initial delivery of a newer event for that
same key. Replays of already-delivered events can come out of
order — consumers must handle that — but on first sight, each
key's events are monotonic in commit time. There is no
cross-key ordering guarantee.

The revlog preserves this through the **flushorder**
mechanism:

### `flushorder` semantics

- Each producer maintains a **per-producer-per-tick**
  monotonic counter. Each time it flushes a file into a tick,
  it tags the file with its current counter value and
  increments. Counter starts at 0 on a fresh tick.
- Different producers can therefore both have files at
  flushorder=0 in the same tick. That's fine: at any instant
  the active producers cover **disjoint spans by
  construction**, so files within one flushorder bucket are
  spatially separable. Readers may parallelize them in any
  order.
- Files at *higher* flushorder are read *after* files at
  lower flushorder. Within a single producer's contributions,
  this preserves per-key commit order (the producer never
  reorders its rangefeed inputs).

### Crash and rebalance

If the job crashes and on resume reassigns a span to a
different producer, the new producer's files for an in-flight
tick may share keys with the prior incarnation's files for the
same span. Per-key ordering is still preserved because the
resume protocol bumps every producer's starting flushorder for
each open tick to **max(prior flushorder for the tick) + 1**
across all producers. The prior incarnation's files thus sit
at strictly lower flushorder and are read first.

### Reader rule

> Process the close marker's `files` in ascending
> `flushorder`. Within a single flushorder value, files may
> be read concurrently / interleaved.

---

## 10. Discovery cost & WORM consistency

- No file is ever modified after PUT.
- No tick is ever reopened after its close marker is written.
- Tick *discovery* uses LIST (which doesn't require mutation).
- Per-tick *consumption* uses GET on known-keyed objects only.
- Retention is bucket-level TTL (e.g. 60 days). The revlog
  itself never deletes anything.

---

## 11. Reserved / out of scope

The format admits future extensions in the spaces below
without breaking the read protocol above. Each is left
unspecified here:

- **`Manifest.inline_tail`** is reserved for the
  small-files-coalescing path. When set, it carries
  per-tick residual data that was too small to justify a
  separate file; readers process it as a virtual data file
  appended at the highest flushorder. When absent (or empty),
  it has no effect.
- **Descriptor revision stream.** The log must capture
  descriptor changes (DDL) so restore and catch-up can
  interpret data correctly across schema boundaries. Likely
  a parallel stream alongside data files; exact shape TBD.
- **Per-file key bounds / blooms / prefix histograms.** Could
  be added to `File` if profiling shows span-pruning would
  help. Today it doesn't.
- **Tick size tunable.** Working number 10s; may go to 5s.
  The format itself is parametric in tick size.

---

## 12. Glossary

- **Tick** — a fixed wall-clock time slice (10s working
  number); the unit of log organization.
- **Tick end** — the right boundary of a tick, in
  `YYYY-MM-DD/HH-MM.SS` UTC (used as a path fragment under
  `log/resolved/` and `log/data/`).
- **Close marker** / **tick manifest** — the proto object
  written when a tick is sealed; lists the tick's data files
  and any inline tail.
- **Data file** — an SSTable holding events for a tick,
  contributed by one producer.
- **Producer** — a DistSQL processor that subscribes
  rangefeeds and flushes data files for its assigned span
  subset.
- **Coordinator** — the DistSQL coordinator that aggregates
  producer reports, advances the flushed frontier, and
  writes close markers when ticks seal.
- **Flushorder** — per-producer-per-tick monotonic counter
  attached to each data file; controls read-side ordering
  (see §9).
- **WORM** — write-once-read-many; the assumed durability
  model for external storage.
