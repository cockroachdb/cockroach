# revlog: on-disk format and layout

This document specifies what the revlog writes to external
storage: paths, framing, encodings, and the invariants the
writer must maintain so readers can rely on them. Motivation,
writer architecture, and reader API contracts live in the
RFC.

The format is shaped by three intents: it must be flushable
often enough to reflect very recent changes; it must support
narrowing to a key-span and time-span subset without scanning
the whole log; and it must be replayable in roughly time
order, so a consumer can close out one time slice before
advancing to the next (rather than streaming all revisions
of one key before moving to the next).

External storage is assumed **WORM**: objects can be created
and read but not modified or deleted. Retention is enforced
by bucket-level TTL.

---

## 1. Time bucketing: ticks

**MVCC time** is sliced into fixed-width **ticks**. Working
width is 10s; the format is parametric in width. Boundaries
are aligned (e.g. `...20s, 30s, 40s...`) so they are
deterministic and need no HLC tail in names.

A tick is identified by its **end time** — the MVCC instant
on its right boundary — formatted as `YYYY-MM-DD/HH-MM.SS`
in UTC (e.g. `2026-04-20/15-30.10`). A tick covers events
with MVCC timestamp in `(tick_start, tick_end]`.

Every change is bucketed by its MVCC timestamp, not by the
wall-clock time of the write. A writer may emit a file for an
old tick after that tick's wall-clock boundary has passed
(late-arriving events such as intent resolution); the tick's
manifest (§4) is what declares it complete.

Lex sort of marker basenames is chronological, and the path
shape (`YYYY-MM-DD/HH-MM.SS`) shares a self-narrowing common
prefix with adjacent times. A flat LIST under the longest
shared prefix of any window's endpoints scales with window
size.

---

## 2. Folder layout

`log/` lives at the **backup collection root** (the URI
passed to `BACKUP ... INTO`), not nested under any individual
full-backup directory.

```
log/
  data/
    <tick-end YYYY-MM-DD/HH-MM.SS>/
      <file_id>.sst
      ...
  resolved/
    <tick-end YYYY-MM-DD/HH-MM.SS>.pb
    ...
  coverage/
    <effective-from-HLC>
    ...
  schema/
    descs/
      <changed-at-HLC>/
        <desc-id>.pb
        ...
```

- `log/data/<tick-end>/<file_id>.sst` — data files
  contributed to a tick. `file_id` is an opaque `int64`; the
  tick manifest references files by ID, never by full path.
- `log/resolved/<tick-end>.pb` — close marker for one tick.
  See §4.
- `log/coverage/<HLC>` — span set the log covered starting at
  the given HLC. See §5.
- `log/schema/descs/<changed-at-HLC>/<desc-id>.pb` — one
  object per descriptor changed at the given HLC. See §6.
  The `descs/` subtree leaves room for sibling subtrees
  (e.g. `tenants/`) later.

---

## 3. Object framing

All non-SSTable objects (tick manifests, coverage manifests,
descriptor entries) share a common framing:

```
[ uint32 little-endian: crc32c(payload) XOR MAGIC ] [ payload ]
```

`MAGIC = 0x62647263` — the ASCII bytes `'c','r','d','b'` read
as a little-endian uint32. Readers XOR the prefix back with
`MAGIC` to recover the claimed CRC, then verify against the
payload.

The XOR'd magic ensures a 4-byte all-zero file is not a valid
framing (the prefix would have to equal `MAGIC`), so accidental
zero / null uploads are unambiguously corruption rather than a
legitimately empty payload. It also acts as a format-version
sentinel: changing `MAGIC` breaks old readers loudly.

**Tombstones use a nil (zero-length) payload**, matching the
KV rangefeed convention for deletions. The 4-byte framing
prefix is still present (and equals `MAGIC` since
`crc32c("") == 0`), so the file is exactly 4 bytes.

Data files (SSTables) don't use this framing: Pebble has its
own block-level and footer checksums.

---

## 4. Tick close marker

Located at `log/resolved/<tick-end>.pb`. A close marker at
this path is the authoritative statement that the tick is
complete (every change for the tick has been durably flushed)
and that the marker's file list is the full set of
contributing data files. Stray files under
`log/data/<tick-end>/` (e.g. left by a crashed or rebalanced
writer) are inert — the reader never sees them.

Format: a marshaled `revlogpb.Manifest` proto, framed per §3.
The proto carries the tick's `(tick_start, tick_end]` MVCC
bounds and the list of contributing data files, each
identified by file ID and tagged with its per-tick flush
order (§8). It may also carry an `inline_tail`: a list of
events (already in `(user_key, mvcc_ts)` order) that the
coordinator coalesced from small per-producer flushes and
chose to inline rather than PUT as a separate data file.
Readers consume `inline_tail` as a virtual data file at
`flushorder = max(file flushorders) + 1`. Exact field
layout per `revlogpb.Manifest`.

---

## 5. Coverage manifest

Records which spans the log covers, so readers can
distinguish "no event happened" from "we weren't watching."

```
log/coverage/<HLC>
```

One object per **coverage epoch**: the HLC at which a span
set took effect. Written at job launch, on every change to
the covered span set, and (optionally) re-emitted unchanged
to refresh against bucket TTL. Lex sort of HLCs is
chronological.

Format: a marshaled `revlogpb.Coverage` proto carrying the
effective HLC, the job's scope spec, and the expanded span
set, framed per §3.

### Coverage vs. data files

Coverage is a step function over time: at any HLC, the
coverage entry whose `effective_from` is the largest value
≤ that HLC defines the in-scope span set. Readers consult
coverage to decide whether a span is in scope at a given
AOST. They do **not** clip events by per-span coverage start.

Specifically: data files for a span S **may** contain events
at MVCC timestamps earlier than the coverage entry that
brought S into scope. When the writer detects that S has
newly entered scope (e.g. via a schema change publishing a
descriptor), it captures S's existing state via an initial
scan stamped at the writer's current data-frontier
position, which can be earlier than the descriptor's
publication timestamp. Subsequent events stream from there
forward.

A reader replaying for AOST `T` must:

- Consult coverage to decide whether S is in scope at `T`.
  If not, ignore S entirely. If so, replay every event for S
  in every data file in the replay window.
- Not attempt to clip events for S by the HLC of the
  coverage entry that introduced S. Doing so would skip the
  initial-scan output that materializes S's pre-entry state,
  producing an incorrect restored state.

This is correct because restore is all-or-nothing per span:
either AOST is before the coverage entry that introduced S
(S is excluded entirely) or AOST is at or after it (S's
full event history in the log replays, and the sum produces
the SQL-valid state at AOST). See the "Pre-publication
captures" section of the continuous backup RFC for the
underlying invariant.

---

## 6. Schema manifest

Records descriptor *changes* over the log's lifetime, so
RESTORE can interpret KV bytes back into rows across schema
boundaries. The base full backup supplies the starting
descriptor snapshot; the schema manifest carries deltas
applied on top. Rangefeed consumers don't read this.

```
log/schema/descs/<changed-at-HLC>/<desc-id>.pb
```

One object per (HLC, descriptor) — a single DDL transaction
touching multiple descriptors writes one object per
descriptor under the same HLC folder.

Format: a marshaled `descpb.Descriptor` framed per §3. No
wrapper proto: the HLC and descriptor ID live in the path.
The format mirrors what was in KV — a *dropped* descriptor
(state DROP) carries its descriptor body; a *deleted*
descriptor (key removed from KV) is written as a nil-payload
tombstone (§3).

---

## 7. Data file format

Data files are **Pebble SSTables** with a custom comparator.

### Key encoding

`(user_key, ascending mvcc_ts)` — note ascending, contra
CockroachDB's standard `EngineKey` (descending). A forward
scan emits per-key revisions oldest-first.

### Value encoding

A marshaled `revlogpb.ValueFrame` carrying the new value and
(when applicable) the prior value. The user key lives in the
SSTable key, not the frame. `prev_value` is populated for
non-insert events when the source rangefeed was subscribed
with `WithDiff`. Exact field layout per `revlogpb.ValueFrame`.

---

## 8. Per-key revision ordering

The format preserves per-key initial-delivery order from the
source rangefeed via the **flushorder** ordinal carried by
each file in the tick manifest (§4):

- Each data file in a tick has a `flushorder` uint.
- Files at lower `flushorder` were flushed before files at
  higher `flushorder`. Within a single producer's
  contributions, this preserves per-key commit order.
- Files at the same `flushorder` do not share keys. Readers
  may process them concurrently / interleaved.
- Readers process a tick's files in ascending `flushorder`,
  then `Manifest.inline_tail` (if present) as a virtual file
  at `flushorder = max(files.flush_order) + 1` — i.e. after
  every real file.

---

## 9. Read protocol

The format supports the following access pattern:

1. **Discover ticks** by flat-LISTing `log/resolved/` under
   the longest common path prefix of the window endpoints
   (§1); results are already in chronological order.
2. **GET each marker**, verifying its framing per §3.
3. **Read each tick's files**: for each manifest, GET each
   `log/data/<tick-end>/<file_id>.sst` and process in
   ascending `flushorder` (§8).
4. **Within a file**: open as a Pebble SSTable with the
   revlog comparator (§7). A forward scan emits per-key
   revisions in commit-time order.

LIST can't be avoided under WORM (no overwriteable index of
closed ticks); the common-prefix narrowing bounds it.

---

## 10. WORM invariants

- No file is ever modified after PUT.
- No tick is ever reopened after its close marker is written.
- Per-tick consumption uses GET on known-keyed objects only.
- Retention is bucket-level TTL. The revlog itself never
  deletes anything.

---

## 11. Reserved

- **Per-file key bounds / blooms / prefix histograms** —
  may be added to `File` if span-pruning becomes worthwhile.
- **`log/schema/tenants/`** and other sibling subtrees under
  `log/schema/` — reserved for non-descriptor schema
  artifacts.

---

## 12. Glossary

- **Tick** — fixed MVCC-time slice (10s working number); the
  unit of log organization.
- **Tick end** — right boundary of a tick, in
  `YYYY-MM-DD/HH-MM.SS` UTC.
- **Close marker / tick manifest** — proto object written
  when a tick is sealed; lists the tick's data files.
- **Data file** — an SSTable holding events for a tick.
- **Flushorder** — per-tick ordinal on each data file
  controlling read-side ordering (§8).
- **WORM** — write-once-read-many; the assumed durability
  model for external storage.
