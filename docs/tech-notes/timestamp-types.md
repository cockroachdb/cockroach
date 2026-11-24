# TIMESTAMP vs TIMESTAMPTZ Internals

The difference between TIMESTAMPTZ and TIMESTAMP is a little subtle. I think of
it this way: TIMESTAMPTZ represents a single specific instant in time in the
universe, which can be agreed upon by all observers, and TIMESTAMP represents
the abstract notion of "10 AM on Monday, Jan 15" without a particular frame of
reference (i.e. timezone).

This document walks through the transformations that happen to each type from
SQL statement to in-memory Datum to disk encoding, and back.

Originally authored by Claude and Michael Erickson, February 2026.

## Mental Model

Both types use Go's `time.Time` internally and store a Unix timestamp on disk.
The difference is entirely in how timezone is handled at the boundaries (input
and output).

- **TIMESTAMPTZ** represents a real instant in time, stored as UTC.
  Timezone conversion happens at input (to UTC) and output (from UTC to session
  timezone).

- **TIMESTAMP** represents a naive wall-clock reading with no timezone.
  It is stored in a `time.Time` "as if" it were UTC, but the UTC label is a
  lie — no timezone logic is applied at any stage.

## Data Pathways

The following example uses session timezone `America/New_York` (UTC-5 in
January) and input string `'2024-01-15 10:00:00'`.

### TIMESTAMPTZ

```
Input: '2024-01-15 10:00:00'
  │
  │  pgdate.ParseTimestamp: no explicit offset in input, so interpret
  │  wall clock in session timezone →  time.Date(2024,1,15,10,0,0, New_York)
  │  Go converts to UTC internally →  15:00:00 UTC
  │
  ▼
Datum: DTimestampTZ{Time: 2024-01-15 15:00:00 UTC}  (Unix 1705330800)
  │
  │  encoding.encodeTime: stores t.Unix() and t.Nanosecond() directly
  │
  ▼
Disk: [timeMarker, varint(1705330800), varint(0)]
  │
  │  timeutil.Unix(1705330800, 0) → 2024-01-15 15:00:00 UTC
  │
  ▼
Datum: DTimestampTZ{Time: 2024-01-15 15:00:00 UTC}
  │
  │  PGWireFormatTimestamp(t, sessionLoc): t.In(New_York) → 10:00:00-05
  │
  ▼
Output: '2024-01-15 10:00:00-05'
```

If the input had an explicit offset (e.g., `'2024-01-15 10:00:00-05:00'`), the
parser uses that offset instead of the session timezone. The result is the same
UTC instant.

### TIMESTAMP

```
Input: '2024-01-15 10:00:00'
  │
  │  pgdate.ParseTimestampWithoutTimezone: session timezone IGNORED,
  │  any explicit offset DISCARDED.
  │  Wall-clock values placed directly into UTC:
  │    time.Date(2024,1,15,10,0,0, time.UTC)
  │
  ▼
Datum: DTimestamp{Time: 2024-01-15 10:00:00 UTC}  (Unix 1705312800)
  │
  │  encoding.encodeTime: stores t.Unix() and t.Nanosecond() directly
  │
  ▼
Disk: [timeMarker, varint(1705312800), varint(0)]
  │
  │  timeutil.Unix(1705312800, 0) → 2024-01-15 10:00:00 UTC
  │
  ▼
Datum: DTimestamp{Time: 2024-01-15 10:00:00 UTC}
  │
  │  PGWireFormatTimestamp(t, nil): no conversion, format as-is
  │
  ▼
Output: '2024-01-15 10:00:00'
```

Session timezone plays no role. The same bytes go in and come out regardless of
timezone settings.

## Casting Between Types

Both casts depend on the session timezone and are marked `Stable` volatility.
The conversion functions in `pkg/sql/sem/tree/datum.go` are inverses of each
other.

### TIMESTAMP → TIMESTAMPTZ (`AddTimeZone`)

Interprets the wall-clock reading as local time in the session timezone, then
converts to UTC:

```go
_, locOffset := d.Time.In(loc).Zone()         // offset = -18000 (-5h)
t := d.Time.Add(time.Duration(-locOffset) * time.Second)  // +5h → 15:00 UTC
```

```
DTimestamp 10:00 "UTC"  →  DTimestampTZ 15:00 UTC
```

This is an implicit cast (happens automatically in expressions).

### TIMESTAMPTZ → TIMESTAMP (`EvalAtAndRemoveTimeZone`)

Renders the UTC instant in the session timezone, then stores that wall-clock
reading as a naive timestamp:

```go
_, locOffset := d.Time.In(loc).Zone()         // offset = -18000 (-5h)
t := d.Time.Add(time.Duration(locOffset) * time.Second)   // -5h → 10:00 "UTC"
```

```
DTimestampTZ 15:00 UTC  →  DTimestamp 10:00 "UTC"
```

This is an assignment-only cast (requires explicit `CAST()` or assignment
context).

### Round-trip hazard

The round-trip is lossless only if the session timezone is the same for both
casts. Casting TIMESTAMP → TIMESTAMPTZ in `America/New_York` and back in
`America/Chicago` shifts the wall-clock value by one hour.

## Comparisons Across Types

`TimeFromDatumForComparison` in `pkg/sql/sem/tree/datum.go` normalizes both
types to a `time.Time` for comparison. It is equivalent to implicitly casting
TIMESTAMP to TIMESTAMPTZ (via the same offset arithmetic as `AddTimeZone`) and
then comparing the UTC instants.

## Key Source Files

| File | What |
|------|------|
| `pkg/sql/sem/tree/datum.go` | DTimestamp/DTimestampTZ definitions, parsing, `AddTimeZone`, `EvalAtAndRemoveTimeZone`, `TimeFromDatumForComparison` |
| `pkg/util/timeutil/pgdate/field_extract.go` | `MakeTimestamp` (with TZ), `MakeTimestampWithoutTimezone` (without TZ), `MakeLocation` |
| `pkg/util/timeutil/pgdate/parsing.go` | `ParseTimestamp`, `ParseTimestampWithoutTimezone` |
| `pkg/util/encoding/encoding.go` | `encodeTime`, `DecodeTimeAscending` (shared disk format) |
| `pkg/sql/sem/tree/pgwire_encode.go` | `PGWireFormatTimestamp` (output formatting) |
| `pkg/sql/pgwire/types.go` | `writeTextTimestamp` / `writeTextTimestampTZ` (wire protocol) |
| `pkg/sql/sem/eval/cast.go` | Cast dispatch between the two types |
| `pkg/sql/sem/cast/cast_map.go` | Cast context and volatility definitions |
