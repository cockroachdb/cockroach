- Feature Name: conversions between time and strings
- Status: completed
- Start Date: 2016-10-06
- Authors: mjibson, dt, knz
- RFC PR: #9790
- Cockroach Issue: #9786 #9762 #14801

# Summary

We and users want functions to convert between strings and time/date
values. Which functions should we provide?

This RFC compares the alternatives:
- do what another RDBMs does
- strftime/strptime
- go format/parse

The outcome of the RFC (with the stated scope to merely establish a
comparison) suggests a tentative advantage for the strftime/strptime
interface, leaving the door open for users to tell us that they would
also like functions using descriptive formats, later.

(Side note: CockroachDB currently provides "experimental" built-in
functions for strptime/strftime using http://github.com/knz/strtime;
the formats supported are symmetric across parsing and formatting.)

# What other RDBMs do

- Pretty much standard: converting via casts. This always uses the
  canonical timestamp/date format; no customization is
  possible. CockroachDB already supports that too.

- PostgreSQL: https://www.postgresql.org/docs/8.1/static/functions-formatting.html

  This interface is pretty reminiscent of COBOL text I/O.  It has a
  myriad of options; there are numerous edge cases and the behavior is
  surprising at times. Rather complex to implement.

  Oracle supports more or less the same interface.

- SQLite: https://sqlite.org/lang_datefunc.html

  Supports `strftime` plus a couple of functions that use
  human-friendly format strings like `"YYYY-MM-DD HH:MM"`. The list of
  supported formats is fixed though.

  No function for parsing.

- MySQL: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html

  `STR_TO_DATE` and `STR_TO_TIME` are pretty much `strptime` in
  disguise with MySQL-specific extensions.
  `DATE_FORMAT` is pretty much `strftime` in disguise and supports the
  same format codes.
  Supports functions to extract individual fields of a time/date:
  `WEEK`, `YEAR`, etc.

- SQL Server (Transact-SQL): https://msdn.microsoft.com/en-us/library/ms187928.aspx

  Uses `CONVERT` with a predetermined fixed set of formats.

# Proposed approaches

The initial implementation in #9762 was to propose
strftime/strptime. Then Matt came up with the argument that Go's
Format/Parse format strings may provide better UX, so we could propose
SQL functions that simply interface with those.

Comparative analysis:

- strftime/strptime:
  - Pros:
    - compatibility: it's what mysql/sqlite already do.
    - mindshare: what most programming languages propose already, so plenty of docs online.
    - usability: enables interleaving arbitrary text and numbers between formatted values.
    - performance: parsing the format string is faster for the underlying function.
  - Cons:
    - no working native Go implementation of strptime as of now,
      so need to use a lib that wraps C's strptime with cgo.
      - This can be alleviated by a Go re-implementation, if/when performance
        becomes a concern.

- Go Format/Parse:
  - Pros:
    - performance: no call via cgo.
    - usability: format strings are more readable (Matt's argument).
	  - Ben: readability here depends on which format is used. For
        "2006-01-02T15:04:05Z07:00" the argument holds, but for
        "060102 15:04:05" (our log format) it's not immediately
        obvious what the first field encodes.
  - Cons:
    - mindshare: quite Go-specific.
    - usability: timezone support is so-so (time not adjusted during parsing)

Regarding usability we can also take note of SQLite which proposes
both strftime and conversions using human-readable formats.

# Additional concerns

As was discovered in the initial implementation, delegating the work
to the underlying C library yields surprising and potentially
incorrect behavior: given the support for format string is different
across platforms and libc versions, a query would yield different
results depending on which node it is run (on an heterogeneous
cluster).

This was averted in #14801 by embedding a particular implementation
common to all Go target platforms.

# Unresolved questions

Where do we want to go?  Currently CockroachDB supports
`experimental_strftime` and `experimental_strptime`. User interest
needs to be gauged, either to retire these built-ins entirely or
migrate them out of `experimental_xxx`.
