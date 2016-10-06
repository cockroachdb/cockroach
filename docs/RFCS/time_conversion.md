- Feature Name: conversions between time and strings
- Status: draft
- Start Date: 2016-10-06
- Authors: mjibson, dt, knz
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #9786 #9762

# Summary

We and users want functions to convert between strings and time/date
values. Which functions should we provide?

This RFC compares the alternatives:
- do what another RDBMs does
- strftime/strptime
- go format/parse

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

- Go Format/Parse:
  - Pros: 
    - performance: no call via cgo. 
	- usability: format strings are more readable.
  - Cons:
    - mindshare: quite Go-specific.
	- usability: timezone support is borken

Regarding usability we can also take note of SQLite which proposes
both strftime and conversions using human-readable formats.

# Unresolved questions

Where do we want to go?
