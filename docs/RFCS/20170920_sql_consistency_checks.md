- Feature Name: Table consistency checks
- Status: draft
- Start Date: 2017-09-20
- Authors: Joey Pereira
- RFC PR: [#18675](https://github.com/cockroachdb/cockroach/issues/18675)
- Cockroach Issue: [#10425](https://github.com/cockroachdb/cockroach/issues/10425)

# Summary

A set of table consistency checks that will ensure the consistency of
the table data, including schemas, foreign keys, and indexes.

These checks will be accessible through an API and through queries. They
will run on a regular schedule that is configurable. They will
prospectively also be usable in logictests.
 
There are two main types of checks, SQL checks and KV checks.

# Motivation

Until this point, in Cockroach, there aren't ways to reliably check the
consistency of table data, and it's only detected on failures that often
occur on access. There have been various consistency errors that have
popped up, appearing as mismatched index entries ([#18705][],
[#18533][]) and unchecked foreign keys ([#17626][], [#17690][]). A
significant factor causing these new errors is the addition of code that
manipulates the tables outside of transactions (schema changes, backup,
CSV import). 

In summary, aspects that can be checked include:
- Secondary index entries exist
- Dangling index references (i.e. missing row but index entry retained)
- `CHECK` constraints
- `NOT NULL` constraints
- `UNIQUE` constraints
- `FOREIGN KEY` constraints
- Indexes using [STORING][]. Wrong copies of data in the index (either
  in the index key or the data payload)
- Tables using [FAMILY][]. Primary index data is organized into column
  families as per the TableDescriptor (e.g. there is no extraneous
  column data in a family and there are no extraneous families)
- Invalid data encodings
- Non-canonical encodings (where we can decode the data, but if we
  re-encode it we get a different value.)
- [Composite encodings][] are valid. This includes `DECIMAL` and
  collated strings types
- Key encoding correctly reflects the ordering of the decoded values

Schemas also require checks for the following:
- Invalid parent ID for interleaved tables
- Invalid index definitions (wrong columns)
- Invalid view descriptors: invalid column types; invalid SQL; invalid
  number of columns.
- Invalid column families

Outside of user data, we can also check system tables.
TODO: What checks are relevant here?

By having consistency checks run periodically we can understand if and
when errors are occurring. As a user of cockroach, I will know my data
is correct and safe. Furthermore, these checks will be an asset for
running during tests. For example, they can be run after each acceptance
and logic test.

[#17626]: https://github.com/cockroachdb/cockroach/issues/17626
[#17690]: https://github.com/cockroachdb/cockroach/issues/17690
[#18533]: https://github.com/cockroachdb/cockroach/issues/18533
[#18705]: https://github.com/cockroachdb/cockroach/issues/18705
[STORING]: https://www.cockroachlabs.com/docs/stable/create-index.html#store-columns
[FAMILY]: https://www.cockroachlabs.com/docs/stable/column-families.html
[Composite encodings]: https://github.com/cockroachdb/cockroach/blob/master/docs/tech-notes/encoding.md#composite-encoding

# Guide-level explanation

## Terminology

The **physical table** is the underlying KV pairs behind tables. This is
where we find primary and secondary indexes (as distinct keys),
groupings of certain data (column families), how index data is organized
(index storing conditions), and the encoding of the data.

On the other hand, the **logical table** is the SQL representation of
the data. This is where we think about data as rows and columns. It's
also where we have table schemas and their corresponding constraints
(`UNIQUE`, `CHECK`, ...).

Checks fall into two categories for their types. They are either a check
on the physical table or the logical table. For example, a grouping of
data only exist as the specific value encoded in the KV pairs
corresponding to an SQL row, and as such the only to can access them is
through the KV layer. Secondary indexes are an exception to this strict
bucketing as they exist in physical tables but have been intentionally
exposed through SQL interfaces.

Alongside the check type, there are two main types of implementations of
the checks that will be discussed in this RFC. The first is using SQL
queries to execute the check, which can be used to check the logical
table[0] -- hence we will call them **SQL checks**. As for the physical
layer checks, these need to be done through operating on the KV pairs
directly -- so we will call these **KV checks**. The method for
running KV checks in 

[0]: As an exception, secondary indexes, which exist in the physical
table, can be checked through SQL to some extent. 

## What and how

Checks of both kinds can be manually invoked via the CLI to check either
a database or table and whether to only do a specific check:
```sql
FSCK DATABASE <database> [FOR <checkname>];
FSCK TABLE <table> [FOR <checkname>];
```

SQL checks can be made to run periodically and could appear as jobs.

The SQL checks, as they are composed of SQL, are fairly easy to express
and create. For example, we can check a secondary index size using[1]:
```sql
# Get the index size
SELECT count(*) FROM table@index
# Get the table size
SELECT count(*) FROM table
# Then assert they are equal
```

For KV checks they will be executed in distSQL processors.

TODO: High level of what happens when we fail a check? (Detailed version
below)

[1]: This isn't a fully correct way to check full secondary index
consistency. There are problems that may impact the index size for a net
zero result, causing false positives.

# Detailed design

Key parts of designing the checks are:
- How are they invoked and executed
- How will each check be implemented
- What do we do when a check fails
- How we can repair various failures

## Invocation and execution

As mentioned earlier, all of the checks are invokable by the CLI.

The checks can also be run on a schedule. The frequency can be
configured via a CLI setting (TBD) and with a command line parameter
`--consistency-check-frequency`. For example,
`--consistency-check-frequency=24H` or
`--consistency-check-frequency=2D`.

TODO: How will the SQL checks get incorporated if needed? What happens
when we begin the FSCK call? I'm assuming we can rewrite the expression to make a
custom entry point?

To run KV checks we can execute our check logic within a distSQL
processor[2].

[2]: Implementation detail: we will either need to create the distSQL
plans manually or create a way to express custom distSQL plans in SQL.
The latter probably warrants more discussion, so won't be the likely
option.

## Implementations of checks

SQL checks on the table schema can be implemented in an already existing
`TableDescriptor.Validate`. The new capabilities are:
- Check for invalid parent ID for interleaved tables
- Check for invalid index definitions (wrong columns)
- Check for invalid view descriptors: invalid column types; invalid SQL; invalid
  number of columns.
- Check for invalid column families

The following checks are on the logical table, and so are possible
through SQL checks[3].
- `CHECK` constraints
- `NOT NULL` constraints
- `UNIQUE` constraints
- `FOREIGN KEY` constraints
- Secondary index entries exist
- Dangling index references (i.e. missing row but index entry retained)

The rest of the checks are on the physical table, and so require KV
checks:
- Check for indexes using [STORING][] have the correct data
- Tables using [FAMILY][] have the data organized into families
  correctly
- Invalid data encodings
- Non-canonical encodings (i.e. data round-trips)
- [Composite encodings][] are valid
- Key encoding correctly reflects the ordering of the decoded values

[3]: An exception is secondary indexes, which may be possible to check
on the SQL layer. The skepticism is that simply checking the size isn't
suffice.

### KV checks

Generally, for the KV checks, we can implement them within a distSQL
processor. Note that this is based on my understanding of how this will
be implemented. Please comment on anything that seems wrong! :)

TODO: Where does the initial distSQL planNode come from? What we need is
to construct a plan with our new scanNode for every node containing
ranges with the database/table of being checked.

Instead of the scanNode leafs consisting of TableReaders, they will
consist of TableCheckers, our check processor. This processor will get
the KV spans we're interested in as input and be able to process all of
them. The bulk of the processing code that is being replaced is the
underlying RowFetcher of TableReaders. Within the current RowFetcher,
`RowFetcher.processKv` is where encoding specific work is done and is
where checks can be done in the mirror version we create.

Within here, we can do the following checks on a per-key basis:
- Check composite encodings are correct and round-trip
- Check key encodings correctly reflect ordering of decoded values
- Check the KV pair is well formed (everything is present, no extra
  data, checksum matches)
- For primary keys, verify that the column family 0 (sentinel) is
  present

While iterating through keys some may be interleaved KV pairs from other
tables. These can be checked that they do refer to a table that should
be present.

### Secondary index existence and dangling index references

Checking secondary index existince and dangling index references in the
KV layer is a more complicated matter as it involves multiple KV pairs
which may not be co-located.

Naively, we can test index correctness via. testing
`SELECT COUNT(*) FROM table@index` and comparing that against the table
size. Unfortunately, this isn't completely correct. Under the scenario
when one index is missing and there is either a duplicate index entry or
a dangling index entry, this will be a false positive result.

In order to successfully complete this check then, a pointwise
comparison will need to be done on the indexes. From SQL, we may require
a new distSQL processor to accomplish efficient pointwise comparison.

TODO: Currently the approach for doing this check in the KV layer
efficiently is not yet determined.

### SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)

A few of these checks can be done already via.
`ALTER TABLE ... VALIDATE CONSTRAINT`, but there may be some issues with
attempting to do a *full* check, as indicated by [#17690][].

Additionally, `VALIDATE CONSTRAINT` only supports `CHECK` and
`FOREIGN KEY` types, so support would need to be added for `NOT NULL`
and `UNIQUE`.

Alternatively, we may want to implement this at the KV layer for which
it would be trival to implement `NOT NULL` and `CHECK`.

For `UNIQUE`, we will can likely use simple SQL to check with, which has
distSQL in place to optimize already.
```sql
# Get distinct value count
SELECT DISTINCT(col) FROM table
# Get total value count
SELECT COUNT(col) FROM table WHERE NOT NULL
# Assert they are equal
```

Alternatively, we can use a `GROUP BY ... WHERE ... > 1` to pull out the
specific value violations, but it most likely is not optimized.
```sql
SELECT col, array_agg(row_id)
FROM table
GROUP BY col
WHERE array_length(row_id) > 2
```

[#17690]: https://github.com/cockroachdb/cockroach/issues/17690

## What do we do when a check fails

TODO: what is a user actionable alert we can do?
- Store the failure into a table for display in admin. This can possibly
  include suggestions of how to repair the failure
- Send a sentry report of the failure
- Other user-notifying actions? Email them?

What does a failure look like? Internally? A prospective model may be
the following
```go
type ConstraintCheckFailure struct {
  // The type of check failure. For example, CHECK, PRIMARY KEY, INDEX,
  // ENCODING, DANGLING_SECONDARY_INDEX, ...
  CheckFailure CheckFailureType
  // Database of the failed check.
  Database string
  // Table of the failed constraint.
  Table string
  // Name of the constraint involved, if it's an SQL constaint check.
  ConstraintName string
  // Columns involved in the check failure.
  Columns []string
  // RowID which failed.
  RowID int64
}
```

TODO: Does this fully capure physical table errors? Are there cases with
physical table errors that we can't identify a table?

## Repairing data of checks

As a course of action, we can attempt to repair the failure. There are 3
categories for the repair action we can take.
- Automatically repair the error
- Prompt a manual process to repair the error
- Do nothing -- not repairable, but we can alert the user

In the case of both manual repair and where no repair is possible, we
may need to do more than just let the user know their data is bad.
1) Will this failed data cause further issues?
2) Should we provide a way to tell the database to automatically take
   action? e.g. for a `NOT NULL` violation, always delete violating
   rows?

### Secondary index entries exist

This is simply a matter of creating the key for the secondary index
entry. We need be aware of any column families or other data grouping
parameters (Does the `STORING` parameter impact secondary index
values?).

### Dangling index reference

In this case, as there is no primary data found the only action we can
take is to delete the secondary index entry. Possibilities for what
could have happened here:
- The secondary index entry was not deleted with the row.
- The primary key changed and the reference did not.
- The secondary index key changed but the entry wasn't updated.

For the latter two cases, the secondary index existence check will check
a problem if a new entry was not made. Therefore it's the best action is
to delete the entry.

### SQL constraints (`CHECK`, `NOT NULL`, `UNIQUE`, `FOREIGN KEY`)

These constraints correspond to user-level data and because of this,
there is no best action to take. Instead what we can do is present
several different actions the user can take, including row deletion or
replacing a value.

### Encoding problems

If there are any problems with encoding, there is not much we can do to
repair the data. If the broken data is the value, and the column happens
to be a secondary or primary index, we can reconstruct the value from
the data in the index key. The same applies in the opposite direction.

If we can't do either, the best we can do is decode the data into a row
as much as possible, and present the data to the user.

TODO: I would guess there's a chance that if encoding is broken for one
element, it may be possible that it has broken parts/all of the
remaining data. This is an extreme case, where we can't decode and
present any data to the user. Farfetched case, but I would think it
_can_ happen.

If the key ordering doesn't correctly reflect the decoded value
ordering, we will want to send an alert. TODO: Does this impact the user
and do we need to surface this? I can via. queries with `ORDER BY`, yea?

## Tests

Being able to run the checks during logictests is not a primary focus,
but ideally we can in order to catch consistency errors through testing.

TODO: Is it possible to just run these distSQL queries during tests? What
restrictions exist within logictests?

# Rationale and Alternatives

For SQL checks, the table metadata is accessible in SQL so checking
constraints is trivial there. A subtle concern is if the SQL will get
optimized into something which doesn't check what we want.

Existing code for table schema checks exist, and it's only natural to
fit them into there.

The remaining checks are intricate and involve the KV layer. Majority of
these checks operate on a single KV pair, and so will be easy to
integrate to other areas. distSQL just provides the simplest way to run
the across all ranges without having to worry about other concerns.

## Queue-based checks

Using queue you have access to the KV interface
but are bound to only a single replica and this was brought up as a
problem for some checks (is it?).

## Change data capture and per-transaction checks

Using a stream of data it is possible to make incremental checks that
check only the set of data that has changed. This isn't as thorough as
other checks but this opens the possibility for checking more
frequently.

# Unresolved questions

- Will using txnKVFetcher within our distSQL processor work? Are there
  other alternatives? Reading from `client.Txn`? Pros/cons?
- Can work from RowFetcher be re-used? It's large, and has lots of
  intricate details about KV encoding, which we need to handle.
  Otherwise we're adding a duplicate of every KV encoding detail that
  needs to be changed in tandem.
- Can the results from this RFC benefit FSCK for backups?
- As all existing jobs are triggered by some manual process, how the
  scheduling will be done for SQL checks is TBD. Suggestions welcome. To
  my current knowledge, a simple goroutine run with a timer to initiate
  the jobs will suffice.
- Can index backfilling be problematic for the index checks? Is it
  possible to detect if that is happening then defer checking index
  consistency? We shouldn't have read capabilities yet which we can
  respect to avoid this, yea?
- What can we provide as actionable to a user who has encountered a
  check failure? Can send an alert via Sentry or other means?
