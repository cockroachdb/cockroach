- Feature Name: UPSERT
- Status: draft
- Start Date: 2016-04-26
- Authors: Daniel Harrison
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: [#1962](https://github.com/cockroachdb/cockroach/issues/1962)


# Summary

`UPSERT` allows a user to atomically either insert a row, or on the basis of the
row already existing, update that existing row instead. Examples include MySQL's
`ON DUPLICATE KEY UPDATE` and `REPLACE`, PostgreSQL's `ON CONFLICT DO UPDATE` or
VoltDB's `UPSERT` statement.


# Motivation

The absence of this feature from Postgres was a long-standing complaint before
being implemented in 9.5. Additionally, it's difficult to use a two column table
as a key/value store without `UPSERT` (as we suggest in response to requests to
make our kv layer public).


# Detailed design

Both a long form `INSERT INTO kv VALUES (1, 'one') ON CONFLICT index DO UPDATE
SET v = 'one'` and a short form `UPSERT INTO kv VALUES(1, 'one')` will be
introduced. In contrast to MySQL's `REPLACE`, the latter will behave as
syntactic sugar for the long form. The short form will be implemented first.

A row is considered in conflict if the specified index is violated by its
insertion (the short form uses the primary index).

In practice, upserts tend to be either insert or update heavy. A user's song
ratings would be mostly inserts but a set of counters would be mostly updates.
There are two options for upsert implementation, one optimizing the insert case
and one the update case. Whichever we pick will be wrong for some uses.

* __Optimize for updates__. Query rows are processed a batch at a time to
prevent unbounded memory usage. A fetch is run to determine which rows have
conflicts. If there are conflicts and the index is a secondary index, a second
fetch is run to get the values needed to compute the update statements. Then
each row is either inserted or updated as appropriate.

  Example (primary key upsert):
  ```sql
INSERT INTO counters (name, val) VALUES('foo', 1) ON CONFLICT name DO UPDATE SET value = value + 1;
```

  The primary key was provided, so the fields needed to compute the update are
fetched directly. If the row didn't exist, we're safe to insert. If it did, a
standard update is run with the fetched values.

  Example (secondary key upsert):
  ```sql
INSERT INTO song_ratings (id, user, song, rating) VALUES(unique_rowid(), 'foo', 'bar', 4) ON CONFLICT song DO UPDATE SET rating = 4;
```

  The key for the specified index is constructed and fetched. If it doesn't
exist, we're safe to insert. Otherwise, the primary key is extracted from the
fetched entry and the values necessary to compute the update are fetched in a
second pass. Finally, the update is run.

* __Optimize for inserts__. Query rows are processed a batch at a time to
prevent unbounded memory usage. It is assumed that there will be no conflicts
and insertion will run normally. The transaction batch is committed and
conditional puts used to find conflicting rows (as they already are for
`INSERT`). If conflicts occur, the index keys of the conflicting rows are
extracted and used to fetch the old values. These are then used to construct a
follow-up batch to update any rows that conflicted.

The update optimized implementation is simpler to implement (the other has
tricky edge cases around kv operations and cleaning up the inserted data when a
conflict occurs). Additionally, the current implementation of CPut will fail a
batch after the first violation and so is unsuitable for implementing the insert
optimized version.

That leaves us with the update optimized implementation. It's possible that in
the future we'll want to implement both and let the user hint which one should
be used.


# Drawbacks

* Having two strategies with different runtime trade-offs is unfortunate.


# Alternatives

* __Optimize for inserts.__ Wait for the Freeze/Unfreeze work to land, then make
it configurable whether a CPut failure aborts the batch. Use this to implement
the strategy described above.

* __Only implement the long form.__ When the short form is sufficient, it's more
clear, and once both are implemented there will be little additional code
required to keep the short form.

* __Implement `REPLACE` with MySQL's semantics__ (a `DELETE` followed by an
`INSERT`) instead of the short form. There are three key differences:

  * When `REPLACE` encounters a conflict, a normal `DELETE` is done and any
applicable `ON DELETE CASCADE` will be triggered. This is fairly surprising and
has historically been a source of confusion. 

  * If some table fields are unspecified, the update will overwrite them with
default values, while `UPSERT` will leave them unchanged.

  * `REPLACE` counts the update case as 2 "affected rows", while `UPSERT` will
count it as 1.

  If the user really wants this behavior, a `DELETE` followed by an `INSERT` in
a transaction is identical.


# Unresolved questions

* In contrast to `REPLACE`, table constraints will be enforced after the logical
insert or update. TODO(dan): Add details on how this will work once
[#6309](https://github.com/cockroachdb/cockroach/pull/6309) is fleshed out.
