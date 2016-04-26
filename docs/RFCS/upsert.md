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

Both a long form `INSERT INTO kv VALUES (1, 'one') ON CONFLICT UPDATE SET v =
'one'` and a short form `UPSERT INTO kv VALUES(1, 'one')` will be introduced. In
contrast to MySQL's `REPLACE`, the latter will behave as syntactic sugar for the
long form. The short form will be implemented first.

A row is considered in conflict if any unique index is violated by its
insertion. This means that an `UPSERT` of one `VALUES` tuple can modify more
than one row if there are conflicts in multiple fields.

At runtime, Cockroach will optimistically assume no conflicts and insert as
normal. The transaction batch will be committed and conditional puts will be
used to find conflicting rows (as they already are for `INSERT`). If necessary,
a follow-up batch will be used to update any rows that conflicted. To keep
memory use for the conflicts from being unbounded, large `UPSERT`s will be
broken into batches of alternating inserts and optional updates.


# Drawbacks

* Batches of inserts followed by batches of updates may be confusing if a single
UPSERT affects the same rows many times.


# Alternatives

* We could only implement the long form. However, the short form is easier to
implement and likely covers many of the real-world use cases. When the short
form is sufficient, it's more clear, and once both are implemented there will be
little additional code required to keep the short form.

* We could implement `REPLACE` with MySQL's semantics (a `DELETE` followed by an
`INSERT`) instead of the short form. There are three key differences:

  * When `REPLACE` encounters a conflict, a normal `DELETE` is done and any
applicable `ON DELETE CASCADE` will be triggered. This is fairly surprising and
has historically been a source of confusion. 

  * If some table fields are unspecified, the update will overwrite them with
default values, while `UPSERT` will leave them unchanged.

  * `REPLACE` counts the update case as 2 "affected-rows", while `UPSERT` will
count it as 1.

  If the user really wants this behavior, a `DELETE` followed by an `INSERT` in a
transaction is identical.


# Unresolved questions

* In contrast to `REPLACE`, table constraints will be enforced after the logical
insert or update. TODO(dan): Add details on how this will work once
[#6309](https://github.com/cockroachdb/cockroach/pull/6309) is fleshed out.
