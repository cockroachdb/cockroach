- Feature Name: Time Travel Queries
- Status: completed
- Start Date: 2016-05-07
- Authors: Matt Jibson
- RFC PR: [#6573](https://github.com/cockroachdb/cockroach/pull/6573)
- Issue PR: [#5963](https://github.com/cockroachdb/cockroach/issues/5963)

# Summary

Add support for `AS OF SYSTEM TIME` to `SELECT` statements to execute
queries at some past time. Plumb doing things at a timestamp to various
layers that previously did not need it.

# Motivation

[SQL:2011](https://en.wikipedia.org/wiki/SQL:2011) adds support for time
travel queries. Our most immediate need for them is to support backups
at a certain timestamp. (Consider a huge table being backed up. We need
to page through the data in a way that will be consistent if a connection
drops and needs to be resumed from where it left off.)

The syntax is: `SELECT * FROM t AS OF SYSTEM TIME '2016-01-01 00:00'`.
The term `TIME` will be used below as the time travel time.

# Detailed design

SQL queries already create a KV transaction with a timestamp. Hooking
up the `AS OF SYSTEM TIME` syntax to set the transaction timestamp is
straightforward. This causes older data in the MVCC table to be fetched.

There are other issues that need to be taken into account.

## Table Schema

Before a SELECT, a table lease is obtained to determine the schema. If the
schema has changed after `TIME`, we need to instead fetch the old schema
as well. Since the schemas are stored in the KV store, we can use the same
technique (setting the KV transaction timestamp) to fetch the old data.

Since this query is known to be readonly, it should not trigger a lease even
if `TIME` is now and the schema is current. We just need the descriptor at
`TIME`.

## MVCC Garbage Collection

MVCC currently garbage collects old data after 1 day. If data is requested
at a time before the oldest value, the MVCC layer currently returns empty
data (not an error). This will be changed to return an error instead of an
empty value. The GC will be changed to keep any data required so that it
can respond to read requests within the GC threshold. For example instead
of removing data older than 1d, it will keep any data that was valid within
1d-ago, and discard the rest.

## Timestamp in the Future

The HLC advance itself to the KV timestamp if that timestamp is higher than
any other time value it has seen. Since `TIME` sets the KV timestamp, this
would effectively allow queries to advance the HLC of the system to any
point on the future, which would cause problems. It will thus be an error
to specify `TIME` greater than the value of `cluster_logical_timestamp()`.

# Unresolved questions

Do we care about the read timestamp cache? Do we care if these statements
abort other transactions? If we do care does the TODO about the 250ms from
now offset solve it?
