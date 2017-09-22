- Feature Name: jsonb_scope
- Status: draft
- Start Date: 2017-09-25
- Authors: Masha Schneider, Justin Jaffray
- RFC PR: ...
- Cockroach Issue: [#2969](https://github.com/cockroachdb/cockroach/issues/2969)

# Summary

Support JSONB in CockroachDB.
JSONB is a column type supported by Postgres that allows users to store and
manipulate JSON documents directly in their database.
It's distinguished from Postgres's plain `JSON` column type in that it stores
a binary representation of the documents, rather than the raw JSON string.
The high-level overview of the features we intend to include is:
* JSON Encoding/Decoding
* Key Encoding for JSON values
* A binary JSON format supporting fast field access
* Support for Postgres style JSON operators and some functions
* Support for inverted indexes

As a complement to this RFC, there is a
[repo](https://github.com/cockroachdb/jsonb-spec) outlining the behaviour of
JSON in more detail.

This RFC is primarily concerned with the scope of the JSONB feature, there will
be a follow-up RFC outlining the technical details of the feature, such as the
precise binary encoding.

# Motivation

JSONB is a popular feature of PostgreSQL and one of the most requested features
for CockroachDB, with the
[issue](https://github.com/cockroachdb/cockroach/issues/2969) having by far
the largest number of thumbs-ups of any feature request.
Many users of CockroachDB would prefer to store JSON blobs in columns, either
in addition to a traditional relational schema, or as a replacement of it
(storing all data in a single JSON column, plus a primary key).

Reasons one might want to use JSON with CockroachDB include:
* Ease of storing and manipulating existing JSON data, without having to do a
  manual conversion to a relational schema
* Storing data that the schema author has little-to-no control over in a
  structured, queryable way
* Having access to CockroachDB's transactions and strong consistency while
  working with the ergonomics of a Mongo-style loose-schema database

## Design

Postgres supports two data types - `JSON` and `JSONB`.
Our datatype will be more similar to `JSONB` in its behaviour, and as a result we
will refer to it as `JSONB`, rather than `JSON`.

There are two main directions we could go with the implementation of JSONB -
broadly, we could split JSON objects across kv entries or put them entirely in a
single kv entry.
This is a material decision as it affects what can plausibly be accomplished
with JSONB columns.
We opt to store JSONB blobs entirely within a single kv entry, with an eye
towards keeping the door open to split them up if this becomes a bottleneck for
users.

## Details

### Operators

We plan to support all of Postgres’s JSON manipulation and access operators
(`->`, `->>`, `||`, `-`, `#-`), in addition to the ones which can be accelerated by
inverted indexes (`@>`, `?`, `?|`, `?&`).

[The SQL/JSON spec](http://standards.iso.org/ittf/PubliclyAvailableStandards/c067367_ISO_IEC_TR_19075-6_2017.zip)
specifies a sub-language (“the SQL/JSON path language”, or JSONPath) for
querying data from JSON objects within SQL.
JSONPath support is not in any current release of Postgres, but is slated for Postgres 11.
This is out of scope for 1.2, but may be in scope for 1.3.
The challenges of supporting JSONPath are primarily implementing it in a way that
can rewrite JSONPath queries as SQL operators so our existing SQL machinery can
deal with them efficiently.

### Indexing

Postgres GIN indexing is supported on JSONB columns to speed up queries
involving the `?`, `?|`, `?&`, and `@>` operators.
This is an important feature for users dealing with unstructured data they
don't have much control over, and we should support it.
There will be a dedicated RFC for inverted indexes in Cockroach.

### Lower Priority Features

There are some nice-to-haves that are less essential for the initial version of
JSONB.

These include
* Functions which can be implemented on the client side
* DistSQL support for JSON aggregates
* Computed indexes

# Unresolved questions
