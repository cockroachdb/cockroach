- Feature Name: jsonb_scope
- Status: accepted
- Start Date: 2017-09-25
- Authors: Masha Schneider, Justin Jaffray
- RFC PR: #18739
- Cockroach Issue: [#2969](https://github.com/cockroachdb/cockroach/issues/2969)

# Summary

Support JSONB in CockroachDB.
JSONB is a column type supported by Postgres that allows users to store and
manipulate JSON documents directly in their database.
It's distinguished from Postgres's plain `JSON` column type in that it stores
a binary representation of the documents, rather than the raw JSON string.
The high-level overview of the features we intend to include is:
* JSON encoding/decoding,
* a key encoding for JSON values,
* a binary JSON format supporting fast field access,
* support for Postgres style JSON operators and some functions,
* support for inverted indexes.

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
`JSON` in Postgres refers to JSON data that is stored as text,
preserving whitespace, duplicate keys, and the ordering of keys, and only
verifying that the value of that column is valid JSON. `JSONB` stores only
the actual meaning of the JSON value, throwing away whitespace, duplicate
keys, and key ordering, allowing for more efficient storage, and removing the
need to parse the entire object to retrieve a specific field.

There are two main directions we could go with the implementation of JSONB -
broadly, we could split JSON objects across kv entries or put them entirely in a
single kv entry.
This is a material decision as it affects what can plausibly be accomplished
with JSONB columns.
We opt to store JSONB blobs entirely within a single kv entry, while keeping
the format flexible enough that it could be forwards-compatible with an
eventual multi-kv format if that becomes necessary.

## Details

### Operators

We plan to support all of Postgres’s JSON manipulation and access operators
(`->`, `->>`, `||`, `-`, `#-`), in addition to the ones which can be accelerated by
inverted indexes (`@>`, `?`, `?|`, `?&`).

The behaviour of each of these operators is described below:

| Operator                  | Description                                           | Example                                                                            |
| ------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `->`                      | Access a JSON field, returning a JSON value.          | `'{"foo":"bar"}'::JSONB->'foo'` = `'bar'::JSONB`                                   |
| `->>`                     | Access a JSON field, returning a string.              | `'{"foo":"bar"}'::JSONB->>'foo'` = `'bar'::STRING`                                 |
| <code>&#124;&#124;</code> | Concatenate JSON arrays, or append to a JSON array.   | <code>'[1, 2]'::JSONB &#124;&#124; '[3]'::JSONB</code> = `'[1, 2, 3]'::JSONB`      |
| `-`                       | Removes a key from a JSON object or array.            | `'{"foo": "bar"}'::JSONB - 'foo'` = `'{}'::JSONB`                                  |
| `#-`                      | Removes a path from a JSON object or array.           | `'{"foo": {"bar":"baz"}}'::JSONB #- '{foo,bar}'` = `'{"foo": {}}'::JSONB`          |
| `@>`                      | Path-value check                                      | `'{"foo": {"baz": 3}, "bar": 2}'::JSONB @> '{"foo": {"baz": 3}}'::JSONB"` = `true` |
| `?`                       | Key-contains check                                    | `'{"foo": 4, "bar": 2}'::JSONB ? 'foo'` = `true`                                   |
| <code>?&#124;</code>      | Some key-contains check                               | <code>'{"foo": 4, "bar": 2}'::JSONB ?&#124; '{foo,baz}'</code> = `true`             |
| `?&`                      | All key-contains check                                | `'{"foo": 4, "bar": 2}'::JSONB ?& '{foo,bar}'` = `true`                            |

A more precise specification of the behaviour of these operators can be found
in the [spec repo](https://github.com/cockroachdb/jsonb-spec).


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
There will be a dedicated RFC for inverted indexes in CockroachDB.

### Lower Priority Features

There are some nice-to-haves that are less essential for the initial version of
JSONB.

These include
* Functions which can be implemented on the client side, `JSON_BUILD_OBJECT`
  and [similar functions](https://github.com/cockroachdb/jsonb-spec/blob/master/processing-functions.spec.md)
* DistSQL support for JSON aggregates, `JSON_ARRAY_AGG` and `JSON_OBJECT_AGG`.
* Computed indexes

## Drawbacks

The `?` key-exists operator will collide with our current contextual help
token `?`. We propose to resolve this by changing the contextual help token to
`??`.

# Unresolved questions
