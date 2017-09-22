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
* JSON encoding/decoding
* Key encoding for JSON values
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
| <code>?&#124;</code>      | Some key-contains check                               | <code>'{"foo": 4, "bar": 2}'::JSONB ?&#124 '{foo,baz}'</code> = `true`             |
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

## Discussion

### Argument for Computed Indexes

There are two primary ways to think about the utility of JSON columns in SQL:

1. As a data blob without any explicit or implicit schema but whose contents
	 we would like to be able to query on a case-by-case basis.

This is the way GIN thinks about JSON. When you have effectively no knowledge
about the contents of the JSON blobs, you need an indexing scheme which is
aware of that fact and can create indexes that will allow you to query
effectively regardless of the actual shape of your data.

However, there's another case which I think is just as important for the goals
we are trying to accomplish by supporting JSON:

2. As a struct, that is, a piece of data with an (implicit) schema, effectively
	 how MongoDB and other NoSQL databases think about documents. Where each row
	 generally has, more or less, the same set of fields.

This is where computed indexes (and computed columns!) are useful with JSON.
The only way to place a queryable index onto a *particular* field within a JSON
object is via a computed index (whose expression is the extraction of that field).
If a user is thinking about their data in a schema-ful way, but is working with
JSON for the ergonomics of it, they need to be able to structure their indexes
in a way that is efficient for their query patterns as if they were working
with either SQL or a dedicated NoSQL database like Mongo.
Using GIN for this use-case is very wasteful - in effect it is equivalent to
indexing every column of a SQL table, regardless of its query patterns.

By implementing computed indexes by way of computed columns, we get another
compelling use case (that is not supported by PG at the moment, since they
don't have computed columns):
If users can create a computed primary key column as a function of their JSONB
column, we get the full benefit of a NoSQL database, where users can think of
their data purely as a collection of JSON blobs.
Without computed columns, a user of JSON columns needs to manually come up with
a primary key value for every row. In many cases this will be some field
extracted from the JSON blob itself.
This leads to the following flow for inserting a JSON blob in application logic:
1. get JSON blob `j` containing primary key field,
2. extract primary key field as `p = j.k`,
3. insert tuple `(p, j)` as your row.

Note this provides no safety that the user won't accidentally modify their pk
field if it still lives in their JSON blob (short of introducing a `CHECK`
constraint to verify this)!

With computed primary keys, this flow changes into this:
1. get JSON blob `j` containing primary key field,
2. Insert `j` directly into database, CockroachDB automatically extracts `j.k`
	 as the primary key field on the table and prevents modifications to `j` which
	 cause the pk to be changed.

Finally, computed indexes open up a hybrid approach as well. If the primary
column is struct-style, where the fields are mostly known, but it contains some
blob-style field, computed indexes allow one to place a GIN index *only on that
field*, rather than having to manually lift it into its own column which is
then GIN-indexed.

# Unresolved questions
