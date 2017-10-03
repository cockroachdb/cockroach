# Inverted Indexes RFC
- Feature Name: Inverted Indexes
- Status: accepted
- Start Date: 9/22/2017
- Authors: Masha Schneider, Jordan Lewis
- RFC PR: #18992
- Cockroach Issue: #2969

# Summary

Add infrastructure for inverted indexes in CockroachDB, which will permit
indexing arbitrarily-nested primitives in JSON column types in the short term,
array columns in the medium term, and full-text indexing of text columns and
geospatial indexes in the long term.

# Motivation

We’ve committed to supporting JSON column types to make CockroachDB useful for
schema-less data, and for compatibility with Postgres applications that use the
`JSONB` column type. (n.b. this document discusses JSON columns and values, but
in CockroachDB they will be referred to as either `JSON` or `JSONB` depending
on the outcome from the `JSON` implementation RFC) Having JSON columns on their
own is useful, but their usefulness is severely curtailed without an efficient
way to index them, and CockroachDB currently only provides indexes that permit
searching based on comparison to a prefix of a column value. In order to
support JSON in a way that is useful to our customers, we need to be able to
index JSON documents in a more fine-grained way than comparison on value
prefix.

Specifically, the list of use cases that we’ve seen for indexing JSON columns
includes:

- Filtering JSON columns based on existence of top-level keys. For example,
  “give me all rows whose JSON column contains the key `foo` at the top level”.
- Filtering JSON columns based on primitive values of top-level keys. For
  example, “give me all rows whose JSON column contains the key `foo` at the
  top level, and its value is equal to `bar`”.
- Filtering JSON columns based on primitive values of deeply-nested keys. For
  example, “give me all rows whose JSON column has the path `foo.bar.baz =
  qux`”.

While out of scope for immediate implementation, here are some other related
questions that are not possible to efficiently answer with CockroachDB’s
current indexing scheme:

- Filtering array columns based on what array elements they contain. For
  example, “give me all rows whose array column contains the integer 3”.
- Filtering text columns based on what words they contain. For example, “give
  me all rows whose text column contain the words “CockroachDB rocks”.

None of the questions posed by these use cases are efficiently answerable by
CockroachDB’s conventional indexes without full table scans, since they are not
satisfiable by ordinary value prefix comparison operators. We’d like to be able
to answer these questions and others like them in an efficient way; inverted
indexes provide a way to do so.

# Guide-level explanation
## What is an inverted index?

All of the above questions share a common structure: they’re all filters on
components of tokenizable data. Arrays are tokenized by element; text is
tokenized by splitting on whitespace and stop words; JSON is tokenized by
key-value pairs.

An inverted index exploits the tokenizable nature of a data type to permit
efficient searches of the following form:


> Give me all of the rows of table `t` whose tokenizable column `c` contains
> tokens that satisfy my constraints.

Being able to answer this general kind of question efficiently is extremely
powerful, and allows us to satisfy the near-term use cases for JSON above.

----------

**Aside: Why is it called an inverted index?**
A common point of confusion around inverted indexes is their name. What makes
an inverted index any more inverted than a secondary index, which points from a
column value back to the primary key of table?

Originally, inverted indexes were designed for full text search over documents.
In that domain, the forward index allows you to retrieve the list of words
associated with a particular document ID. To implement full text search, you
need to reverse the mapping: instead of pointing from document ID to a list of
words, you would point from word to list of document IDs.

This inversion of the direction of the index is where the name inverted index
comes from.

----------
## Review: Ordinary Indexes in CockroachDB

An ordinary primary or secondary index in CockroachDB maps one or several
column values to an **index key**. In the KV layer this is represented as a key
with the **index key** and maybe some other other column values. The KV layer
is sorted, which makes retrieval of index values fast when you know the index
key. Consider the following table:


    CREATE TABLE t (
          keyCol  INT PRIMARY KEY,
          jsonCol JSONB
    );

In a simplified view of the KV layer, this table would look like the following:

| **Key (table id, index id, value)** | **Value**                                    |
| ----------------------------------- | -------------------------------------------- |
| `t/1/1`                             | `{"x": "a", "y": 7}` |
| `t/1/2`                             | `{"x": "b", "z": {"a": true, "b": false}}`   |
| `t/1/3`                             | `{"y": 7, "z": {"b": false}}`                |

In this case, the keys in the KV have a prefix containing the table name, the
index key and the name of the column for which the value is stored in the value
field. This makes it very fast to run queries where the index key is in the
`where` clause. For example:

    SELECT jsonCol FROM t where keyCol = x;

## Why are ordinary secondary indexes insufficient?

An ordinary index works very well when you’re searching based on prefixes of
sorted data. However, if we want to be able to query over tokenizable data like
in the motivation section, this approach doesn’t save enough information about
the individual components. We need something more sophisticated because the
component of interest might not be a prefix on the whole data structure.

For example, suppose we wanted to query the table above for all rows where
`json``Col->z->b = false`. At a glance it’s easy to see that both rows `2` and
`3` satisfy that constraint.

An ordinary secondary index built by storing the entire `jsonCol` as the key
and the `keyCol` as the value would not help us answer this question
efficiently, since it would be sorted lexicographically by the `valueCol` and
the JSON `"z"` key of the value is not necessarily the first JSON key of
the value. We could get around this if we had computed indexes, by creating an
index on `valueCol->z`, but in order to do that we would need to know that we
were indexing on `z` beforehand. Users of unstructured data types would like
their indexes to work on the whole space *without* specifying the keys to index
on.

## How does inverted indexing solve the problem?

At a high level without going into detail about the physical key encoding, an
inverted index on the above data would look like the following:

| **Key (json path + value)** | **Value (rows whose** `**valueCol**` **contains the key)** |
| --------------------------- | ---------------------------------------------------------- |
| `x:a`                       | `1`                                                        |
| `x:b`                       | `2`                                                        |
| `y:7`                       | `1`                                                        |
| `y:7`                       | `3`                                                        |
| `z:a:true`                  | `2`                                                        |
| `z:b:false`                 | `2`                                                        |
| `z:b:false`                 | `3`                                                        |

The question we posed above `valueCol->z->b = false` is now incredibly natural
to answer with this type of index. Now it’s a straightforward lookup into the
KV, just the way ordinary indexes are.

This general approach to indexing is usable on any tokenizable datatype, such
as arrays, text, and even XML, although we won't be adding support for any of
these datatypes in the near term.

# Reference-level explanation

## Detailed Design: Key encoding

CockroachDB's inverted index for JSON will encode the full JSON path for every
leaf value in a JSON document, similar to Postgres' GIN index with the
`jsonb_path_ops` modifier.

Postgres supports two flavors of inverted indexes for JSON `jsonb_ops` and
`jsonb_path_ops`. Roughly, the difference is that `jsonb_ops` collapses the
hierarchy of the JSON document, whereas `jsonb_path_ops` preserves it. The
result is that `jsonb_ops` produces a larger and less precise index more similar
to an ordinary full-text index.

**`jsonb_ops`**

The first method, `jsonb_ops`, works by adding every JSON key and value from
the indexed column into the index as a key. It’s essentially no different from
full-text search - keys and values are treated almost identically, and all of
the hierarchical information from the input JSON is discarded. Given the
following JSON:

    {"x": "b", "z": {"a": true, "b": false}}

the keys produced would be:

    Ka
    Kb
    Kx
    Kz
    Vb
    Vtrue
    Vfalse

To find the results for the following containment query,

    SELECT * from t WHERE jsonCol @> {"x":"b", "z:"{"a":true}}

we would have to look up the index key for every key and value in the input to
get a set of primary keys per object, then take the intersection of those sets,
and finally re-check the containment condition for all of the rows in that
intersection to make sure that the structure is correct. This is inefficient in
terms of both time and space. In this example, it’s obvious that a very large
number of rows can have `true` as a value in their JSON column so we would have
to perform a join on a very large data set. The resultant index will also be
quite large on disk, since every individual key and value gets indexed.

**`jsonb_path_ops`**

The second type of JSON index that Postgres supports is called
`jsonb_path_ops`. This type of index encodes the full JSON path for every leaf
value into the index key. For the example above, the resultant index keys would
be the following:

    x:b
    z:a:true
    z:b:false

This type of index produces significantly fewer keys, only one per leaf from
original JSON, not one per token. Performing lookups on this encoding is very
efficient for full path queries because it’s just one lookup.

From user feedback we’ve received, users are mostly interested in running
queries supported by the second type. The second type is also more space and
time efficient. Therefore, we propose implementing the second variety of index
initially. Once we implement full text search, we will have the first variety
for free using JSON keys and values as the tokens instead of words.

**Encoding the index**

Inverted indexes are traditionally encoded as a “postings list”, where each key
points to a single compound value containing a list of matching primary index
ids. Traditional RDBMS systems can encode this compound value efficiently by
using their internal row id representations in the list. Because CockroachDB
doesn’t have internal row ids, we would have to implement a traditional
postings list by storing a list of all missing subcomponents of matching
primary keys in the RocksDB value.

There are two disadvantages of doing it this way. Firstly, naive insertion and
deletion would be very inefficient. When modifying the index for a JSON object
we would have to read everything in the KV value, modify it, and then write it
all back, for every single leaf in the JSON. This produces more MVCC garbage on
that set of keys, is more expensive over the wire, and would cause contention
on the set of highly-accessed and frequently written keys. Secondly, since
there’s no bound on the number of rows that might contain a particular path,
one individual key’s value could easily grow past the soft 64 megabyte value
limit for RocksDB. The only way to get around that limit would be to add logic
to split and merge values, adding unnecessary complexity.

Fortunately, CockroachDB has already solved this problem for non-unique
secondary indexes where we encode missing primary key components as part of
the index key. Inverted indexes can reuse this approach to avoid the
disadvantages of the posting list. We can encode both the full JSON path and
the missing primary key components into the index key.

For example, a row with primary key `pk` and JSON column `{"a": "b", "c": "d"}`
in table `1` would produce the following keys in an inverted index with id `2`:

    /1/2/a/b/pk
    /1/2/c/d/pk

As an implementation note, Postgres deviates from this idea by storing a hash
of all of values of the path components as its index key to save space. This
space savings comes at the cost of losing a lot of index capabilities: sub-path
matching, top-level-key matching, and range scans of path and leaf values. This
approach isn’t useful for us, since all of our keys are stored in sorted order
and RocksDB does prefix compression on adjacent keys, so we don’t have to
sacrifice those neat capabilities to save space.

**Physical encoding**

JSON values contain two collection types:

- Objects: collections of key/value pairs
- Arrays: Ordered lists of values

We will encode these two structures into index keys in a very similar way. The
full key schema looks like this:

`/table id/index id/(json object key | arr)…/json leaf value/pk columns…/`

where `arr` is a separator indicating that the next key is part of an array.
There’s only one leaf value per index key, so the start of the primary key
columns always follows the leaf value, which is detectable by its encoding. The
value component of an index KV is left unused. In the future, inverted indexes
could be extended by adding stored columns into the value component like
secondary indexes currently allow with `STORING`.

For example, a row with primary key `pk`, JSON column `{"a": "b", "c": "d"}` and
text column "foo" in table `1` would produce the following key-value pairs in an
inverted index with id `2` that's marked to store the text column:

    /1/2/a/b/pk -> "foo"
    /1/2/c/d/pk -> "foo"

**Key encoding JSON objects**

Encoding JSON objects into the index key is straightforward. Every key in a
path is written to the index key after the index id and is prefixed with a type
tag. Object keys don’t need ordinary type tags, because they can only be
strings. However, we still need to add some sort of tag to distinguish them
from leaf values, which will need to be encoded with a type tag. We’ll pick
`NOTNULL`, which is not used by the leaf value encoding.

JSON leaf values have four types, `null`, boolean, string or number. Since we
don’t know what they are when reading the values back from a key, we’ll need to
tag them with types. CockroachDB already uses type tags for its value encoding,
so we can reuse those here.

For example, if we had a table with id `1` with columns `keyCol` and `jsonCol`
containing the following values:

| keyCol | jsonCol                                    |
| ------ | ------------------------------------------ |
| `pk1`  | `{"x": "b", "z": {"a": true, "b": false}}` |
| `pk2`  | `{"x": "b", "y": null}`                    |

and an inverted index on `jsonCol` with index id `2`, we’d encode the following
index keys:

    1/2/x/b/pk1
    1/2/x/b/pk2
    1/2/y/null/pk2
    1/2/z/a/true/pk1
    1/2/z/b/false/pk1

To reduce clutter, we've dropped the type tags from our examples. For
completeness, the final index key above would look like the following with all
type tags included:

    1/2/NOTNULL/z/NOTNULL/b/BOOL/false/pk1

**Key encoding JSON Arrays**

JSON arrays are ordered lists of other JSON values. For example:

    ["a", 3, [4, 5, 4], [false, true], {foo:bar}]

To encode array membership into an index key, the `ARRAY` type tag is reused to
indicate that the subsequent key component is part of an array. With `arr`
representing the `ARRAY` type tag and assuming the primary key is `pk1`, the
example above would produce the following index keys:


    1/2/arr/3/pk1
    1/2/arr/a/pk1
    1/2/arr/arr/4/pk1
    1/2/arr/arr/5/pk1
    1/2/arr/arr/false/pk1
    1/2/arr/arr/true/pk1
    1/2/arr/foo/bar/pk1

We don’t actually distinguish arrays with an index or name, just a level of
nesting. To understand why, consider the following query:

    SELECT * from t where jsonCol @> [[false]];

This should return the JSON above because there is indeed a value `false`
nested two deep in an array. However, if we included the indices of the array
values, we wouldn’t be able to disambiguate between the inner array `[4,5]` and
`[false, true]`. If we just add a prefix for array containment, we don’t have
this problem.

## Detailed design: operators, index selection, index management

**Supported Operators**

Postgres provides the following indexable operators on JSON:

| @> | Does the left JSON value contain the right JSON path/value entries at the top level? |
| -- | ------------------------------------------------------------------------------------ |
| ?  | Does the string exist as a top-level key within the JSON value?                      |
| ?& | Do all of these array strings exist as top-level keys?                               |
| ?| | Do any of these array strings exist as top-level keys?                               |

Note that, unlike Postgres’ `json_path_ops` index, the proposed index will
support all of the operators above, not just the first one. The reason for this
extra support is that the proposed encoding is lossless with respect to the
structure of the input JSON, unlike Postgres’ lossy hash encoding; searching
for a top-level key is as simple as performing a range scan. For example, given
the column value `{a: {c: 3}}` and the resultant index key `.../a/c/3/`,
completing the query `col ? a` merely requires checking to see if any rows are
available in the keyspan `/a -> /b`.

Even though supporting this feature isn't in scope for initial implementation,
the proposed index key encoding permits range queries and partial path matches
that are not even expressible by Postgres’s `@>` operator semantics. For
example, the query `SELECT * FROM j WHERE col->a->>x > 3`, though inexpressible
via Postgres’ indexable JSON operators, is easily expressed via a scan over
keyspan `/a/3 -> /b`.

**Index Creation**
The syntax of creating an inverted index on a `JSONB` column type should match
Postgres, for compatibility with existing Postgres applications.

Therefore, we must support the following syntax even though we don’t support
Postgres’ GIN indexes in general:

    CREATE INDEX <optional name> ON <table> USING GIN(<column> <opt_ops_specifier>)

where `<opt_ops_specifier>` can be only `jsonb_path_ops`, since that’s the
index mode we’ve chosen to support.

For ergonomics, we might want to also support a syntax like the following:

    CREATE INVERTED INDEX <optional name> ON <table> (<column> <opt_ops_specifier>)

A future extension to inverted indexes to permit stored columns could have the
`STORING` syntax that we currently implement for secondary indexes:

    CREATE INVERTED INDEX <optional name> ON <table> (<column> <opt_ops_specifier>) STORING (<cols>)

The fact that an index is inverted will be stored in its index descriptor,
which will inform the index encoder/decoders and index selection algorithms how
to behave correctly.

Inverted indexes will be physically backfilled and updated in the same fashion
as ordinary secondary indexes.

**Index Selection**

Once we have an inverted index created, when do we choose to use it?

If the query contains a constraint against an indexed JSON column that uses any
of the above operators, we add the inverted index to the set of index
candidates.

In general, the priority of indexes during index selection is determined by
sorting the index constraints by *selectivity*, an estimate of how good the
index will be at constraining the number of rows it returns given the
operations that constrain it. At the time of this writing, selectivity is
determined by a heuristic that takes into account the number of constrained
index columns, whether or not the index is covering, and the order of the
index. Notably, there’s no effort made to guess how many rows will be returned
by a given constraint, since we don’t currently collect table statistics.

Since the inverted index might have a very large number of matching rows per
index key, in general we have to assume that it has fairly low selectivity.
It’s not exactly clear how to weight an inverted index versus an
incompletely-constrained unique index on the same table, since we don’t have
table statistics that inform us of the selectivity of a partial match of the
other index. If a user is performing a containment query on an indexed column,
though, we can probably make the assumption that they want to use the index.
So, we’ll weight a JSON column constraint quite high - but lower than a
fully-constrained unique index.

To give users recourse in the case where these heuristics cause an undesirable
index to be picked, the explicit index selection operator `table@index` will
also be supported for inverted indexes.

If we guessed that keys in JSON values roughly follow a uniform distribution,
one might be able to guess the selectivity of a JSON inverted index constraint
by counting the number of keys in the path to search, with the rationale that
the longer the path constraint, the less likely it is that documents contain
that path. However, it’s clear that JSON values aren’t in general uniformly
distributed, since it’s very common to have "hot paths" in a particular JSON
schema, so this estimation doesn’t seem all that useful.

**Index usage**

Unlike ordinary secondary indexes, inverted indexes behave differently when
they are constrained multiple times. The idea is that, since the index is
inverted and contains all of the primary keys that contain the value in the
constraint, multiple constraints are implemented by checking each constraint
individually and intersecting the resultant primary keys. For example, the
containment query `col @> '3' AND col @> 'b'` requires 2 index scans: one for
all the rows that contain `3`, and one for all the rows that contain `b`. The
resultant row keys are then intersected to produce the final result.

`OR` constraints, on the other hand, result in the output sets being unioned as
normal.

Containment queries that search for objects that contain multiple leaf values
are similar - they can be treated as a conjunction of constraints that each
contain one full path to a leaf value. For example, the query
`col @> '{"animal": "dog", "color": "green"}'` requires 2 index scans: one for
all the rows that contain `{"animal": "dog"}`, and one for all the rows that
contain `{"color": "green"}`. The results are intersected like above.

Simple array containment queries are similar as well: the query
`col @> '{"a": [1,2]}'` produces 2 index scans, one for `/a/1` and one for
`/a/2/`. The results are intersected as above.

Queries that contain multiple constraints within a single JSON object or array
past the first level of nesting require special handling: for each result in
the intersection of the index scans, an index join against the primary table
must be performed to re-check the query’s index condition. To understand why,
observe the following examples and adversarial inputs:

| query                              | adversarial input           |
| ---------------------------------- | ----------------------------|
| `col @> '[{"a": 1, "b": 2}]'`      | `[{"a": 1}, {"b": 2}]`      |
| `col @> '[[1,2]]'`                 | `[[1], [2]]`                |


In each of those cases, an intersection of index scans for each of the paths
through the query object will both include rows with the adversarial input, but
the adversarial input does not actually contain the query. To resolve this
issue, each row in the intersection must be rechecked against the index
condition.

**Match intersection algorithm**

As mentioned above, the fundamental new operation required for performing a
search on an inverted index is intersecting 1 or more sets of primary keys and
using that as the input to an index scan. This operation will be built into a
new plan node that sits in between a scan node and an index join node to avoid
polluting the pre-existing scan node implementations.

The inverted index intersection node will handle unioning the output of `OR`
constraints, and intersecting the output of `AND` constraints as described
above. Specifically, this node will retrieve all of the matching primary keys,
deduplicate/intersect/union them as necessary, and use the output to perform an
index join against the primary index.

Since the primary keys for each inverted index prefix (everything up to the
primary key) are sorted, intersection and union can be performed with a cheap
in-memory merge operation.

The sorting of these primary key sets could be used in a future implementation
step to transform this naive index join into a merge join, but that won't be
done as part of the initial implementation.

## Drawbacks

Inverted indexes are very large, since they generate an unbounded number of
index keys for every value. Implementing ordinary secondary indexes over
computed columns (which are not yet implemented as of the time of writing)
would produce a smaller index, at the cost of requiring that the customers know
ahead of time which keys they need an index on.

## Rationale and Alternatives

**Rejected JSON Array Encoding**
For key encoding arrays we considered leaving out the array special character
prefix altogether. For the JSON object in the example above, the following keys
would be encoded:

    table/jsonCol/3/pk
    table/jsonCol/4/pk
    table/jsonCol/5/pk
    table/jsonCol/a/pk
    table/jsonCol/false/pk
    table/jsonCol/foo/bar/pk
    table/jsonCol/true/pk

The advantage of this approach is a shorter key. The disadvantage is that we
would have to do more disambiguation for a given query because the nesting
information would be lost. For example, searching for containment of `[[1]]`
in an index on a table with values `[1,2]` and `[[1,2]]` would generate a
keyspan that selected both rows, since the nesting information was thrown away
on index creation.

## Future work

This RFC leaves the door open for several exciting features:

1. Inverted indexes on other column types. The key encoding described above can
   mostly be reused for other column types, as can the index manipulation and
   result set intersection code.
2. Efficient use of JSON inverted indexes for queries that search for key
   existence below the top level. More index selection analysis would be
   required, but we can support a filter like `WHERE v->a ? b` with the proposed
   index structure.
3. Efficient use of JSON inverted indexes for range scans on leaf values. The
   `@>` operator doesn’t support this, but a filter like `WHERE v->>a > 10`
   could utilize a range scan over the index since it’s sorted.

## Unresolved questions

**Index weighting**

How exactly should we weight the selectivity of constraints on inverted indexes
versus ordinary indexes?

For example, the following query includes a partially constrained secondary
index alongside a constrained inverted index.

    CREATE TABLE a (a INT, b INT, c JSON, PRIMARY KEY (a, b));
    CREATE INVERTED INDEX ON a(c);

    SELECT * FROM a WHERE a = 3 AND c @> {"foo": "bar"};

It's not clear which filter has better selectivity, since we don't have table
statistics to inform us of the distribution of rows in `a`.

**Use cases for path unaware inverted indexes**

It's not clear to us whether there will be demand for path unaware inverted
indexes on JSON columns.
