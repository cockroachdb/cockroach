- Feature Name: Array Column Type Encoding
- Status: draft
- Start Date: 2017-05-19
- Authors: Justin Jaffray, Jordan Lewis
- RFC PR:
- Cockroach Issue: #15818

# Summary

Provide a kv encoding for arrays sufficient to permit array column types in
user tables in non-indexed positions.

Specifically, once this RFC is implemented, users will be able to create and
store tables like the following:

    CREATE TABLE t (INT a PRIMARY KEY,
                    INT[] intArr,
                    STRING[] stringArr,
                    DATE[] dateArr);

but not like the following index or table:

    CREATE INDEX idx ON t(intArr);
    CREATE TABLE u (INT[] arr PRIMARY KEY);

Adding ordinary or Postgres GIN-style indexing support for arrays is out of
scope for this RFC and will be addressed elsewhere.

Users will be able to perform all existing array operations on their array
columns, such as inclusion tests, concatenation and element retrieval.

# Motivation

Arrays are a popular feature of Postgres and are a frequently requested feature
for CockroachDB.

While the proper relational way to handle the problem arrays solve is to
create a separate table and do a join, in many cases it can be easier,
cleaner, and faster to simply have an array as a column type.

# Detailed design

## Array Features and Limitations

Our arrays, like Postgres' arrays, are 1-indexed, homogenous, and rectangular.
Arrays can have at most 16 dimensions (this limit is arbitrary -
Postgres restricts to 6 dimensions). Unlike Postgres, we include the
dimensionality of an array in the column type, so a given column can only
contain arrays with the same number of dimensions. We also do not support
the Postgres feature of lower bounds on dimensions other than 1.

Like Postgres, we do not support nested array types.

Arrays are limited to at most 2^32 elements, although it's likely that the 64MB
row size limit will be the proximal limiting factor to large arrays

## Schema Definition

A column can be declared to contain an array by appending `[]` to the name
of any non-array datatype. So `int[]` denotes a 1-dimensional array of ints,
`int[][]` denotes a 2-dimensional array of ints, and so on.
This is the same as Postgres, with the distinction that Postgres does not
enforce the specified dimensionality of the arrays.

### Column Type protobuf encoding

The column type protobuf will be modified to include an `ARRAY` value for
the `Kind`, and a nullable field indicating the element type if the type is
an array.

## KV Layer Representation

Arrays will be encoded into a single kv entry. This places a restriction on the
maximum size of an array, but significantly reduces the complexity of
implementation.

### Value Encoding

Array values will be encoded using a format very similar to the one used in
Postgres. The format is:

* Array type tag
* Element type tag
* The number of dimensions in the array as a uint8
* Length of each dimension, as uint32s
* An offset to the data (or 0 if there's no NULLs bitmap), as a uint32
* Optional NULL bitmap showing the location of NULLs in the array, laid out
in row-major order (with length padded to be a multiple of 8)
* Actual data, laid out in row-major order

As the data is homogenous, and the NULL bitmap tells us the location of any
`NULL`s in the array, each datum does not need to include its column value
tag. Variable-sized datums do need to be prefixed with their lengths according
to their usual encodings.

### Key Encoding

Encoding arrays as KV-layer keys is out of scope of this RFC. See the
Comparison section for some discussion of what a future key encoding could look
like.

## Result Arrays

The existing in-memory array values will need to be augmented to support
multidimensionality and elements besides ints and strings.

A number of [Postgres array
functions](https://www.postgresql.org/docs/10/static/functions-array.html)
such as `array_append` (and its corresponding operator form `||`),
`array_cat`, and others will also need to be implemented for users to be
able to operate on their arrays.

## Comparison

Comparison needs to be implemented at this stage in order to support
operations such as using arrays as an `ORDER BY` field or as part of `IN`
expressions. One important consideration is that the future key-encoding of
array values will have to agree with the ordering we use (though it's not out
of the question to change said ordering until the key-encoding is implemented).

Comparison will work as follows:

We disallow comparison of arrays with different dimensionalities. If the arrays
are not the same size, the array with the first smaller dimension sorts before
the larger array. If the arrays are the same size, comparison is lexicographic
in row-major order. If, during this operation, the comparison would compare a
`NULL`, the result of comparison is `NULL`.

Not being strictly lexicographic is a departure from how Postgres handles
comparison of arrays, which is outlined on their
[docs](https://www.postgresql.org/docs/9.5/static/functions-array.html), but
is a decision made in the interest of simplifying the key-encoding of arrays
when it comes time to allow indexing on array columns. Another departure from
Postgres is the treatment of `NULL`. In Postgres, for the purposes of
comparison a `NULL` in an array is treated as the maximum possible value.

# Drawbacks

* The described format does not allow fast random access to a given
index in the array, requiring a scan of the entire array to access any given
element. This is in line with how Postgres handles arrays, and applications
repeatedly requiring this kind of access would be better served by using
`unnest` on the array and treating it as a table.
* Restricting storage to a single kv entry per array places a hard limit on
the maximum size of a single array (64MB). This may be surprising to users who
expect arrays to allow storing a lot of data, or who are used to Postgres's
1GB array size limit using
[TOAST columns](https://www.postgresql.org/docs/9.5/static/storage-toast.html).

# Alternatives

* We could be more Postgres-compatible in terms of our choice of ordering
(and being strictly lexicographic is arguably more intuitive than going by
length first). This will make creating the key-encoding later on more
difficult, however.
* Instead of storing a bitmap showing the location of every `NULL` in the
array, we could tag every value within the array.
This was deemed wasteful as arrays are restricted to 64MB to begin
with, and it is expected that the common case is that arrays do not contain
any `NULL`s (and thus the bitmap can be omitted).
* The protobuf changes could alternatively be implemented by having the
existing `Kind` denote the element type, with no special cases required, as
a 0-dimensional array can be interpreted as a scalar. This option is
attractive but it was rejected on the grounds that it overly-centralizes the
concept of column types around arrays and makes scalar datum types the special
case rather than the common case. That said, when it comes time to implement
it may turn out that this alternative is significantly easier.

# Unresolved questions

* Are there any considerations for the encoding of arrays that would make it
easier for us to migrate to a multiple-kv-entry-per-array setup in the
future?
* How/where do we restrict the maximum size of arrays? Do we just allow them
to grow bigger than 64mb and then suffer the consequences?
