- Feature Name: Array Column Type Encoding
- Status: completed
- Start Date: 2017-05-19
- Authors: Justin Jaffray, Jordan Lewis
- RFC PR: [#16172](https://github.com/cockroachdb/cockroach/pull/16172)
- Cockroach Issue: [#15818](https://github.com/cockroachdb/cockroach/issues/15818)

# Summary

Provide a kv encoding for arrays sufficient to permit array column types in
user tables in non-indexed positions.

Specifically, once this RFC is implemented, users will be able to create and
store tables like the following:

```sql
CREATE TABLE t (INT a PRIMARY KEY,
                INT[] intArr,
                STRING[] stringArr,
                DATE[] dateArr);
```

but not like the following index or table:

```sql
    CREATE INDEX idx ON t(intArr);
    CREATE TABLE u (INT[] arr PRIMARY KEY);
```

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

Some use cases users have shared include:

* Storing small sets, one example being a lightweight ACL system, where only users within an array can edit a row
* A way of storing associated data without having to perform a join at query time
* A way to ensure that child data is stored proximally to its parent (a problem we can solve today via interleaved tables)

# Detailed design

## Array Features and Limitations

Our arrays, like Postgres' arrays, are 1-indexed, homogenous, and
rectangular. Arrays can have at most 16 dimensions (this limit is
arbitrary—Postgres restricts to 6 dimensions). Unlike Postgres, we include
the dimensionality (number of dimensions) of an array in the column type, so
a given column can only contain arrays with the same number of dimensions.
We also do not support the Postgres feature of lower bounds on dimensions
other than 1.

Like Postgres, we do not support nested array types.
This is somewhat obscured by Postgres' use of syntax, which appear to
describe nested arrays, but actually describe multidimensional arrays:
```sql
SELECT ARRAY[ARRAY[1,2,3], ARRAY[4,5,6]];
```
It's of note that this is not describing an array of arrays.
Rather, it's describing a two dimensional array. This is an
important distinction because it means that Postgres can't support
subarrays of different lengths:
```sql
SELECT ARRAY[ARRAY[1,2,3], ARRAY[4,5]];
```
is invalid.
For multidimensionality, we will only support this alternate syntax, which Postgres treats
as equivalent:
```sql
SELECT ARRAY[[1,2,3],[4,5,6]];
```

Arrays are limited to at most 2^31-1 elements, although it's likely that the 64MB
row size limit will be the proximal limiting factor to large arrays.

## Schema Definition

A column can be declared to contain an array by appending `[]` to the name
of any non-array datatype. So `int[]` denotes a 1-dimensional array of ints,
`int[][]` denotes a 2-dimensional array of ints, and so on.
This is the same as Postgres, with the distinction that Postgres does not
enforce the specified dimensionality of the arrays.

### Column Type protobuf encoding

The column type protobuf will be modified to include an `ARRAY` value for
the `SemanticType`, and a nullable field indicating the element type if the type is
an array. The existing `array_dimensions` field will be replaced with an int
field denoting the number of dimensions in the array.

We also introduce a `version` field to the column type.
This will allow us to make a change to support multi-kv arrays in the future.
Existing columns will not be changed to the new version, so that during
deployment of a version supporting multi-kv arrays, existing columns will not
be rewritten in a way incompatible with old nodes (however we will have to
instruct users not to add new `ARRAY` columns during such an upgrade).
If a node sees a version number it doesn’t understand, it gives an appropriate
error message (“please finish upgrading the cluster”).

## KV Layer Representation

Arrays will be encoded into a single kv entry. This places a restriction on the
maximum size of an array, but significantly reduces the complexity of
implementation.

### Value Encoding

Array values will be encoded using a format very similar to the one used in
Postgres. The format is:

* Byte type tag
* Length in bytes
* Value type tag
* A byte, encoding:
    * The number of dimensions in the array as the low 4 bits
    * A 4-bit flag bitmap in the high 4 bits, having all but the lowest bit
      reserved, with the lowest bit representing whether we have a NULL bitmap
* Length of each dimension, as varints
* Optional NULL bitmap showing the location of NULLs in the array, laid out
in row-major order (with length padded to be a multiple of 8)
* Actual data, laid out in row-major order, omitting elements which are `NULL`.

As the data is homogenous, and the NULL bitmap tells us the location of any
`NULL`s in the array, each datum does not need to include its column value
tag. Variable-sized datums do need to be prefixed with their lengths according
to their usual encodings.

In the future, additional flags could be used to indicate things like
whether this array spans multiple kv entries (and thus whether we have an
associated number of kv entries), or be used as a version number.

### Key Encoding

Encoding arrays as KV-layer keys is out of scope of this RFC.

## Result Arrays

The existing in-memory array values will need to be augmented to support
multidimensionality and elements besides ints and strings.

A number of [Postgres array
functions](https://www.postgresql.org/docs/10/static/functions-array.html)
such as `array_append` (and its corresponding operator form `||`),
`array_cat`, and others will also need to be implemented for users to be
able to operate on their arrays.

## Comparison

Currently, all data types must have a comparison implementation in order
to support `IN` expressions. If we remove that requirement it will allow
us to avoid the complicated discussion of how to compare arrays
(which has subtle interactions with how we will have to key-encode
arrays, if we go that route eventually).

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

* Instead of storing a bitmap showing the location of every `NULL` in the
array, we could tag every value within the array.
This was deemed wasteful as arrays are restricted to 64MB to begin
with, and it is expected that the common case is that arrays do not contain
any `NULL`s (and thus the bitmap can be omitted).
* The protobuf changes could alternatively be implemented by having the
existing `SemanticType` denote the element type, with no special cases required, as
a 0-dimensional array can be interpreted as a scalar. This option is
attractive but it was rejected on the grounds that it overly-centralizes the
concept of column types around arrays and makes scalar datum types the special
case rather than the common case. That said, when it comes time to implement
it may turn out that this alternative is significantly easier.

# Unresolved questions
