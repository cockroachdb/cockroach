- Feature Name: JSONB Encoding
- Status: accepted
- Start Date: 2017-10-05
- Author: Justin Jaffray
- RFC PR: #19062
- Cockroach Issue: #2969

# Summary

Provide a result value and kv encoding for JSONB values as described in the
[scoping RFC](https://github.com/cockroachdb/cockroach/pull/18739). The
motivation and use-cases for including JSONB were covered in the scoping
RFC. As a result of this change, users will be able to manipulate JSONB values
both in SQL result values and in columns within tables.  We opt to disallow key
encoding of JSONB values at this time, due to complications regarding the
key encoding of numeric values.

The format will be optimized to reduce unnecessary decoding of values. This is
because as opposed to array columns, we expect the common case of this format
to be extracting a subset of the fields, rather than the entire thing at once.

# Motivation

Covered in the scoping RFC.

# Reference-level explanation

There are several axes upon which a binary encoding for JSONB in CockroachDB
should be evaluated, among these are

* speed of access/decoding,
* speed of encoding,
* size of encoding (and compressibility),
* and flexibility.

In addition, as CockroachDB already provides a way to handle structured data
with a known set of fields (the relational model!), we should have an eye
towards the opposite use-case - unstructured data with an unknown (possibly
large) set of fields.

The proposed value encoding is to use that of Postgres, modified as appropriate
to fit into CockroachDB.  This RFC will first describe the format in detail,
then go into the alternatives considered and why they were rejected.

## JSONB Encoding

A JSON value can be one of seven types:

* `true`,
* `false`,
* `null`,
* a UTF-8 `string`,
* an arbitrary-precision `number`,
* an `array` of zero or more JSON values,
* or an `object` consisting of key-value pairs, where the keys are unique
  strings and the values are JSON values.

Every JSON value save for the root has a corresponding header called a *JEntry*,
which is a 32-bit value encoding the following information:
```
0 000 0000000000000000000000000000
^ ^   ^
│ │   └─ 28 bits denoting either the length of this value, or its end+1 offset from
│ │       the beginning of the innermost container this value is within.
│ └─ 3 bits denoting the type of the value (six possible values - one of the 5
│     scalar types or a container)
└─ 1 bit denoting whether this entry stores a length or an offset.
```

The reason for the length vs. offset flag is explained in [this
comment](https://github.com/postgres/postgres/blob/90627cf98a8e7d0531789391fd798c9bfcc3bc1a/src/include/utils/jsonb.h#L113-L138).
It's not immediately clear that this compressibility trade-off is relevant for
CockroachDB, in particular given that we use different compression algorithms than
Postgres.
The [rationale section](#rationale) includes the results of some rough experiments with
Snappy (the compression algorithm used in CockroachDB), but more detailed
experiments will be needed.

A *container header* is a 32-bit value with the following format:
```
000 00000000000000000000000000000
^   ^
│   │
│   │
│   └─ 29 bits denoting the number of elements in an array, or the number of
│       key-value pairs in an object.
└─ 3 bits denoting the type of this container (`array`, `object`, or `scalar`)
```

The root JSON object is always a container, but it might be a `scalar`
container (defined below).

### `true`, `false`, `null`

The values `true`, `false`, and `null` are expressed entirely by their type.
Their encoding is the empty sequence of bytes.

### `string`

`string`s are encoded as normal UTF-8 strings.

### `number`

In Postgres, `number` maps on to the Postgres `NUMERIC` type. Thus, this
differs from JavaScript's `number` type as Postgres JSONB `number`s are
arbitrary-precision.
In CockroachDB, `number` will be encoded using CockroachDB's `DECIMAL` encoding.

We can experiment, using the remaining free type tags, with possibly encoding
numbers as plain int64s or float64s where possible, to see if this gives a
significant improvement in either encoding size or decoding speed.

### `array`

An `array` is stored as
* a container header tagged as an array, containing the number of elements in
  the array,
* the JEntry header of every element, with every `$STRIDE`th JEntry storing an
  offset, and every other one storing a length, and then finally
* the encodings of the elements themselves.

### `object`

An `object` is stored as
* a container header tagged as an object, containing the number of
  key-value pairs in the object, then
* the JEntry header of every key (which are sorted, unique strings) followed by
  the JEntry header of every value, with every `$STRIDE`th JEntry storing an
  offset, and every other one storing a length, then finally
* the encoding of every key followed by the encoding of every value (with the
  `i`th key mapping to the `i`th value).

### `scalar`

The `scalar` container type exists so that the root value has a place to put
its JEntry (which is needed if it isn't an `object` or an `array`). It only
occurs at the root and appears only if the root value is `true`, `false`,
`null`, a `string`, or a `number`.  It contains exactly one value and its
encoding consists of
* a container header tagged as a scalar,
* the JEntry for the singular value contained within,
* the encoding of the contained value.

## Examples

### encode(`null`)

```
0x10000000 <- scalar container header
0x40000000 <- JEntry header signifying null type and 0 length
(no encoding)
```

### encode(`[true, "hello", {"a": "b"}]`)

(This glosses over numerics because their particular encoding already exists
within CockroachDB and is not very enlightening for this discussion.)

```
0x40000003   <- array container header (3 elements)
0x30000000   <- JEntry header of true
0x00000005   <- JEntry header of "hello"
0x5000000e   <- JEntry header of {"a": "b"} (whose encoding is 0x0e bytes long)
(encoding of true, which is empty)
0x68656c6c6f <- encoding of "hello"
0x20000001   <- object container header (1 key-value pair)
0x30000001   <- JEntry header of "a"
0x30000001   <- JEntry header of "b"
0x61         <- encoding of "a"
0x62         <- encoding of "b"
```

## Implementation

Implementation of JSONB will follow three major steps:
* begin with only JSONB result values which are ignorant to any encoding,
  (similar to how CockroachDB supported array values before it supported array
  column types),
* implement the encoding to-and-from the existing naive JSONB result values,
* modify the JSONB result values to take advantage of the encoding.

As the implementation of the JSONB result values can easily be changed in the
future (as opposed to the encoding, which cannot), it's less important to nail
down the precise implementation of them ahead of time, and they can be tuned
and optimized as time permits.

## Rationale

### Offset-vs-length

Some cursory benchmarks with Snappy imply a modest but significant improvement
in compressibility with values encoded with lengths vs. offsets (string
encoding and BSON included for interest's sake):

#### 1000 JSON objects with 200 keys, no keys in common across objects
```
compression of jsonb-len: 6838008B -> 4959971B (72.54%)
compression of jsonb-off: 6838008B -> 5779418B (84.52%)
compression of json-text: 7025581B -> 6596903B (93.90%)
compression of bson:      7566787B -> 5651655B (74.69%)
```

#### 1000 JSON objects with 200 keys, all keys in common across objects
```
compression of jsonb-len: 6944660B -> 3860875B (55.59%)
compression of jsonb-off: 6944660B -> 4911796B (70.73%)
compression of json-text: 7249122B -> 5841104B (80.58%)
compression of bson:      7726628B -> 4713029B (61.00%)
```

Comparable results were seen comparing the compression of RocksDB SSTables, so
this seems like a reasonable concern.

`$STRIDE` is a tunable value, which will have to be chosen after some
benchmarking of the compressibility vs. performance trade-off, along with
information from customers on the shape of their JSON data.

Initially we will set it to be infinite, always using lengths. If this proves
to be bad for lookup performance we can tweak it as need be.

## Alternative Encodings

### String-encoding

The simplest way we could represent JSON is simply as text.  This is the way
the SQL/JSON spec says it should be done and was how Postgres's initial
implementation worked (their `JSON` as opposed to `JSONB`), however, this has
two primary undesirable properties. First, it's quite bloated (and not very
compressible under Snappy).  More problematic however is the access speed.
Extracting a field from a JSON object stored this way requires parsing the
object, something we would like to avoid in the common case.

### BSON

BSON is the encoding used in MongoDB.
Its design is documented at [bsonspec.org](http://bsonspec.org/spec.html).
This format is proven and well-understood.
It seems optimized for documents with small sets of keys, as such, it doesn't
contain an index of entries at the beginning of a document, and it doesn't
mandate that keys be sorted. This makes sense, as Mongo doesn't support the type of
inverted indexing Postgres supports and thus expects users to have a more
structured schema with a small set of keys.
As we are designing for use-cases more similar to Postgres, these
considerations don't seem as valuable for us.

# Unresolved questions
