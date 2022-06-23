<h1 align="center">parquet-go</h1>
<p align="center">
        <a href="https://github.com/fraugster/parquet-go/releases"><img src="https://img.shields.io/github/v/tag/fraugster/parquet-go.svg?color=brightgreen&label=version&sort=semver"></a>
        <a href="https://circleci.com/gh/fraugster/parquet-go/tree/master"><img src="https://circleci.com/gh/fraugster/parquet-go/tree/master.svg?style=shield"></a>
        <a href="https://goreportcard.com/report/github.com/fraugster/parquet-go"><img src="https://goreportcard.com/badge/github.com/fraugster/parquet-go"></a>
        <a href="https://codecov.io/gh/fraugster/parquet-go"><img src="https://codecov.io/gh/fraugster/parquet-go/branch/master/graph/badge.svg"/></a>
        <a href="https://godoc.org/github.com/fraugster/parquet-go"><img src="https://img.shields.io/badge/godoc-reference-blue.svg?color=blue"></a>
        <a href="https://github.com/fraugster/parquet-go/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue"></a>
</p>

---

parquet-go is an implementation of the [Apache Parquet file format](https://github.com/apache/parquet-format)
in Go. It provides functionality to both read and write parquet files, as well
as high-level functionality to manage the data schema of parquet files, to
directly write Go objects to parquet files using automatic or custom
marshalling and to read records from parquet files into Go objects using
automatic or custom marshalling.

parquet is a file format to store nested data structures in a flat columnar
format. By storing in a column-oriented way, it allows for efficient reading
of individual columns without having to read and decode complete rows. This
allows for efficient reading and faster processing when using the file format
in conjunction with distributed data processing frameworks like Apache Hadoop
or distributed SQL query engines like Presto and AWS Athena.

This implementation is divided into several packages. The top-level package is
the low-level implementation of the parquet file format. It is accompanied by
the sub-packages parquetschema and floor. parquetschema provides functionality
to parse textual schema definitions as well as the data types to manually or
programmatically construct schema definitions. floor is a high-level wrapper
around the low-level package. It provides functionality to open parquet files
to read from them or write to them using automated or custom marshalling and
unmarshalling.

## Supported Features

| Feature                                  | Read | Write | Note |
| ---                                      | ---- | ---- | --- |
| Compression                              | Yes  | Yes  | Only Gzip and SNAPPY are supported out of the box, but it is possible to add other compressors, see the `RegisterBlockCompressor` function |
| Dictionary Encoding                      | Yes  | Yes  |
| Run Length Encoding / Bit-Packing Hybrid | Yes  | Yes  | The reader can read RLE/Bit-pack encoding, but the writer only uses bit-packing |
| Delta Encoding                           | Yes  | Yes  |
| Byte Stream Split                        | No   | No   |
| Data page V1                             | Yes  | Yes  |
| Data page V2                             | Yes  | Yes  |
| Statistics in page meta data             | No   | Yes  | Page meta data is generally not made available to users and not used by parquet-go.
| Index Pages                              | No   | No   |
| Dictionary Pages                         | Yes  | Yes  |
| Encryption                               | No   | No   |
| Bloom Filter                             | No   | No   |
| Logical Types                            | Yes  | Yes  | Support for logical type is in the high-level package (floor) the low level parquet library only supports the basic types, see the type mapping table |

## Supported Data Types

| Type in parquet         | Type in Go      | Note |
| ----------------------- | --------------- | ---- |
| boolean                 | bool            |
| int32                   | int32           | See the note about the int type |
| int64                   | int64           | See the note about the int type |
| int96                   | [12]byte        |
| float                   | float32         |
| double                  | float64         |
| byte_array              | []byte          |
| fixed_len_byte_array(N) | [N]byte, []byte | use any positive number for `N` |

Note: the low-level implementation only supports int32 for the INT32 type and int64 for the INT64 type in Parquet.
Plain int or uint are not supported. The high-level `floor` package contains more extensive support for these
data types.

## Supported Logical Types

| Logical Type   | Mapped to Go types      | Note |
| -------------- | ----------------------- | ---- |
| STRING         | string, []byte          |
| DATE           | int32, time.Time        | int32: days since Unix epoch (Jan 01 1970 00:00:00 UTC); time.Time only in `floor` |
| TIME           | int32, int64, time.Time | int32: TIME(MILLIS, ...), int64: TIME(MICROS, ...), TIME(NANOS, ...); time.Time only in `floor` |
| TIMESTAMP      | int64, int96, time.Time | time.Time only in `floor`
| UUID           | [16]byte                |
| LIST           | []T                     | slices of any type |
| MAP            | map[T1]T2               | maps with any key and value types |
| ENUM           | string, []byte          |
| BSON           | []byte                  |
| DECIMAL        | []byte, [N]byte         |
| INT            | {,u}int{8,16,32,64}     | implementation is loose and will allow any INT logical type converted to any signed or unsigned int Go type. |

## Supported Converted Types

| Converted Type       | Mapped to Go types  | Note |
| -------------------- | ------------------- | ---- |
| UTF8                 | string, []byte      |
| TIME_MILLIS          | int32               | Number of milliseconds since the beginning of the day |
| TIME_MICROS          | int64               | Number of microseconds since the beginning of the day |
| TIMESTAMP_MILLIS     | int64               | Number of milliseconds since Unix epoch (Jan 01 1970 00:00:00 UTC) |
| TIMESTAMP_MICROS     | int64               | Number of milliseconds since Unix epoch (Jan 01 1970 00:00:00 UTC) |
| {,U}INT_{8,16,32,64} | {,u}int{8,16,32,64} | implementation is loose and will allow any converted type with any int Go type. |
| INTERVAL             | [12]byte            |

Please note that converted types are deprecated. Logical types should be used preferably.

## Schema Definition

parquet-go comes with support for textual schema definitions. The sub-package
`parquetschema` comes with a parser to turn the textual schema definition into
the right data type to use elsewhere to specify parquet schemas. The syntax
has been mostly reverse-engineered from a similar format also supported but
barely documented in [Parquet's Java implementation](https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/schema/MessageTypeParser.java).

For the full syntax, please have a look at the [parquetschema package Go documentation](http://godoc.org/github.com/fraugster/parquet-go/parquetschema).

Generally, the schema definition describes the structure of a message. Parquet
will then flatten this into a purely column-based structure when writing the
actual data to parquet files.

A message consists of a number of fields. Each field either has type or is a
group. A group itself consists of a number of fields, which in turn can have
either a type or are a group themselves. This allows for theoretically
unlimited levels of hierarchy.

Each field has a repetition type, describing whether a field is required (i.e.
a value has to be present), optional (i.e. a value can be present but doesn't
have to be) or repeated (i.e. zero or more values can be present). Optionally,
each field (including groups) have an annotation, which contains a logical type
or converted type that annotates something about the general structure at this
point, e.g. `LIST` indicates a more complex list structure, or `MAP` a key-value
map structure, both following certain conventions. Optionally, a typed field
can also have a numeric field ID. The field ID has no purpose intrinsic to the
parquet file format.

Here is a simple example of a message with a few typed fields:

```
message coordinates {
    required float64 latitude;
    required float64 longitude;
    optional int32 elevation = 1;
    optional binary comment (STRING);
}
```

In this example, we have a message with four typed fields, two of them
required, and two of them optional. `float64`, `int32` and `binary` describe
the fundamental data type of the field, while `longitude`, `latitude`,
`elevation` and `comment` are the field names. The parentheses contain
an annotation `STRING` which indicates that the field is a string, encoded
as binary data, i.e. a byte array. The field `elevation` also has a field
ID of `1`, indicated as numeric literal and separated from the field name
by the equal sign `=`.

In the following example, we will introduce a plain group as well as two
nested groups annotated with logical types to indicate certain data structures:

```
message transaction {
    required fixed_len_byte_array(16) txn_id (UUID);
    required int32 amount;
    required int96 txn_ts;
    optional group attributes {
        optional int64 shop_id;
        optional binary country_code (STRING);
        optional binary postcode (STRING);
    }
    required group items (LIST) {
        repeated group list {
            required int64 item_id;
            optional binary name (STRING);
        }
    }
    optional group user_attributes (MAP) {
        repeated group key_value {
            required binary key (STRING);
            required binary value (STRING);
        }
    }
}
```

In this example, we see a number of top-level fields, some of which are
groups. The first group is simply a group of typed fields, named `attributes`.

The second group, `items` is annotated to be a `LIST` and in turn contains a
`repeated group list`, which in turn contains a number of typed fields. When
a group is annotated as `LIST`, it needs to follow a particular convention:
it has to contain a `repeated group` named `list`. Inside this group, any
fields can be present.

The third group, `user_attributes` is annotated as `MAP`. Similar to `LIST`,
it follows some conventions. In particular, it has to contain only a single
`required group` with the name `key_value`, which in turn contains exactly two
fields, one named `key`, the other named `value`. This represents a map
structure in which each key is associated with one value.

## Examples

For examples how to use both the low-level and high-level APIs of this library, please
see the directory `examples`. You can also check out the accompanying tools (see below)
for more advanced examples. The tools are located in the `cmd` directory.

## Tools

`parquet-go` comes with tooling to inspect and generate parquet tools.

### parquet-tool

`parquet-tool` allows you to inspect the meta data, the schema and the number of rows
as well as print the content of a parquet file. You can also use it to split an existing
parquet file into multiple smaller files.

Install it by running `go get github.com/fraugster/parquet-go/cmd/parquet-tool` on your command line.
For more detailed help on how to use the tool, consult `parquet-tool --help`.

### csv2parquet

`csv2parquet` makes it possible to convert an existing CSV file into a parquet file. By default,
all columns are simply turned into strings, but you provide it with type hints to influence
the generated parquet schema.

You can install this tool by running `go get github.com/fraugster/parquet-go/cmd/csv2parquet` on your command line.
For more help, consult `csv2parquet --help`.

## Contributing

If you want to hack on this repository, please read the short [CONTRIBUTING.md](CONTRIBUTING.md)
guide first.

# Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available,
see the [tags on this repository][tags].

## Authors

- **Forud Ghafouri** - *Initial work* [fzerorubigd](https://github.com/fzerorubigd)
- **Andreas Krennmair** - *floor package, schema parser* [akrennmair](https://github.com/akrennmair)
- **Stefan Koshiw** - *Engineering Manager for Core Team* [panamafrancis](https://github.com/panamafrancis)

See also the list of [contributors][contributors] who participated in this project.

## Special Mentions

- **Nathan Hanna** - *proposal and prototyping of automatic schema generator* [jnathanh](https://github.com/jnathanh)

## License

Copyright 2021 Fraugster GmbH

This project is licensed under the Apache-2 License - see the [LICENSE](LICENSE) file for details.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 

[tags]: https://github.com/fraugster/parquet-go/tags
[contributors]: https://github.com/fraugster/parquet-go/graphs/contributors

