# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.10.0] - 2022-02-18

- Updated to parquet-format 2.9.0.
- Added support for page data statistics.
- Improved test coverage.
- Fixed issue with dictionary encoding where bit width was determined incorrectly.
- Added support for writing per-page CRC32 checksums.
- Added support for optional CRC32 checksum validation on read.
- Fixed issue with float32/64 dictionary encoding where trying to encode NaN failed with an error.

## [v0.9.0] - 2022-02-10

- Implemented lazy loading of pages.
- Added support for writing multiple data pages per row group.

## [v0.8.0] - 2022-02-02
- Set correct total size and total compressed in row group data.
- Fixed delta bit-pack encoding of int32 and int64 and delta-length bit-packing encoding of byte\_arrays when only a single value is written.
- Fixed build issue in the floor package that somehow made it into the previous release.

## [v0.7.0] - 2022-02-01
- Relax schema parser requirement that field names begin with a letter or underscore.
- Remove unsigned support from all integer encodings.
- Introduced explicit Clone method for parquetschema.SchemaDefinition.
- Corrected parsing of DECIMAL as ConvertedType if scale and precision aren't available.
- Fixed nested optional field reads.

## [v0.6.1] - 2021-11-19
- perf - cache result of GetSchemaDefinition call
- updated readme, added special mentions section

## [v0.6.0] - 2021-11-3
- Added a schema generator which uses reflection to automatically generate a parquet schema from a go object.
- Upgraded CI golang to 1.17.2
- Added test to reproduce issue 41: https://github.com/fraugster/parquet-go/issues/41 

## [v0.5.0] - 2021-10-29
- Upgraded thrift to v0.15.0
- Fixed reading & writing of INT96 time

## [v0.4.0] - 2021-10-06
- Fixed issues where fields in structs that were not defined in the schema raised errors (array, slice, time, map)
- Added support for reflect encoding/decoding of additional types to/from parquet types
    - Go type <-> Parquet type
    - int     <-> int64
    - int32   <-> int64
    - int16   <-> int64
    - int8    <-> int64
    - uint    <-> int64
    - uint16  <-> int64
    - uint8   <-> int64
    - int64   <-> int32
    - uint64  <-> int32
    - uint32  <-> int32
- Removed some inconsistent api behaviors
    - reflect marshaling/unmarshalling now ignores fields not defined in the schema (this already happens when
      the marshaller is used by floor.Writer and goparquet.FileReader)
    - int32PlainDecoder and int32PlainEncoder now no longer support uint values
- Added function `IsAfterUnixEpoch` to allow test whether a timestamp can be written as Julian date.
- Allowed setting selected columns after opening a parquet file through `SetSelectedColumns` method.
- Fixed `dictStore` to correctly reset `valueSize` which should reduce size of written files.
- Fixed reflection-based marshal/unmarshaling for some basic types.
- Fixed `DECIMAL`s computation of maximum amount of digits.
- Improved parsing of legacy timestamps.
- Allowed reading of file metadata before properly opening a parquet file.
- Added method `SeekToRowGroup` to allow seeking to specific row groups.

## [v0.3.0] - 2020-12-15
- Added examples how to use the low-level and high-level APIs.
- Added backwards compability in floor reader and writer for lists as produced by Athena.
- Fixed many crash issues found during fuzzing of the parquet reader.

## [v0.2.1] - 2020-11-04
- Release to correct missing changelog.

## [v0.2.0] - 2020-11-04
- Added `csv2parquet` command to convert CSV files into parquet files
- Fixed issue in `parquet-tool cat` that wouldn't print any records if no limit was provided
- Added support for backward compatible MAPs and LISTs in schema validation
- Added ValidateStrict method to validate without regard for backwards compatibility

## [v0.1.1] - 2020-05-26
- Added high-level interface to access file and column metadata

## [v0.1.0] - 2020-04-24
- Initial release

[Unreleased]: https://github.com/fraugster/parquet-go/compare/v0.10.0...HEAD
[v0.10.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.10.0
[v0.9.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.9.0
[v0.8.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.8.0
[v0.7.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.7.0
[v0.6.1]: https://github.com/fraugster/parquet-go/releases/tag/v0.6.1
[v0.6.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.6.0
[v0.5.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.5.0
[v0.4.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.4.0
[v0.3.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.3.0
[v0.2.1]: https://github.com/fraugster/parquet-go/releases/tag/v0.2.1
[v0.2.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.2.0
[v0.1.1]: https://github.com/fraugster/parquet-go/releases/tag/v0.1.1
[v0.1.0]: https://github.com/fraugster/parquet-go/releases/tag/v0.1.0
