// Package goparquet is an implementation of the parquet file format in Go. It provides
// functionality to both read and write parquet files, as well as high-level functionality
// to manage the data schema of parquet files, to directly write Go objects to parquet files
// using automatic or custom marshalling and to read records from parquet files into
// Go objects using automatic or custom marshalling.
//
// parquet is a file format to store nested data structures in a flat columnar format. By
// storing in a column-oriented way, it allows for efficient reading of individual columns
// without having to read and decode complete rows. This allows for efficient reading and
// faster processing when using the file format in conjunction with distributed data processing
// frameworks like Apache Hadoop or distributed SQL query engines like Presto and AWS Athena.
//
// This particular implementation is divided into several packages. The top-level package
// that you're currently viewing is the low-level implementation of the file format. It is
// accompanied by the sub-packages parquetschema and floor.
//
// parquetschema provides functionality to parse textual schema definitions as well as the
// data types to manually or programmatically construct schema definitions by other means
// that are open to the user. The textual schema definition format is based on the barely
// documented schema definition format that is implemented in the parquet Java implementation.
// See the parquetschema sub-package for further documentation on how to use this package
// and the grammar of the schema definition format as well as examples.
//
// floor is a high-level wrapper around the low-level package. It provides functionality
// to open parquet files to read from them or to write to them. When reading from parquet files,
// floor takes care of automatically unmarshal the low-level data into the user-provided
// Go object. When writing to parquet files, user-provided Go objects are first marshalled
// to a low-level data structure that is then written to the parquet file. These mechanisms
// allow to directly read and write Go objects without having to deal with the details of the
// low-level parquet format. Alternatively, marshalling and unmarshalling can be implemented
// in a custom manner, giving the user maximum flexibility in case of disparities between
// the parquet schema definition and the actual Go data structure. For more information, please
// refer to the floor sub-package's documentation.
//
// To aid in working with parquet files, this package also provides a commandline tool named
// "parquet-tool" that allows you to inspect a parquet file's schema, meta data, row count and
// content as well as to merge and split parquet files.
//
// When operating with parquet files, most users should be able to cover their regular use cases
// of reading and writing files using just the high-level floor package as well as the
// parquetschema package. Only if a user has more special requirements in how to work with
// the parquet files, it is advisable to use this low-level package.
//
// To write to a parquet file, the type provided by this package is the FileWriter. Create a
// new *FileWriter object using the NewFileWriter function. You have a number of options available
// with which you can influence the FileWriter's behaviour. You can use these options to e.g. set
// meta data, the compression algorithm to use, the schema definition to use, or whether the
// data should be written in the V2 format. If you didn't set a schema definition, you then need
// to manually create columns using the functions NewDataColumn, NewListColumn and NewMapColumn,
// and then add them to the FileWriter by using the AddColumn method. To further structure
// your data into groups, use AddGroup to create groups. When you add columns to groups, you need
// to provide the full column name using dotted notation (e.g. "groupname.fieldname") to AddColumn.
// Using the AddData method, you can then add records. The provided data is of type map[string]interface{}.
// This data can be nested: to provide data for a repeated field, the data type to use for the
// map value is []interface{}. When the provided data is a group, the data type for the group itself
// again needs to be map[string]interface{}.
//
// The data within a parquet file is divided into row groups of a certain size. You can either set
// the desired row group size as a FileWriterOption, or you can manually check the estimated data
// size of the current row group using the CurrentRowGroupSize method, and use FlushRowGroup
// to write the data to disk and start a new row group. Please note that CurrentRowGroupSize
// only estimates the _uncompressed_ data size. If you've enabled compression, it is impossible
// to predict the compressed data size, so the actual row groups written to disk may be a lot
// smaller than uncompressed, depending on how efficiently your data can be compressed.
//
// When you're done writing, always use the Close method to flush any remaining data and to
// write the file's footer.
//
// To read from files, create a FileReader object using the NewFileReader function. You can
// optionally provide a list of columns to read. If these are set, only these columns are read
// from the file, while all other columns are ignored. If no columns are proided, then all
// columns are read.
//
// With the FileReader, you can then go through the row groups (using PreLoad and SkipRowGroup).
// and iterate through the row data in each row group (using NextRow). To find out how many rows
// to expect in total and per row group, use the NumRows and RowGroupNumRows methods. The number
// of row groups can be determined using the RowGroupCount method.
package goparquet

//go:generate go run bitpack_gen.go
