// Package parquetschema contains functions and data types to manage
// schema definitions for the parquet-go package. Most importantly,
// provides a schema definition parser to turn a textual representation
// of a parquet schema into a SchemaDefinition object.
//
// For the purpose of giving users the ability to define parquet schemas
// in other ways, this package also exposes the data types necessary for it.
// Users have the possibility to manually assemble their own SchemaDefinition
// object manually and programmatically.
//
// To construct a schema definition, start with a SchemaDefinition object and
// set its RootDocument field to a ColumnDefinition. This "root column" describes
// the whole message. The root column doesn't have a type on its own, so the
// SchemaElement can be left unset. Inside the root column definition, you then
// need to populate children. For each of the children, you need to set the SchemaElement,
// and either SchemaElement.Type or the children. This is for the following reason:
// if no type is set, it indicates that this column is a group, consisting of its children.
// A group without children is nonsensical. If a type is set, it indicates that the field
// is of a particular type, and therefore can't have any children.
//
// For the purpose of ensuring that schema definitions that were constructed not
// by the schema parser are sound and don't miss any information, you can use the Validate()
// function on the SchemaDefinition. It validates the schema definition for general soundness
// of the set data types, the overall structure (types vs groups), as well as whether
// logical types or converted types were used and whether the elements using these logical
// or converted types adhere to the conventions as laid out by the parquet documentation. You can
// find this documentation here: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
package parquetschema
