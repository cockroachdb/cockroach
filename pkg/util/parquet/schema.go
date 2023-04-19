// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parquet

import (
	"math"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Setting parquet.Repetitions.Optional makes parquet a column nullable. When
// writing a datum, we will always specify a definition level to indicate if the
// datum is null or not. See comments on nonNilDefLevel or nilDefLevel for more info.
var defaultRepetitions = parquet.Repetitions.Optional

// A schema field is an internal identifier for schema nodes used by the parquet library.
// A value of -1 will let the library auto-assign values. This does not affect reading
// or writing parquet files.
const defaultSchemaFieldID = int32(-1)

// The parquet library utilizes a type length of -1 for all types
// except for the type parquet.FixedLenByteArray, in which case the type
// length is the length of the array. See comment on (*schema.PrimitiveNode).TypeLength()
const defaultTypeLength = -1

// A column stores column metadata.
type column struct {
	node      schema.Node
	colWriter colWriter
	decoder   decoder
	typ       *types.T
}

// A SchemaDefinition stores a parquet schema.
type SchemaDefinition struct {
	// The index of a column when reading or writing parquet files
	// will correspond to the column's index in this array.
	cols []column

	// The schema is a root node with terminal children nodes which represent
	// primitive types such as int or bool. The individual columns can be
	// traversed using schema.Column(i). The children are indexed from [0,
	// len(cols)).
	schema *schema.Schema
}

// NewSchema generates a SchemaDefinition.
//
// Columns in the returned SchemaDefinition will match
// the order they appear in the supplied parameters.
func NewSchema(columnNames []string, columnTypes []*types.T) (*SchemaDefinition, error) {
	if len(columnTypes) != len(columnNames) {
		return nil, errors.AssertionFailedf("the number of column names must match the number of column types")
	}

	cols := make([]column, 0)
	fields := make([]schema.Node, 0)

	for i := 0; i < len(columnNames); i++ {
		parquetCol, err := makeColumn(columnNames[i], columnTypes[i], defaultRepetitions)
		if err != nil {
			return nil, err
		}
		cols = append(cols, parquetCol)
		fields = append(fields, parquetCol.node)
	}

	groupNode, err := schema.NewGroupNode("schema", parquet.Repetitions.Required,
		fields, defaultSchemaFieldID)
	if err != nil {
		return nil, err
	}
	return &SchemaDefinition{
		cols:   cols,
		schema: schema.NewSchema(groupNode),
	}, nil
}

// makeColumn constructs a column.
func makeColumn(colName string, typ *types.T, repetitions parquet.Repetition) (column, error) {
	result := column{typ: typ}
	var err error
	switch typ.Family() {
	case types.BoolFamily:
		result.node = schema.NewBooleanNode(colName, repetitions, defaultSchemaFieldID)
		result.colWriter = scalarWriter(writeBool)
		result.decoder = boolDecoder{}
		result.typ = types.Bool
		return result, nil
	case types.StringFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)

		if err != nil {
			return result, err
		}
		result.colWriter = scalarWriter(writeString)
		result.decoder = stringDecoder{}
		return result, nil
	case types.IntFamily:
		// Note: integer datums are always signed: https://www.cockroachlabs.com/docs/stable/int.html
		if typ.Oid() == oid.T_int8 {
			result.node, err = schema.NewPrimitiveNodeLogical(colName,
				repetitions, schema.NewIntLogicalType(64, true),
				parquet.Types.Int64, defaultTypeLength,
				defaultSchemaFieldID)
			if err != nil {
				return result, err
			}
			result.colWriter = scalarWriter(writeInt64)
			result.decoder = int64Decoder{}
			return result, nil
		}

		result.node = schema.NewInt32Node(colName, repetitions, defaultSchemaFieldID)
		result.colWriter = scalarWriter(writeInt32)
		result.decoder = int32Decoder{}
		return result, nil
	case types.DecimalFamily:
		// According to PostgresSQL docs, scale or precision of 0 implies max
		// precision and scale. This code assumes that CRDB matches this behavior.
		// https://www.postgresql.org/docs/10/datatype-numeric.html
		precision := typ.Precision()
		scale := typ.Scale()
		if typ.Precision() == 0 {
			precision = math.MaxInt32
		}
		if typ.Scale() == 0 {
			// Scale cannot exceed precision, so we do not set it to math.MaxInt32.
			// This is relevant for cases when the precision is nonzero, but the scale is 0.
			scale = precision
		}

		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.NewDecimalLogicalType(precision,
				scale), parquet.Types.ByteArray, defaultTypeLength,
			defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.colWriter = scalarWriter(writeDecimal)
		result.decoder = decimalDecoder{}
		return result, nil
	case types.UuidFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.UUIDLogicalType{},
			parquet.Types.FixedLenByteArray, uuid.Size, defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.colWriter = scalarWriter(writeUUID)
		result.decoder = uUIDDecoder{}
		return result, nil
	case types.TimestampFamily:
		// Note that all timestamp datums are in UTC: https://www.cockroachlabs.com/docs/stable/timestamp.html
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.colWriter = scalarWriter(writeTimestamp)
		result.decoder = timestampDecoder{}
		return result, nil
	case types.ArrayFamily:
		// Arrays for type T are represented by the following:
		// message schema {                 -- toplevel schema
		//   optional group a (LIST) {      -- list column
		//     repeated group list {
		//       optional T element;
		//     }
		//   }
		// }
		// Representing arrays this way makes it easier to differentiate NULL, [NULL],
		// and [] when encoding.
		// There is more info about encoding arrays here:
		// https://arrow.apache.org/blog/2022/10/08/arrow-parquet-encoding-part-2/
		elementCol, err := makeColumn("element", typ.ArrayContents(), parquet.Repetitions.Optional)
		if err != nil {
			return result, err
		}
		innerListFields := []schema.Node{elementCol.node}
		innerListNode, err := schema.NewGroupNode("list", parquet.Repetitions.Repeated, innerListFields, defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		outerListFields := []schema.Node{innerListNode}
		result.node, err = schema.NewGroupNodeLogical(colName, parquet.Repetitions.Optional, outerListFields, schema.ListLogicalType{}, defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.decoder = elementCol.decoder
		scalarColWriter, ok := elementCol.colWriter.(scalarWriter)
		if !ok {
			return result, errors.AssertionFailedf("expected scalar column writer")
		}
		result.colWriter = arrayWriter(scalarColWriter)
		result.typ = elementCol.typ
		return result, nil

		// TODO(#99028): implement support for the remaining types.
		//	case types.INetFamily:
		//	case types.JsonFamily:
		//	case types.FloatFamily:
		//	case types.BytesFamily:
		//	case types.BitFamily:
		//	case types.EnumFamily:
		//	case types.Box2DFamily:
		//	case types.GeographyFamily:
		//	case types.GeometryFamily:
		//	case types.DateFamily:
		//	case types.TimeFamily:
		//	case types.TimeTZFamily:
		//	case types.IntervalFamily:
		//	case types.TimestampTZFamily:
	default:
		return result, pgerror.Newf(pgcode.FeatureNotSupported, "parquet export does not support the %v type", typ.Family())
	}
}
