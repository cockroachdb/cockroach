// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachparquet

import (
	"math"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/lib/pq/oid"
)

// A schema field is an internal identifier for schema nodes used by the parquet library.
// A value of -1 will let the library auto-assign values. This does not affect reading
// or writing parquet files.
const defaultSchemaFieldID = int32(-1)

// The parquet library utilizes a type length of -1 for all types
// except for the type parquet.FixedLenByteArray, in which case the type
// length is the length of the array. See comment on (*schema.PrimitiveNode).TypeLength()
const defaultTypeLength = -1

// A column stores the parquet schema node and encoder for a
type column struct {
	node      schema.Node
	colWriter columnWriter
	decoder   decoder
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
// the order they appear in the supplied iterator.
func NewSchema(iter SchemaRowIter) (*SchemaDefinition, error) {
	cols := make([]column, 0)
	fields := make([]schema.Node, 0)

	if err := iter(func(colName string, typ *types.T) error {
		parquetCol, err := makeColumn(colName, typ)
		if err != nil {
			return err
		}
		cols = append(cols, parquetCol)
		fields = append(fields, parquetCol.node)
		return nil
	}); err != nil {
		return nil, err
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
func makeColumn(colName string, typ *types.T) (column, error) {
	// Setting parquet.Repetitions.Optional makes parquet interpret all columns as nullable.
	// When writing data, we will specify a definition level of 0 (null) or 1 (not null).
	// See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
	// for more information regarding definition levels.
	defaultRepetitions := parquet.Repetitions.Optional

	result := column{}
	var err error
	switch typ.Family() {
	case types.BoolFamily:
		result.node = schema.NewBooleanNode(colName, defaultRepetitions, defaultSchemaFieldID)
		result.colWriter = boolColumnWriter
		result.decoder = boolDecoder{}
		return result, nil
	case types.StringFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)

		if err != nil {
			return result, err
		}
		result.colWriter = stringColumnWriter
		result.decoder = stringDecoder{}
		return result, nil
	case types.IntFamily:
		// Note: integer datums are always signed: https://www.cockroachlabs.com/docs/stable/int.html
		if typ.Oid() == oid.T_int8 {
			result.node, err = schema.NewPrimitiveNodeLogical(colName,
				defaultRepetitions, schema.NewIntLogicalType(64, true),
				parquet.Types.Int64, defaultTypeLength,
				defaultSchemaFieldID)
			if err != nil {
				return result, err
			}
			result.colWriter = int64ColumnWriter
			result.decoder = int64Decoder{}
			return result, nil
		}

		result.node = schema.NewInt32Node(colName, defaultRepetitions, defaultSchemaFieldID)
		result.colWriter = int32ColumnWriter
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
			defaultRepetitions, schema.NewDecimalLogicalType(precision,
				scale), parquet.Types.ByteArray, defaultTypeLength,
			defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.colWriter = decimalColumnWriter
		result.decoder = decimalDecoder{}
		return result, nil
	case types.UuidFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.UUIDLogicalType{},
			parquet.Types.FixedLenByteArray, uuid.Size, defaultSchemaFieldID)
		if err != nil {
			return result, err
		}
		result.colWriter = uuidColumnWriter
		result.decoder = uUIDDecoder{}
		return result, nil
	case types.TimestampFamily:
		// Note that all timestamp datums are in UTC: https://www.cockroachlabs.com/docs/stable/timestamp.html
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			defaultRepetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return result, err
		}

		result.colWriter = timestampColumnWriter
		result.decoder = timestampDecoder{}
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
		//	case types.ArrayFamily:
	default:
		return result, pgerror.Newf(pgcode.FeatureNotSupported, "parquet export does not support the %v type yet", typ.Family())
	}
}
