// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parquet

import (
	"fmt"
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

// Setting parquet.Repetitions.Optional makes a column nullable. When writing a
// datum, we will always specify a definition level to indicate if the datum is
// null or not. See comments on nonNilDefLevel or nilDefLevel for more info.
var defaultRepetitions = parquet.Repetitions.Optional

// A schema field is an internal identifier for schema nodes used by the parquet
// library. A value of -1 will let the library auto-assign values. This does not
// affect reading or writing parquet files.
const defaultSchemaFieldID = int32(-1)

// The parquet library utilizes a type length of -1 for all types
// except for the type parquet.FixedLenByteArray, in which case the type
// length is the length of the array. See comment on
// (*schema.PrimitiveNode).TypeLength()
const defaultTypeLength = -1

// A datumColumn stores metadata for a CRDB datum column.
type datumColumn struct {
	node      schema.Node
	colWriter colWriter
	typ       *types.T

	// numPhysicalCols is the number of columns which will be created in a
	// parquet file for this datumColumn. physicalColsStartIdx is the index of
	// the first of these columns.
	//
	// Typically, there is a 1:1 mapping between columns and physical columns in
	// a file. However, if there is at least one tuple in the schema, then the
	// mapping is not 1:1 since each field in a tuple gets its own physical
	// column.
	//
	// Ex. For the datumColumns [i int, j (int, int), k (int, int)] where j and
	// k are tuples, the physical columns generated will be [i int, j.0 int,
	// j.1, int, k.0 int, k.1 int]. The start index of datumColumn j is 1. The
	// start index of datumColumn k is 3.
	numPhysicalCols      int
	physicalColsStartIdx int
}

// A SchemaDefinition stores a parquet schema.
type SchemaDefinition struct {
	cols []datumColumn

	// The schema is a root node with terminal children nodes which represent
	// primitive types such as int or bool.
	schema *schema.Schema
}

func (sd *SchemaDefinition) NumColumns() int {
	return len(sd.cols)
}

// NewSchema generates a SchemaDefinition.
//
// Columns in the returned SchemaDefinition will match the order they appear in
// the supplied parameters.
func NewSchema(columnNames []string, columnTypes []*types.T) (*SchemaDefinition, error) {
	if len(columnTypes) != len(columnNames) {
		return nil, errors.AssertionFailedf("the number of column names must match the number of column types")
	}

	cols := make([]datumColumn, 0)
	fields := make([]schema.Node, 0)

	physicalColStartIdx := 0
	for i := 0; i < len(columnNames); i++ {
		if columnTypes[i] == nil {
			return nil, errors.AssertionFailedf("column %s missing type information", columnNames[i])
		}
		column, err := makeColumn(columnNames[i], columnTypes[i], defaultRepetitions)
		if err != nil {
			return nil, err
		}

		column.physicalColsStartIdx = physicalColStartIdx
		physicalColStartIdx += column.numPhysicalCols

		cols = append(cols, column)
		fields = append(fields, column.node)
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

// makeColumn constructs a datumColumn. It does not populate
// datumColumn.physicalColsStartIdx.
func makeColumn(colName string, typ *types.T, repetitions parquet.Repetition) (datumColumn, error) {
	result := datumColumn{typ: typ, numPhysicalCols: 1}
	var err error
	switch typ.Family() {
	case types.BoolFamily:
		result.node = schema.NewBooleanNode(colName, repetitions, defaultSchemaFieldID)
		result.colWriter = scalarWriter(writeBool)
		return result, nil
	case types.StringFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeString)
		return result, nil
	case types.IntFamily:
		// Note: integer datums are always signed: https://www.cockroachlabs.com/docs/stable/int.html
		if typ.Oid() == oid.T_int8 {
			result.node, err = schema.NewPrimitiveNodeLogical(colName,
				repetitions, schema.NewIntLogicalType(64, true),
				parquet.Types.Int64, defaultTypeLength,
				defaultSchemaFieldID)
			if err != nil {
				return datumColumn{}, err
			}
			result.colWriter = scalarWriter(writeInt64)
			return result, nil
		}

		result.node = schema.NewInt32Node(colName, repetitions, defaultSchemaFieldID)
		result.colWriter = scalarWriter(writeInt32)
		return result, nil
	case types.PGLSNFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.NewIntLogicalType(64, true),
			parquet.Types.Int64, defaultTypeLength,
			defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writePGLSN)
		return result, nil
	case types.RefCursorFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeString)
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
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeDecimal)
		return result, nil
	case types.UuidFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.UUIDLogicalType{},
			parquet.Types.FixedLenByteArray, uuid.Size, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeUUID)
		return result, nil
	case types.TimestampFamily:
		// We do not use schema.TimestampLogicalType because the library will enforce
		// a physical type of int64, which is not sufficient for CRDB timestamps.
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeTimestamp)
		return result, nil
	case types.TimestampTZFamily:
		// We do not use schema.TimestampLogicalType because the library will enforce
		// a physical type of int64, which is not sufficient for CRDB timestamps.
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeTimestampTZ)
		return result, nil
	case types.INetFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeINet)
		return result, nil
	case types.JsonFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.JSONLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeJSON)
		return result, nil
	case types.BitFamily:
		result.node, err = schema.NewPrimitiveNode(colName,
			repetitions, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeBit)
		return result, nil
	case types.BytesFamily:
		result.node, err = schema.NewPrimitiveNode(colName,
			repetitions, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeBytes)
		return result, nil
	case types.EnumFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.EnumLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeEnum)
		return result, nil
	case types.DateFamily:
		// We do not use schema.DateLogicalType because the library will enforce
		// a physical type of int32, which is not sufficient for CRDB timestamps.
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeDate)
		return result, nil
	case types.Box2DFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeBox2D)
		return result, nil
	case types.GeographyFamily:
		result.node, err = schema.NewPrimitiveNode(colName,
			repetitions, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeGeography)
		return result, nil
	case types.GeometryFamily:
		result.node, err = schema.NewPrimitiveNode(colName,
			repetitions, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeGeometry)
		return result, nil
	case types.IntervalFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeInterval)
		return result, nil
	case types.TimeFamily:
		// CRDB stores time datums in microseconds, adjusted to UTC.
		// See https://www.cockroachlabs.com/docs/stable/time.html.
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.NewTimeLogicalType(true, schema.TimeUnitMicros), parquet.Types.Int64,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeTime)
		return result, nil
	case types.TimeTZFamily:
		// We cannot use the schema.NewTimeLogicalType because it does not support
		// timezones.
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeTimeTZ)
		return result, nil
	case types.FloatFamily:
		if typ.Oid() == oid.T_float4 {
			result.node, err = schema.NewPrimitiveNode(colName,
				repetitions, parquet.Types.Float,
				defaultTypeLength, defaultSchemaFieldID)
			if err != nil {
				return datumColumn{}, err
			}
			result.colWriter = scalarWriter(writeFloat32)
			return result, nil
		}
		result.node, err = schema.NewPrimitiveNode(colName,
			repetitions, parquet.Types.Double,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeFloat64)
		return result, nil
	case types.OidFamily:
		result.node = schema.NewInt32Node(colName, repetitions, defaultSchemaFieldID)
		result.colWriter = scalarWriter(writeOid)
		return result, nil
	case types.CollatedStringFamily:
		result.node, err = schema.NewPrimitiveNodeLogical(colName,
			repetitions, schema.StringLogicalType{}, parquet.Types.ByteArray,
			defaultTypeLength, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		result.colWriter = scalarWriter(writeCollatedString)
		return result, nil
	case types.ArrayFamily:
		// Arrays for type T are represented by the following:
		// message schema {                 -- toplevel schema
		//   optional group a (LIST) {      -- column for this array
		//     repeated group list {
		//       optional T element;
		//     }
		//   }
		// }
		// Representing arrays this way makes it easier to differentiate NULL, [NULL],
		// and [] when encoding.
		// There is more info about encoding arrays here:
		// https://arrow.apache.org/blog/2022/10/08/arrow-parquet-encoding-part-2/
		if typ.ArrayContents().Family() == types.ArrayFamily {
			return result, pgerror.Newf(pgcode.FeatureNotSupported,
				"parquet writer does not support nested arrays")
		}
		if typ.ArrayContents().Family() == types.TupleFamily {
			return result, pgerror.Newf(pgcode.FeatureNotSupported,
				"parquet writer does not support tuples in arrays")
		}

		elementCol, err := makeColumn("element", typ.ArrayContents(),
			parquet.Repetitions.Optional)
		if err != nil {
			return datumColumn{}, err
		}

		innerListFields := []schema.Node{elementCol.node}
		innerListNode, err := schema.NewGroupNode("list", parquet.Repetitions.Repeated,
			innerListFields, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}

		outerListFields := []schema.Node{innerListNode}

		result.node, err = schema.NewGroupNodeLogical(colName, parquet.Repetitions.Optional,
			outerListFields, schema.ListLogicalType{}, defaultSchemaFieldID)
		if err != nil {
			return datumColumn{}, err
		}
		// NB: Because we assert that the inner elements are not arrays or tuples,
		// it is guaranteed that they are scalar.
		scalarColWriter, ok := elementCol.colWriter.(scalarWriter)
		if !ok {
			return datumColumn{}, errors.AssertionFailedf("expected scalar column writer")
		}
		result.colWriter = arrayWriter(scalarColWriter)
		result.typ = elementCol.typ
		return result, nil
	case types.TupleFamily:
		// Tuples for types T1...Tn are represented by the following:
		// message schema {                 -- toplevel schema
		//   optional group a (LIST) {
		//       optional T1 element;       -- physical column for the first field
		//       ...
		//       optional Tn element;       -- physical column for the nth field
		//   }
		// }
		// There is more info about encoding groups here:
		// https://arrow.apache.org/blog/2022/10/08/arrow-parquet-encoding-part-2/
		contents := typ.TupleContents()
		if len(contents) == 0 {
			return result, pgerror.Newf(pgcode.FeatureNotSupported,
				"parquet writer does not support empty tuples")
		}
		labels := typ.TupleLabels() // may be nil.
		nodes := make([]schema.Node, 0, len(contents))
		colWriters := make([]writeFn, 0, len(contents))
		for i, innerTyp := range contents {
			if innerTyp.Family() == types.ArrayFamily {
				return result, pgerror.Newf(pgcode.FeatureNotSupported,
					"parquet writer does not support arrays in tuples")
			}
			if innerTyp.Family() == types.TupleFamily {
				return result, pgerror.Newf(pgcode.FeatureNotSupported,
					"parquet writer does not support nested tuples")
			}
			var label string
			if labels == nil {
				label = fmt.Sprintf("%s_col%d", colName, i)
			} else {
				label = labels[i]
			}
			elementCol, err := makeColumn(label, innerTyp, defaultRepetitions)
			if err != nil {
				return datumColumn{}, err
			}
			nodes = append(nodes, elementCol.node)
			// NB: Because we assert that the inner elements are not arrays or tuples,
			// it is guaranteed that they are scalar.
			wFn, ok := elementCol.colWriter.(scalarWriter)
			if !ok {
				return datumColumn{}, errors.AssertionFailedf("expected scalar column writer")
			}
			colWriters = append(colWriters, writeFn(wFn))
		}

		result.colWriter = tupleWriter(colWriters)
		result.node, err = schema.NewGroupNode(colName, parquet.Repetitions.Optional,
			nodes, defaultSchemaFieldID)
		result.numPhysicalCols = len(colWriters)
		if err != nil {
			return datumColumn{}, err
		}
		return result, nil
	default:
		return result, pgerror.Newf(pgcode.FeatureNotSupported,
			"parquet writer does not support the type family %v", typ.Family())
	}
}
