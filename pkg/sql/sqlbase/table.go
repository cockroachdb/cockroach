// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var nameToVisibleTypeMap = map[string]ColumnType_VisibleType{
	"INTEGER":         ColumnType_INTEGER,
	"INT4":            ColumnType_INTEGER,
	"INT8":            ColumnType_BIGINT,
	"INT64":           ColumnType_BIGINT,
	"BIT":             ColumnType_BIT,
	"INT2":            ColumnType_SMALLINT,
	"SMALLINT":        ColumnType_SMALLINT,
	"FLOAT4":          ColumnType_REAL,
	"REAL":            ColumnType_REAL,
	"FLOAT8":          ColumnType_DOUBLE_PRECISON,
	"DOUBLE PRECISON": ColumnType_DOUBLE_PRECISON,
}

func exprContainsVarsError(context string, Expr parser.Expr) error {
	return fmt.Errorf("%s expression '%s' may not contain variable sub-expressions", context, Expr)
}

func incompatibleExprTypeError(
	context string, expectedType parser.Type, actualType parser.Type,
) error {
	return fmt.Errorf("incompatible type for %s expression: %s vs %s",
		context, expectedType, actualType)
}

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	expr parser.Expr, expectedType parser.Type, context string, searchPath parser.SearchPath,
) (parser.TypedExpr, error) {
	if parser.ContainsVars(expr) {
		return nil, exprContainsVarsError(context, expr)
	}
	ctx := parser.SemaContext{SearchPath: searchPath}
	typedExpr, err := parser.TypeCheck(expr, &ctx, expectedType)
	if err != nil {
		return nil, err
	}
	defaultType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(defaultType) && typedExpr != parser.DNull {
		// The DEFAULT expression must match the column type exactly unless it is a
		// constant NULL value.
		return nil, incompatibleExprTypeError(context, expectedType, defaultType)
	}
	return typedExpr, nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
// The search path is used for name resolution for DEFAULT expressions.
func MakeColumnDefDescs(
	d *parser.ColumnTableDef, searchPath parser.SearchPath, evalCtx *parser.EvalContext,
) (*ColumnDescriptor, *IndexDescriptor, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != parser.NotNull && !d.PrimaryKey,
	}

	// Set Type.SemanticType and Type.Locale.
	colDatumType := parser.CastTargetToDatumType(d.Type)
	colTyp, err := DatumTypeToColumnType(colDatumType)
	if err != nil {
		return nil, nil, err
	}
	col.Type = colTyp

	// Set other attributes of col.Type and perform type-specific verification.
	switch t := d.Type.(type) {
	case *parser.BoolColType:
	case *parser.IntColType:
		col.Type.Width = int32(t.Width)
		if t.IsSerial() {
			if d.HasDefaultExpr() {
				return nil, nil, fmt.Errorf("SERIAL column %q cannot have a default value", col.Name)
			}
			s := "unique_rowid()"
			col.DefaultExpr = &s
		}
		if val, present := nameToVisibleTypeMap[t.Name]; present {
			col.Type.VisibleType = val
		}
	case *parser.FloatColType:
		// If the precision for this float col was intentionally specified as 0, return an error.
		if t.Prec == 0 && t.PrecSpecified {
			return nil, nil, errors.New("precision for type float must be at least 1 bit")
		}
		col.Type.Precision = int32(t.Prec)
		if val, present := nameToVisibleTypeMap[t.Name]; present {
			col.Type.VisibleType = val
		}
	case *parser.DecimalColType:
		col.Type.Width = int32(t.Scale)
		col.Type.Precision = int32(t.Prec)

		switch {
		case col.Type.Precision == 0 && col.Type.Width > 0:
			// TODO (seif): Find right range for error message.
			return nil, nil, errors.New("invalid NUMERIC precision 0")
		case col.Type.Precision < col.Type.Width:
			return nil, nil, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				col.Type.Width, col.Type.Precision)
		}
	case *parser.DateColType:
	case *parser.TimestampColType:
	case *parser.TimestampTZColType:
	case *parser.IntervalColType:
	case *parser.UUIDColType:
	case *parser.StringColType:
		col.Type.Width = int32(t.N)
	case *parser.NameColType:
	case *parser.BytesColType:
	case *parser.CollatedStringColType:
		col.Type.Width = int32(t.N)
	case *parser.ArrayColType:
		for i, e := range t.BoundsExprs {
			ctx := parser.SemaContext{SearchPath: searchPath}
			te, err := parser.TypeCheckAndRequire(e, &ctx, parser.TypeInt, "array bounds")
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't get bound %d", i)
			}
			d, err := te.Eval(nil)
			if err != nil {
				return nil, nil, errors.Wrapf(err, "couldn't Eval bound %d", i)
			}
			b := parser.MustBeDInt(d)
			col.Type.ArrayDimensions = append(col.Type.ArrayDimensions, int32(b))
		}
	case *parser.VectorColType:
		if _, ok := t.ParamType.(*parser.IntColType); !ok {
			return nil, nil, errors.Errorf("vectors of type %s are unsupported", t.ParamType)
		}
	case *parser.OidColType:
	default:
		return nil, nil, errors.Errorf("unexpected type %T", t)
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type.
		if _, err := SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, colDatumType, "DEFAULT", searchPath,
		); err != nil {
			return nil, nil, err
		}
		var p parser.Parser
		if err := p.AssertNoAggregationOrWindowing(
			d.DefaultExpr.Expr, "DEFAULT expressions", searchPath,
		); err != nil {
			return nil, nil, err
		}

		// Type check and simplify: this performs constant folding and reduces the expression.
		typedExpr, err := parser.TypeCheck(d.DefaultExpr.Expr, nil, col.Type.ToDatumType())
		if err != nil {
			return nil, nil, err
		}
		if typedExpr, err = p.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, nil, err
		}
		// Try to evaluate once. If it is aimed to succeed during a
		// backfill, it must succeed here too. This tries to ensure that
		// we don't end up failing the evaluation during the schema change
		// proper.
		if _, err := typedExpr.Eval(evalCtx); err != nil {
			return nil, nil, err
		}
		d.DefaultExpr.Expr = typedExpr

		s := parser.Serialize(d.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	var idx *IndexDescriptor
	if d.PrimaryKey || d.Unique {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	}

	return col, idx, nil
}

// MakeIndexKeyPrefix returns the key prefix used for the index's data. If you
// need the corresponding Span, prefer desc.IndexSpan(indexID) or
// desc.PrimaryIndexSpan().
func MakeIndexKeyPrefix(desc *TableDescriptor, indexID IndexID) []byte {
	var key []byte
	if i, err := desc.FindIndexByID(indexID); err == nil && len(i.Interleave.Ancestors) > 0 {
		key = encoding.EncodeUvarintAscending(key, uint64(i.Interleave.Ancestors[0].TableID))
		key = encoding.EncodeUvarintAscending(key, uint64(i.Interleave.Ancestors[0].IndexID))
		return key
	}
	key = encoding.EncodeUvarintAscending(key, uint64(desc.ID))
	key = encoding.EncodeUvarintAscending(key, uint64(indexID))
	return key
}

// EncodeIndexKey creates a key by concatenating keyPrefix with the encodings of
// the columns in the index.
//
// If a table or index is interleaved, `encoding.encodedNullDesc` is used in
// place of the family id (a varint) to signal the next component of the key.
// An example of one level of interleaving (a parent):
// /<parent_table_id>/<parent_index_id>/<field_1>/<field_2>/NullDesc/<table_id>/<index_id>/<field_3>/<family>
//
// Returns the key and whether any of the encoded values were NULLs.
//
// Note that ExtraColumnIDs are not encoded, so the result isn't always a
// full index key.
func EncodeIndexKey(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []parser.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	return EncodePartialIndexKey(
		tableDesc,
		index,
		len(index.ColumnIDs), /* encode all columns */
		colMap,
		values,
		keyPrefix,
	)
}

// EncodePartialIndexKey encodes a partial index key; only the first numCols of
// index.ColumnIDs are encoded.
func EncodePartialIndexKey(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	numCols int,
	colMap map[ColumnID]int,
	values []parser.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	colIDs := index.ColumnIDs[:numCols]
	// We know we will append to the key which will cause the capacity to grow so
	// make it bigger from the get-go.
	key = make([]byte, len(keyPrefix), 2*len(keyPrefix))
	copy(key, keyPrefix)
	dirs := directions(index.ColumnDirections)[:numCols]

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
			}

			partial := false
			length := int(ancestor.SharedPrefixLen)
			if length > len(colIDs) {
				length = len(colIDs)
				partial = true
			}
			var n bool
			key, n, err = EncodeColumns(colIDs[:length], dirs[:length], colMap, values, key)
			if err != nil {
				return key, containsNull, err
			}
			containsNull = containsNull || n
			if partial {
				// Early stop. Note that if we had exactly SharedPrefixLen columns
				// remaining, we want to append the next tableID/indexID pair because
				// that results in a more specific key.
				return key, containsNull, nil
			}
			colIDs, dirs = colIDs[length:], dirs[length:]
			// We reuse NotNullDescending (0xfe) as the interleave sentinel.
			key = encoding.EncodeNotNullDescending(key)
		}

		key = encoding.EncodeUvarintAscending(key, uint64(tableDesc.ID))
		key = encoding.EncodeUvarintAscending(key, uint64(index.ID))
	}

	var n bool
	key, n, err = EncodeColumns(colIDs, dirs, colMap, values, key)
	containsNull = containsNull || n
	return key, containsNull, err
}

type directions []IndexDescriptor_Direction

func (d directions) get(i int) (encoding.Direction, error) {
	if i < len(d) {
		return d[i].ToEncodingDirection()
	}
	return encoding.Ascending, nil
}

// EncodeColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []ColumnID,
	directions directions,
	colMap map[ColumnID]int,
	values []parser.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		var val parser.Datum
		if i, ok := colMap[id]; ok {
			// TODO(pmattis): Need to convert the values[i] value to the type
			// expected by the column.
			val = values[i]
		} else {
			val = parser.DNull
		}

		if val == parser.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, containsNull, err
		}

		if key, err = EncodeTableKey(key, val, dir); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

func appendEncDatumsToKey(
	key roachpb.Key, values EncDatumRow, dirs []IndexDescriptor_Direction, alloc *DatumAlloc,
) (roachpb.Key, error) {
	for i, val := range values {
		encoding := DatumEncoding_ASCENDING_KEY
		if dirs[i] == IndexDescriptor_DESC {
			encoding = DatumEncoding_DESCENDING_KEY
		}
		var err error
		key, err = val.Encode(alloc, encoding, key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

// MakeKeyFromEncDatums creates a key by concatenating keyPrefix with the
// encodings of the given EncDatum values. The values correspond to
// index.ColumnIDs.
//
// If a table or index is interleaved, `encoding.encodedNullDesc` is used in
// place of the family id (a varint) to signal the next component of the key.
// An example of one level of interleaving (a parent):
// /<parent_table_id>/<parent_index_id>/<field_1>/<field_2>/NullDesc/<table_id>/<index_id>/<field_3>/<family>
//
// Note that ExtraColumnIDs are not encoded, so the result isn't always a
// full index key.
func MakeKeyFromEncDatums(
	values EncDatumRow,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	keyPrefix []byte,
	alloc *DatumAlloc,
) (roachpb.Key, error) {
	dirs := index.ColumnDirections
	if len(values) != len(dirs) {
		return nil, errors.Errorf("%d values, %d directions", len(values), len(dirs))
	}
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key := make(roachpb.Key, len(keyPrefix), len(keyPrefix)*2)
	copy(key, keyPrefix)

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
			}

			length := int(ancestor.SharedPrefixLen)
			var err error
			key, err = appendEncDatumsToKey(key, values[:length], dirs[:length], alloc)
			if err != nil {
				return nil, err
			}
			values, dirs = values[length:], dirs[length:]

			// We reuse NotNullDescending (0xfe) as the interleave sentinel.
			key = encoding.EncodeNotNullDescending(key)
		}

		key = encoding.EncodeUvarintAscending(key, uint64(tableDesc.ID))
		key = encoding.EncodeUvarintAscending(key, uint64(index.ID))
	}
	return appendEncDatumsToKey(key, values, dirs, alloc)
}

// EncodeDatum encodes a datum (order-preserving encoding, suitable for keys).
func EncodeDatum(b []byte, d parser.Datum) ([]byte, error) {
	if values, ok := d.(*parser.DTuple); ok {
		return EncodeDatums(b, values.D)
	}
	return EncodeTableKey(b, d, encoding.Ascending)
}

// EncodeDatums encodes a Datums (order-preserving).
func EncodeDatums(b []byte, d parser.Datums) ([]byte, error) {
	for _, val := range d {
		var err error
		b, err = EncodeDatum(b, val)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// EncodeTableKey encodes `val` into `b` and returns the new buffer. The
// encoded value is guaranteed to be lexicographically sortable, but not
// guaranteed to be round-trippable during decoding.
func EncodeTableKey(b []byte, val parser.Datum, dir encoding.Direction) ([]byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, errors.Errorf("invalid direction: %d", dir)
	}

	if val == parser.DNull {
		if dir == encoding.Ascending {
			return encoding.EncodeNullAscending(b), nil
		}
		return encoding.EncodeNullDescending(b), nil
	}

	switch t := parser.UnwrapDatum(val).(type) {
	case *parser.DBool:
		var x int64
		if *t {
			x = 1
		} else {
			x = 0
		}
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, x), nil
		}
		return encoding.EncodeVarintDescending(b, x), nil
	case *parser.DInt:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *parser.DFloat:
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, float64(*t)), nil
		}
		return encoding.EncodeFloatDescending(b, float64(*t)), nil
	case *parser.DDecimal:
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, &t.Decimal), nil
		}
		return encoding.EncodeDecimalDescending(b, &t.Decimal), nil
	case *parser.DString:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *parser.DBytes:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *parser.DDate:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *parser.DTimestamp:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *parser.DTimestampTZ:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *parser.DInterval:
		if dir == encoding.Ascending {
			return encoding.EncodeDurationAscending(b, t.Duration)
		}
		return encoding.EncodeDurationDescending(b, t.Duration)
	case *parser.DUuid:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.GetBytes()), nil
		}
		return encoding.EncodeBytesDescending(b, t.GetBytes()), nil
	case *parser.DTuple:
		for _, datum := range t.D {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *parser.DCollatedString:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.Key), nil
		}
		return encoding.EncodeBytesDescending(b, t.Key), nil
	case *parser.DArray:
		for _, datum := range t.Array {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *parser.DOid:
		if dir == encoding.Ascending {

			return encoding.EncodeVarintAscending(b, int64(t.DInt)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(t.DInt)), nil
	}
	return nil, errors.Errorf("unable to encode table key: %T", val)
}

// EncodeTableValue encodes `val` into `appendTo` using DatumEncoding_VALUE
// and returns the new buffer. The encoded value is guaranteed to round
// trip and decode exactly to its input, but is not guaranteed to be
// lexicographically sortable.
func EncodeTableValue(
	appendTo []byte, colID ColumnID, val parser.Datum, scratch []byte,
) ([]byte, error) {
	if val == parser.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch t := parser.UnwrapDatum(val).(type) {
	case *parser.DBool:
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bool(*t)), nil
	case *parser.DInt:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *parser.DFloat:
		return encoding.EncodeFloatValue(appendTo, uint32(colID), float64(*t)), nil
	case *parser.DDecimal:
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &t.Decimal), nil
	case *parser.DString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *parser.DBytes:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *parser.DDate:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *parser.DTimestamp:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *parser.DTimestampTZ:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *parser.DInterval:
		return encoding.EncodeDurationValue(appendTo, uint32(colID), t.Duration), nil
	case *parser.DUuid:
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), t.UUID), nil
	case *parser.DArray:
		a, err := encodeArray(t, scratch)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeArrayValue(appendTo, uint32(colID), a), nil
	case *parser.DCollatedString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(t.Contents)), nil
	case *parser.DOid:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.DInt)), nil
	}
	return nil, errors.Errorf("unable to encode table value: %T", val)
}

// MakeEncodedKeyVals returns a slice of EncDatums with the correct types for
// the given columns.
func MakeEncodedKeyVals(desc *TableDescriptor, columnIDs []ColumnID) ([]EncDatum, error) {
	keyVals := make([]EncDatum, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		keyVals[i].Type = col.Type
	}
	return keyVals, nil
}

// DecodeTableIDIndexID decodes a table id followed by an index id.
func DecodeTableIDIndexID(key []byte) ([]byte, ID, IndexID, error) {
	var tableID uint64
	var indexID uint64
	var err error

	key, tableID, err = encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, 0, 0, err
	}
	key, indexID, err = encoding.DecodeUvarintAscending(key)
	if err != nil {
		return nil, 0, 0, err
	}

	return key, ID(tableID), IndexID(indexID), nil
}

// DecodeIndexKeyPrefix decodes the prefix of an index key and returns the
// index id and a slice for the rest of the key.
//
// Don't use this function in the scan "hot path".
func DecodeIndexKeyPrefix(
	a *DatumAlloc, desc *TableDescriptor, key []byte,
) (indexID IndexID, remaining []byte, err error) {
	// TODO(dan): This whole operation is n^2 because of the interleaves
	// bookkeeping. We could improve it to n with a prefix tree of components.

	interleaves := append([]IndexDescriptor{desc.PrimaryIndex}, desc.Indexes...)

	for component := 0; ; component++ {
		var tableID ID
		key, tableID, indexID, err = DecodeTableIDIndexID(key)
		if err != nil {
			return 0, nil, err
		}
		if tableID == desc.ID {
			// Once desc's table id has been decoded, there can be no more
			// interleaves.
			break
		}

		for i := len(interleaves) - 1; i >= 0; i-- {
			if len(interleaves[i].Interleave.Ancestors) <= component ||
				interleaves[i].Interleave.Ancestors[component].TableID != tableID ||
				interleaves[i].Interleave.Ancestors[component].IndexID != indexID {

				// This component, and thus this interleave, doesn't match what was
				// decoded, remove it.
				copy(interleaves[i:], interleaves[i+1:])
				interleaves = interleaves[:len(interleaves)-1]
			}
		}
		// The decoded key doesn't many any known interleaves
		if len(interleaves) == 0 {
			return 0, nil, errors.Errorf("no known interleaves for key")
		}

		// Anything left has the same SharedPrefixLen at index `component`, so just
		// use the first one.
		for i := uint32(0); i < interleaves[0].Interleave.Ancestors[component].SharedPrefixLen; i++ {
			l, err := encoding.PeekLength(key)
			if err != nil {
				return 0, nil, err
			}
			key = key[l:]
		}

		// We reuse NotNullDescending as the interleave sentinel, consume it.
		var ok bool
		key, ok = encoding.DecodeIfNotNull(key)
		if !ok {
			return 0, nil, errors.Errorf("invalid interleave key")
		}
	}

	return indexID, key, err
}

// DecodeIndexKey decodes the values that are a part of the specified index
// key. ValTypes is a slice returned from makeKeyVals. The remaining bytes in the
// index key are returned which will either be an encoded column ID for the
// primary key index, the primary key suffix for non-unique secondary indexes
// or unique secondary indexes containing NULL or empty. If the given descriptor
// does not match the key, false is returned with no error.
func DecodeIndexKey(
	a *DatumAlloc,
	desc *TableDescriptor,
	index *IndexDescriptor,
	vals []EncDatum,
	colDirs []encoding.Direction,
	key []byte,
) ([]byte, bool, error) {
	var decodedTableID ID
	var decodedIndexID IndexID
	var err error

	if len(index.Interleave.Ancestors) > 0 {
		for _, ancestor := range index.Interleave.Ancestors {
			key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
			if err != nil {
				return nil, false, err
			}
			if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
				return nil, false, nil
			}

			length := int(ancestor.SharedPrefixLen)
			key, err = DecodeKeyVals(vals[:length], colDirs[:length], key)
			if err != nil {
				return nil, false, err
			}
			vals, colDirs = vals[length:], colDirs[length:]

			// We reuse NotNullDescending as the interleave sentinel, consume it.
			var ok bool
			key, ok = encoding.DecodeIfNotNull(key)
			if !ok {
				return nil, false, nil
			}
		}
	}

	key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
	if err != nil {
		return nil, false, err
	}
	if decodedTableID != desc.ID || decodedIndexID != index.ID {
		return nil, false, nil
	}

	key, err = DecodeKeyVals(vals, colDirs, key)
	if err != nil {
		return nil, false, err
	}

	// We're expecting a column family id next (a varint). If descNotNull is
	// actually next, then this key is for a child table.
	if _, ok := encoding.DecodeIfNotNull(key); ok {
		return nil, false, nil
	}

	return key, true, nil
}

// DecodeKeyVals decodes the values that are part of the key. The decoded
// values are stored in the vals. If this slice is nil, the direction
// used will default to encoding.Ascending.
func DecodeKeyVals(vals []EncDatum, directions []encoding.Direction, key []byte) ([]byte, error) {
	if directions != nil && len(directions) != len(vals) {
		return nil, errors.Errorf("encoding directions doesn't parallel val: %d vs %d.",
			len(directions), len(vals))
	}
	for j := range vals {
		enc := DatumEncoding_ASCENDING_KEY
		if directions != nil && (directions[j] == encoding.Descending) {
			enc = DatumEncoding_DESCENDING_KEY
		}
		var err error
		vals[j], key, err = EncDatumFromBuffer(vals[j].Type, enc, key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

// ExtractIndexKey constructs the index (primary) key for a row from any index
// key/value entry, including secondary indexes.
//
// Don't use this function in the scan "hot path".
func ExtractIndexKey(
	a *DatumAlloc, tableDesc *TableDescriptor, entry client.KeyValue,
) (roachpb.Key, error) {
	indexID, key, err := DecodeIndexKeyPrefix(a, tableDesc, entry.Key)
	if err != nil {
		return nil, err
	}
	if indexID == tableDesc.PrimaryIndex.ID {
		return entry.Key, nil
	}

	index, err := tableDesc.FindIndexByID(indexID)
	if err != nil {
		return nil, err
	}

	// Extract the values for index.ColumnIDs.
	values, err := MakeEncodedKeyVals(tableDesc, index.ColumnIDs)
	if err != nil {
		return nil, err
	}
	dirs := make([]encoding.Direction, len(index.ColumnIDs))
	for i, dir := range index.ColumnDirections {
		dirs[i], err = dir.ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	if i, err := tableDesc.FindIndexByID(indexID); err == nil && len(i.Interleave.Ancestors) > 0 {
		// TODO(dan): In the interleaved index case, we parse the key twice; once to
		// find the index id so we can look up the descriptor, and once to extract
		// the values. Only parse once.
		var ok bool
		_, ok, err = DecodeIndexKey(a, tableDesc, i, values, dirs, entry.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("descriptor did not match key")
		}
	} else {
		key, err = DecodeKeyVals(values, dirs, key)
		if err != nil {
			return nil, err
		}
	}

	// Extract the values for index.ExtraColumnIDs
	extraValues, err := MakeEncodedKeyVals(tableDesc, index.ExtraColumnIDs)
	if err != nil {
		return nil, err
	}
	dirs = make([]encoding.Direction, len(index.ExtraColumnIDs))
	for i := range index.ExtraColumnIDs {
		// Implicit columns are always encoded Ascending.
		dirs[i] = encoding.Ascending
	}
	extraKey := key
	if index.Unique {
		extraKey, err = entry.Value.GetBytes()
		if err != nil {
			return nil, err
		}
	}
	_, err = DecodeKeyVals(extraValues, dirs, extraKey)
	if err != nil {
		return nil, err
	}

	// Encode the index key from its components.
	values = append(values, extraValues...)
	colMap := make(map[ColumnID]int)
	for i, columnID := range index.ColumnIDs {
		colMap[columnID] = i
	}
	for i, columnID := range index.ExtraColumnIDs {
		colMap[columnID] = i + len(index.ColumnIDs)
	}
	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	decodedValues := make([]parser.Datum, len(values))
	var da DatumAlloc
	for i, value := range values {
		err := value.EnsureDecoded(&da)
		if err != nil {
			return nil, err
		}
		decodedValues[i] = value.Datum
	}
	indexKey, _, err := EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colMap, decodedValues, indexKeyPrefix)
	return indexKey, err
}

const datumAllocSize = 16 // Arbitrary, could be tuned.

// DatumAlloc provides batch allocation of datum pointers, amortizing the cost
// of the allocations.
type DatumAlloc struct {
	dintAlloc         []parser.DInt
	dfloatAlloc       []parser.DFloat
	dstringAlloc      []parser.DString
	dbytesAlloc       []parser.DBytes
	ddecimalAlloc     []parser.DDecimal
	ddateAlloc        []parser.DDate
	dtimestampAlloc   []parser.DTimestamp
	dtimestampTzAlloc []parser.DTimestampTZ
	dintervalAlloc    []parser.DInterval
	duuidAlloc        []parser.DUuid
	doidAlloc         []parser.DOid
	scratch           []byte
	env               parser.CollationEnvironment
}

// NewDInt allocates a DInt.
func (a *DatumAlloc) NewDInt(v parser.DInt) *parser.DInt {
	buf := &a.dintAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DInt, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDFloat allocates a DFloat.
func (a *DatumAlloc) NewDFloat(v parser.DFloat) *parser.DFloat {
	buf := &a.dfloatAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DFloat, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDString allocates a DString.
func (a *DatumAlloc) NewDString(v parser.DString) *parser.DString {
	buf := &a.dstringAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DString, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDName allocates a DName.
func (a *DatumAlloc) NewDName(v parser.DString) parser.Datum {
	return parser.NewDNameFromDString(a.NewDString(v))
}

// NewDBytes allocates a DBytes.
func (a *DatumAlloc) NewDBytes(v parser.DBytes) *parser.DBytes {
	buf := &a.dbytesAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DBytes, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDecimal allocates a DDecimal.
func (a *DatumAlloc) NewDDecimal(v parser.DDecimal) *parser.DDecimal {
	buf := &a.ddecimalAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DDecimal, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDate allocates a DDate.
func (a *DatumAlloc) NewDDate(v parser.DDate) *parser.DDate {
	buf := &a.ddateAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DDate, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestamp allocates a DTimestamp.
func (a *DatumAlloc) NewDTimestamp(v parser.DTimestamp) *parser.DTimestamp {
	buf := &a.dtimestampAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DTimestamp, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestampTZ allocates a DTimestampTZ.
func (a *DatumAlloc) NewDTimestampTZ(v parser.DTimestampTZ) *parser.DTimestampTZ {
	buf := &a.dtimestampTzAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DTimestampTZ, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDInterval allocates a DInterval.
func (a *DatumAlloc) NewDInterval(v parser.DInterval) *parser.DInterval {
	buf := &a.dintervalAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DInterval, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDUuid allocates a DUuid.
func (a *DatumAlloc) NewDUuid(v parser.DUuid) *parser.DUuid {
	buf := &a.duuidAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DUuid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDOid allocates a DOid.
func (a *DatumAlloc) NewDOid(v parser.DOid) parser.Datum {
	buf := &a.doidAlloc
	if len(*buf) == 0 {
		*buf = make([]parser.DOid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// DecodeTableKey decodes a table key/value.
func DecodeTableKey(
	a *DatumAlloc, valType parser.Type, key []byte, dir encoding.Direction,
) (parser.Datum, []byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, nil, errors.Errorf("invalid direction: %d", dir)
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return parser.DNull, key, nil
	}
	var rkey []byte
	var err error
	switch valType {
	case parser.TypeBool:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		// No need to chunk allocate DBool as MakeDBool returns either
		// parser.DBoolTrue or parser.DBoolFalse.
		return parser.MakeDBool(parser.DBool(i != 0)), rkey, err
	case parser.TypeInt:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDInt(parser.DInt(i)), rkey, err
	case parser.TypeFloat:
		var f float64
		if dir == encoding.Ascending {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		return a.NewDFloat(parser.DFloat(f)), rkey, err
	case parser.TypeDecimal:
		var d apd.Decimal
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		dd := a.NewDDecimal(parser.DDecimal{Decimal: d})
		return dd, rkey, err
	case parser.TypeString:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDString(parser.DString(r)), rkey, err
	case parser.TypeName:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDName(parser.DString(r)), rkey, err
	case parser.TypeBytes:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return a.NewDBytes(parser.DBytes(r)), rkey, err
	case parser.TypeDate:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDDate(parser.DDate(t)), rkey, err
	case parser.TypeTimestamp:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestamp(parser.DTimestamp{Time: t}), rkey, err
	case parser.TypeTimestampTZ:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestampTZ(parser.DTimestampTZ{Time: t}), rkey, err
	case parser.TypeInterval:
		var d duration.Duration
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		return a.NewDInterval(parser.DInterval{Duration: d}), rkey, err
	case parser.TypeUUID:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		u, err := uuid.FromBytes(r)
		return a.NewDUuid(parser.DUuid{UUID: u}), rkey, err
	case parser.TypeOid:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDOid(parser.MakeDOid(parser.DInt(i))), rkey, err
	default:
		if _, ok := valType.(parser.TCollatedString); ok {
			var r string
			_, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
			if err != nil {
				return nil, nil, err
			}
			return nil, nil, errors.Errorf("TODO(eisen): cannot decode collation key: %q", r)
		}
		return nil, nil, errors.Errorf("TODO(pmattis): decoded index key: %s", valType)
	}
}

// DecodeTableValue decodes a value encoded by EncodeTableValue.
func DecodeTableValue(a *DatumAlloc, valType parser.Type, b []byte) (parser.Datum, []byte, error) {
	_, dataOffset, _, typ, err := encoding.DecodeValueTag(b)
	if err != nil {
		return nil, b, err
	}
	// NULL, true, and false are special, because their values are fully encoded by their value tag.
	if typ == encoding.Null {
		return parser.DNull, b[dataOffset:], nil
	} else if typ == encoding.True {
		return parser.MakeDBool(parser.DBool(true)), b[dataOffset:], nil
	} else if typ == encoding.False {
		return parser.MakeDBool(parser.DBool(false)), b[dataOffset:], nil
	}
	return decodeUntaggedDatum(a, valType, b[dataOffset:])
}

type arrayHeader struct {
	hasNulls      bool
	numDimensions int
	elementType   encoding.Type
	length        uint64
	nullBitmap    []byte
}

func (h arrayHeader) isNull(i uint64) bool {
	return h.hasNulls && ((h.nullBitmap[i/8]>>(i%8))&1) == 1
}

func numBytesInBitArray(numBits int) int {
	return (numBits + 7) / 8
}

func makeBitVec(src []byte, length int) (b, bitVec []byte) {
	nullBitmapNumBytes := numBytesInBitArray(length)
	return src[nullBitmapNumBytes:], src[:nullBitmapNumBytes]
}

func decodeArrayHeader(b []byte) (arrayHeader, []byte, error) {
	if len(b) < 2 {
		return arrayHeader{}, b, errors.Errorf("buffer too small")
	}
	hasNulls := b[0]&hasNullFlag != 0
	b = b[1:]
	_, dataOffset, _, encType, err := encoding.DecodeValueTag(b)
	if err != nil {
		return arrayHeader{}, b, err
	}
	b = b[dataOffset:]
	b, _, length, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return arrayHeader{}, b, err
	}
	nullBitmap := []byte(nil)
	if hasNulls {
		b, nullBitmap = makeBitVec(b, int(length))
	}
	return arrayHeader{
		hasNulls: hasNulls,
		// TODO(justin): support multiple dimensions.
		numDimensions: 1,
		elementType:   encType,
		length:        length,
		nullBitmap:    nullBitmap,
	}, b, nil
}

func decodeArray(a *DatumAlloc, elementType parser.Type, b []byte) (parser.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, b, err
	}
	header, b, err := decodeArrayHeader(b)
	if err != nil {
		return nil, b, err
	}
	result := parser.DArray{
		Array:    make(parser.Datums, header.length),
		ParamTyp: elementType,
	}
	var val parser.Datum
	for i := uint64(0); i < header.length; i++ {
		if header.isNull(i) {
			result.Array[i] = parser.DNull
		} else {
			val, b, err = decodeUntaggedDatum(a, elementType, b)
			if err != nil {
				return nil, b, err
			}
			result.Array[i] = val
		}
	}
	return &result, b, nil
}

// decodeUntaggedDatum is used to decode a Datum whose type is known, and which
// doesn't have a value tag (either due to it having been consumed already or
// not having one in the first place).
func decodeUntaggedDatum(a *DatumAlloc, t parser.Type, buf []byte) (parser.Datum, []byte, error) {
	switch t {
	case parser.TypeInt:
		b, i, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDInt(parser.DInt(i)), b, nil
	case parser.TypeString, parser.TypeName:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDString(parser.DString(data)), b, nil
	case parser.TypeBool:
		// The value of booleans are encoded in their tag, so we don't have an
		// "Untagged" version of this function.
		b, data, err := encoding.DecodeBoolValue(buf)
		if err != nil {
			return nil, b, err
		}
		d := parser.DBool(data)
		return &d, b, nil
	case parser.TypeFloat:
		b, data, err := encoding.DecodeUntaggedFloatValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDFloat(parser.DFloat(data)), b, nil
	case parser.TypeDecimal:
		b, data, err := encoding.DecodeUntaggedDecimalValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDecimal(parser.DDecimal{Decimal: data}), b, nil
	case parser.TypeBytes:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDBytes(parser.DBytes(data)), b, nil
	case parser.TypeDate:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDate(parser.DDate(data)), b, nil
	case parser.TypeTimestamp:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestamp(parser.DTimestamp{Time: data}), b, nil
	case parser.TypeTimestampTZ:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestampTZ(parser.DTimestampTZ{Time: data}), b, nil
	case parser.TypeInterval:
		b, data, err := encoding.DecodeUntaggedDurationValue(buf)
		return a.NewDInterval(parser.DInterval{Duration: data}), b, err
	case parser.TypeUUID:
		b, data, err := encoding.DecodeUntaggedUUIDValue(buf)
		return a.NewDUuid(parser.DUuid{UUID: data}), b, err
	case parser.TypeOid:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		return a.NewDOid(parser.MakeDOid(parser.DInt(data))), b, err
	default:
		switch typ := t.(type) {
		case parser.TCollatedString:
			b, data, err := encoding.DecodeUntaggedBytesValue(buf)
			return parser.NewDCollatedString(string(data), typ.Locale, &a.env), b, err
		case parser.TArray:
			return decodeArray(a, typ.Typ, buf)
		}
		return nil, buf, errors.Errorf("couldn't decode type %s", t)
	}
}

// IndexEntry represents an encoded key/value for an index entry.
type IndexEntry struct {
	Key   roachpb.Key
	Value roachpb.Value
}

// valueEncodedColumn represents a composite or stored column of a secondary
// index.
type valueEncodedColumn struct {
	id          ColumnID
	isComposite bool
}

// byID implements sort.Interface for []valueEncodedColumn based on the id
// field.
type byID []valueEncodedColumn

func (a byID) Len() int           { return len(a) }
func (a byID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byID) Less(i, j int) bool { return a[i].id < a[j].id }

// EncodeSecondaryIndex encodes key/values for a secondary index. colMap maps
// ColumnIDs to indices in `values`.
func EncodeSecondaryIndex(
	tableDesc *TableDescriptor,
	secondaryIndex *IndexDescriptor,
	colMap map[ColumnID]int,
	values []parser.Datum,
) (IndexEntry, error) {
	secondaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc, secondaryIndex.ID)
	secondaryIndexKey, containsNull, err := EncodeIndexKey(
		tableDesc, secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
	if err != nil {
		return IndexEntry{}, err
	}

	// Add the extra columns - they are encoded ascendingly which is done by
	// passing nil for the encoding directions.
	extraKey, _, err := EncodeColumns(secondaryIndex.ExtraColumnIDs, nil,
		colMap, values, nil)
	if err != nil {
		return IndexEntry{}, err
	}

	entry := IndexEntry{Key: secondaryIndexKey}

	if !secondaryIndex.Unique || containsNull {
		// If the index is not unique or it contains a NULL value, append
		// extraKey to the key in order to make it unique.
		entry.Key = append(entry.Key, extraKey...)
	}

	// Index keys are considered "sentinel" keys in that they do not have a
	// column ID suffix.
	entry.Key = keys.MakeFamilyKey(entry.Key, 0)

	var entryValue []byte
	if secondaryIndex.Unique {
		// Note that a unique secondary index that contains a NULL column value
		// will have extraKey appended to the key and stored in the value. We
		// require extraKey to be appended to the key in order to make the key
		// unique. We could potentially get rid of the duplication here but at
		// the expense of complicating scanNode when dealing with unique
		// secondary indexes.
		entryValue = extraKey
	} else {
		// The zero value for an index-key is a 0-length bytes value.
		entryValue = []byte{}
	}

	var cols []valueEncodedColumn
	for _, id := range secondaryIndex.StoreColumnIDs {
		cols = append(cols, valueEncodedColumn{id: id, isComposite: false})
	}
	for _, id := range secondaryIndex.CompositeColumnIDs {
		cols = append(cols, valueEncodedColumn{id: id, isComposite: true})
	}
	sort.Sort(byID(cols))

	var lastColID ColumnID
	// Composite columns have their contents at the end of the value.
	for _, col := range cols {
		val := values[colMap[col.id]]
		if val == parser.DNull || (col.isComposite && !val.(parser.CompositeDatum).IsComposite()) {
			continue
		}
		if lastColID > col.id {
			panic(fmt.Errorf("cannot write column id %d after %d", col.id, lastColID))
		}
		colIDDiff := col.id - lastColID
		lastColID = col.id
		entryValue, err = EncodeTableValue(entryValue, colIDDiff, val, nil)
		if err != nil {
			return IndexEntry{}, err
		}
	}
	entry.Value.SetBytes(entryValue)

	return entry, nil
}

// EncodeSecondaryIndexes encodes key/values for the secondary indexes. colMap
// maps ColumnIDs to indices in `values`. secondaryIndexEntries is the return
// value (passed as a parameter so the caller can reuse between rows) and is
// expected to be the same length as indexes.
func EncodeSecondaryIndexes(
	tableDesc *TableDescriptor,
	indexes []IndexDescriptor,
	colMap map[ColumnID]int,
	values []parser.Datum,
	secondaryIndexEntries []IndexEntry,
) error {
	for i := range indexes {
		var err error
		secondaryIndexEntries[i], err = EncodeSecondaryIndex(tableDesc, &indexes[i], colMap, values)
		if err != nil {
			return err
		}
	}
	return nil
}

// CheckColumnType verifies that a given value is compatible
// with the type requested by the column. If the value is a
// placeholder, the type of the placeholder gets populated.
func CheckColumnType(col ColumnDescriptor, typ parser.Type, pmap *parser.PlaceholderInfo) error {
	if typ == parser.TypeNull {
		return nil
	}

	// If the value is a placeholder, then the column check above has
	// populated 'colTyp' with a type to assign to it.
	colTyp := col.Type.ToDatumType()
	if p, pok := typ.(parser.TPlaceholder); pok {
		if err := pmap.SetType(p.Name, colTyp); err != nil {
			return fmt.Errorf("cannot infer type for placeholder %s from column %q: %s",
				p.Name, col.Name, err)
		}
	} else if !typ.Equivalent(colTyp) {
		// Not a placeholder; check that the value cast has succeeded.
		return fmt.Errorf("value type %s doesn't match type %s of column %q",
			typ, col.Type.SemanticType, col.Name)
	}
	return nil
}

func checkElementType(paramType parser.Type, columnType ColumnType) error {
	semanticType, err := DatumTypeToColumnSemanticType(paramType)
	if err != nil {
		return err
	}
	if semanticType != *columnType.ArrayContents {
		return errors.Errorf("type of array contents %s doesn't match column type %s",
			paramType, columnType.ArrayContents)
	}
	if cs, ok := paramType.(parser.TCollatedString); ok {
		if cs.Locale != *columnType.Locale {
			return errors.Errorf("locale of collated string array being inserted (%s) doesn't match locale of column type (%s)",
				cs.Locale, *columnType.Locale)
		}
	}
	return nil
}

// MarshalColumnValue returns a Go primitive value equivalent of val, of the
// type expected by col. If val's type is incompatible with col, or if
// col's type is not yet implemented, an error is returned.
func MarshalColumnValue(col ColumnDescriptor, val parser.Datum) (roachpb.Value, error) {
	var r roachpb.Value

	if val == parser.DNull {
		return r, nil
	}

	switch col.Type.SemanticType {
	case ColumnType_BOOL:
		if v, ok := val.(*parser.DBool); ok {
			r.SetBool(bool(*v))
			return r, nil
		}
	case ColumnType_INT:
		if v, ok := parser.AsDInt(val); ok {
			r.SetInt(int64(v))
			return r, nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(*parser.DFloat); ok {
			r.SetFloat(float64(*v))
			return r, nil
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*parser.DDecimal); ok {
			err := r.SetDecimal(&v.Decimal)
			return r, err
		}
	case ColumnType_STRING, ColumnType_NAME:
		if v, ok := parser.AsDString(val); ok {
			r.SetString(string(v))
			return r, nil
		}
	case ColumnType_BYTES:
		if v, ok := val.(*parser.DBytes); ok {
			r.SetString(string(*v))
			return r, nil
		}
	case ColumnType_DATE:
		if v, ok := val.(*parser.DDate); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case ColumnType_TIMESTAMP:
		if v, ok := val.(*parser.DTimestamp); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case ColumnType_TIMESTAMPTZ:
		if v, ok := val.(*parser.DTimestampTZ); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case ColumnType_INTERVAL:
		if v, ok := val.(*parser.DInterval); ok {
			err := r.SetDuration(v.Duration)
			return r, err
		}
	case ColumnType_UUID:
		if v, ok := val.(*parser.DUuid); ok {
			r.SetBytes(v.GetBytes())
			return r, nil
		}
	case ColumnType_ARRAY:
		if v, ok := val.(*parser.DArray); ok {
			if err := checkElementType(v.ParamTyp, col.Type); err != nil {
				return r, err
			}
			b, err := encodeArray(v, nil)
			if err != nil {
				return r, err
			}
			r.SetBytes(b)
			return r, nil
		}
	case ColumnType_COLLATEDSTRING:
		if col.Type.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		if v, ok := val.(*parser.DCollatedString); ok {
			if v.Locale == *col.Type.Locale {
				r.SetString(v.Contents)
				return r, nil
			}
			return r, fmt.Errorf("locale %q doesn't match locale %q of column %q",
				v.Locale, *col.Type.Locale, col.Name)
		}
	case ColumnType_OID:
		if v, ok := val.(*parser.DOid); ok {
			r.SetInt(int64(v.DInt))
			return r, nil
		}
	default:
		return r, errors.Errorf("unsupported column type: %s", col.Type.SemanticType)
	}
	return r, fmt.Errorf("value type %s doesn't match type %s of column %q",
		val.ResolvedType(), col.Type.SemanticType, col.Name)
}

const hasNullFlag = 1 << 4

func encodeArrayHeader(h arrayHeader, buf []byte) ([]byte, error) {
	// The header byte we append here is formatted as follows:
	// * The low 4 bits encode the number of dimensions in the array.
	// * The high 4 bits are flags, with the lowest representing whether the array
	//   contains NULLs, and the rest reserved.
	headerByte := h.numDimensions
	if h.hasNulls {
		headerByte = headerByte | hasNullFlag
	}
	buf = append(buf, byte(headerByte))
	buf = encoding.EncodeValueTag(buf, encoding.NoColumnID, h.elementType)
	buf = encoding.EncodeNonsortingUvarint(buf, h.length)
	return buf, nil
}

// setBit sets the bit in the given bitmap at index idx to 1. It's used to
// construct the NULL bitmap within arrays.
func setBit(bitmap []byte, idx int) {
	bitmap[idx/8] = bitmap[idx/8] | (1 << uint(idx%8))
}

func encodeArray(d *parser.DArray, scratch []byte) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return scratch, err
	}
	scratch = scratch[0:0]
	elementType, err := parserTypeToEncodingType(d.ParamTyp)
	if err != nil {
		return nil, err
	}
	header := arrayHeader{
		hasNulls: d.HasNulls,
		// TODO(justin): support multiple dimensions.
		numDimensions: 1,
		elementType:   elementType,
		length:        uint64(d.Len()),
		// We don't encode the NULL bitmap in this function because we do it in lockstep with the
		// main data.
	}
	scratch, err = encodeArrayHeader(header, scratch)
	if err != nil {
		return nil, err
	}
	nullBitmapStart := len(scratch)
	if d.HasNulls {
		for i := 0; i < numBytesInBitArray(d.Len()); i++ {
			scratch = append(scratch, 0)
		}
	}
	for i, e := range d.Array {
		var err error
		if d.HasNulls && e == parser.DNull {
			setBit(scratch[nullBitmapStart:], i)
		} else {
			scratch, err = encodeArrayElement(scratch, e)
			if err != nil {
				return nil, err
			}
		}
	}
	return scratch, nil
}

func parserTypeToEncodingType(t parser.Type) (encoding.Type, error) {
	switch t {
	case parser.TypeInt:
		return encoding.Int, nil
	case parser.TypeOid:
		return encoding.Int, nil
	case parser.TypeFloat:
		return encoding.Float, nil
	case parser.TypeDecimal:
		return encoding.Decimal, nil
	case parser.TypeBytes:
		return encoding.Bytes, nil
	case parser.TypeString:
		return encoding.Bytes, nil
	case parser.TypeBytes:
		return encoding.Bytes, nil
	case parser.TypeTimestamp, parser.TypeTimestampTZ, parser.TypeDate:
		return encoding.Time, nil
	case parser.TypeInterval:
		return encoding.Duration, nil
	case parser.TypeBool:
		return encoding.True, nil
	case parser.TypeUUID:
		return encoding.UUID, nil
	default:
		if t.FamilyEqual(parser.TypeCollatedString) {
			return encoding.Bytes, nil
		}
		return 0, errors.Errorf("Don't know encoding type for %s", t)
	}
}

func encodeArrayElement(b []byte, d parser.Datum) ([]byte, error) {
	switch t := d.(type) {
	case *parser.DInt:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *parser.DString:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *parser.DBytes:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *parser.DFloat:
		return encoding.EncodeUntaggedFloatValue(b, float64(*t)), nil
	case *parser.DBool:
		return encoding.EncodeBoolValue(b, encoding.NoColumnID, bool(*t)), nil
	case *parser.DDecimal:
		return encoding.EncodeUntaggedDecimalValue(b, &t.Decimal), nil
	case *parser.DDate:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *parser.DTimestamp:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *parser.DTimestampTZ:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *parser.DInterval:
		return encoding.EncodeUntaggedDurationValue(b, t.Duration), nil
	case *parser.DUuid:
		return encoding.EncodeUntaggedUUIDValue(b, t.UUID), nil
	case *parser.DOid:
		return encoding.EncodeUntaggedIntValue(b, int64(t.DInt)), nil
	case *parser.DCollatedString:
		return encoding.EncodeUntaggedBytesValue(b, []byte(t.Contents)), nil
	}
	return nil, errors.Errorf("don't know how to encode %s", d)
}

// UnmarshalColumnValue decodes the value from a key-value pair using the type
// expected by the column. An error is returned if the value's type does not
// match the column's type.
func UnmarshalColumnValue(
	a *DatumAlloc, typ ColumnType, value *roachpb.Value,
) (parser.Datum, error) {
	if value == nil {
		return parser.DNull, nil
	}

	switch typ.SemanticType {
	case ColumnType_BOOL:
		v, err := value.GetBool()
		if err != nil {
			return nil, err
		}
		return parser.MakeDBool(parser.DBool(v)), nil
	case ColumnType_INT:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDInt(parser.DInt(v)), nil
	case ColumnType_FLOAT:
		v, err := value.GetFloat()
		if err != nil {
			return nil, err
		}
		return a.NewDFloat(parser.DFloat(v)), nil
	case ColumnType_DECIMAL:
		v, err := value.GetDecimal()
		if err != nil {
			return nil, err
		}
		dd := a.NewDDecimal(parser.DDecimal{Decimal: v})
		return dd, nil
	case ColumnType_STRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDString(parser.DString(v)), nil
	case ColumnType_BYTES:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDBytes(parser.DBytes(v)), nil
	case ColumnType_DATE:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDDate(parser.DDate(v)), nil
	case ColumnType_TIMESTAMP:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestamp(parser.DTimestamp{Time: v}), nil
	case ColumnType_TIMESTAMPTZ:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestampTZ(parser.DTimestampTZ{Time: v}), nil
	case ColumnType_INTERVAL:
		d, err := value.GetDuration()
		if err != nil {
			return nil, err
		}
		return a.NewDInterval(parser.DInterval{Duration: d}), nil
	case ColumnType_COLLATEDSTRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return parser.NewDCollatedString(string(v), *typ.Locale, &a.env), nil
	case ColumnType_UUID:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		u, err := uuid.FromBytes(v)
		if err != nil {
			return nil, err
		}
		return a.NewDUuid(parser.DUuid{UUID: u}), nil
	case ColumnType_NAME:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDName(parser.DString(v)), nil
	case ColumnType_OID:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDOid(parser.MakeDOid(parser.DInt(v))), nil
	default:
		return nil, errors.Errorf("unsupported column type: %s", typ.SemanticType)
	}
}

// CheckValueWidth checks that the width (for strings, byte arrays, and
// bit string) and scale (for decimals) of the value fits the specified
// column type. Used by INSERT and UPDATE.
func CheckValueWidth(col ColumnDescriptor, val parser.Datum) error {
	switch col.Type.SemanticType {
	case ColumnType_STRING:
		if v, ok := parser.AsDString(val); ok {
			if col.Type.Width > 0 && utf8.RuneCountInString(string(v)) > int(col.Type.Width) {
				return fmt.Errorf("value too long for type %s (column %q)",
					col.Type.SQLString(), col.Name)
			}
		}
	case ColumnType_INT:
		if v, ok := parser.AsDInt(val); ok {
			if col.Type.Width > 0 {

				// Width is defined in bits.
				width := uint(col.Type.Width - 1)

				// https://www.postgresql.org/docs/9.5/static/datatype-bit.html
				// "bit type data must match the length n exactly; it is an error
				// to attempt to store shorter or longer bit strings. bit varying
				// data is of variable length up to the maximum length n; longer
				// strings will be rejected." Bits are unsigned, so we need to
				// increase the width for the type check below.
				// TODO(nvanbenschoten): Because we do not propagate the "varying"
				if col.Type.VisibleType == ColumnType_BIT {
					width = uint(col.Type.Width)
				}

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					return fmt.Errorf("integer out of range for type %s (column %q)", col.Type.VisibleType, col.Name)
				}
			}
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*parser.DDecimal); ok {
			if err := parser.LimitDecimalWidth(&v.Decimal, int(col.Type.Precision), int(col.Type.Width)); err != nil {
				return errors.Wrapf(err, "type %s (column %q)", col.Type.SQLString(), col.Name)
			}
		}
	}
	return nil
}

// ConstraintType is used to identify the type of a constraint.
type ConstraintType string

const (
	// ConstraintTypePK identifies a PRIMARY KEY constraint.
	ConstraintTypePK ConstraintType = "PRIMARY KEY"
	// ConstraintTypeFK identifies a FOREIGN KEY constraint.
	ConstraintTypeFK ConstraintType = "FOREIGN KEY"
	// ConstraintTypeUnique identifies a FOREIGN constraint.
	ConstraintTypeUnique ConstraintType = "UNIQUE"
	// ConstraintTypeCheck identifies a CHECK constraint.
	ConstraintTypeCheck ConstraintType = "CHECK"
)

// ConstraintDetail describes a constraint.
type ConstraintDetail struct {
	Kind        ConstraintType
	Columns     []string
	Details     string
	Unvalidated bool

	// Only populated for FK, PK, and Unique Constraints.
	Index *IndexDescriptor

	// Only populated for FK Constraints.
	FK              *ForeignKeyReference
	ReferencedTable *TableDescriptor
	ReferencedIndex *IndexDescriptor

	// Only populated for Check Constraints.
	CheckConstraint *TableDescriptor_CheckConstraint
}

type tableLookupFn func(ID) (*TableDescriptor, error)

// GetConstraintInfo returns a summary of all constraints on the table.
func (desc TableDescriptor) GetConstraintInfo(
	ctx context.Context, txn *client.Txn,
) (map[string]ConstraintDetail, error) {
	var tableLookup tableLookupFn
	if txn != nil {
		tableLookup = func(id ID) (*TableDescriptor, error) {
			return GetTableDescFromID(ctx, txn, id)
		}
	}
	return desc.collectConstraintInfo(tableLookup)
}

// GetConstraintInfoWithLookup returns a summary of all constraints on the
// table using the provided function to fetch a TableDescriptor from an ID.
func (desc TableDescriptor) GetConstraintInfoWithLookup(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc TableDescriptor) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc TableDescriptor) collectConstraintInfo(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	info := make(map[string]ConstraintDetail)

	// Indexes provide PK, Unique and FK constraints.
	indexes := desc.AllNonDropIndexes()
	for i := range indexes {
		index := &indexes[i]
		if index.ID == desc.PrimaryIndex.ID {
			if _, ok := info[index.Name]; ok {
				return nil, errors.Errorf("duplicate constraint name: %q", index.Name)
			}
			colHiddenMap := make(map[ColumnID]bool, len(desc.Columns))
			for i, column := range desc.Columns {
				colHiddenMap[column.ID] = desc.Columns[i].Hidden
			}
			// Don't include constraints against only hidden columns.
			// This prevents the auto-created rowid primary key index from showing up
			// in show constraints.
			hidden := true
			for _, id := range index.ColumnIDs {
				if !colHiddenMap[id] {
					hidden = false
					break
				}
			}
			if hidden {
				continue
			}
			detail := ConstraintDetail{Kind: ConstraintTypePK}
			if tableLookup != nil {
				detail.Columns = index.ColumnNames
				detail.Index = index
			}
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, errors.Errorf("duplicate constraint name: %q", index.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeUnique}
			if tableLookup != nil {
				detail.Columns = index.ColumnNames
				detail.Index = index
			}
			info[index.Name] = detail
		}

		if index.ForeignKey.IsSet() {
			if _, ok := info[index.ForeignKey.Name]; ok {
				return nil, errors.Errorf("duplicate constraint name: %q", index.ForeignKey.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeFK}
			detail.Unvalidated = index.ForeignKey.Validity == ConstraintValidity_Unvalidated
			if tableLookup != nil {
				numCols := len(index.ColumnIDs)
				if index.ForeignKey.SharedPrefixLen > 0 {
					numCols = int(index.ForeignKey.SharedPrefixLen)
				}
				detail.Columns = index.ColumnNames[:numCols]
				detail.Index = index
				other, err := tableLookup(index.ForeignKey.Table)
				if err != nil {
					return nil, errors.Errorf("error resolving table %d referenced in foreign key",
						index.ForeignKey.Table)
				}
				otherIdx, err := other.FindIndexByID(index.ForeignKey.Index)
				if err != nil {
					return nil, errors.Errorf("error resolving index %d in table %s referenced in foreign key",
						index.ForeignKey.Index, other.Name)
				}
				detail.Details = fmt.Sprintf("%s.%v", other.Name, otherIdx.ColumnNames)
				detail.FK = &index.ForeignKey
				detail.ReferencedTable = other
				detail.ReferencedIndex = otherIdx
			}
			info[index.ForeignKey.Name] = detail
		}
	}

	for _, c := range desc.Checks {
		if _, ok := info[c.Name]; ok {
			return nil, errors.Errorf("duplicate constraint name: %q", c.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeCheck}
		detail.Unvalidated = c.Validity == ConstraintValidity_Unvalidated
		if tableLookup != nil {
			detail.Details = c.Expr
			detail.CheckConstraint = c
		}
		info[c.Name] = detail
	}
	return info, nil
}

// MakePrimaryIndexKey creates a key prefix that corresponds to a table row
// (in the primary index); it is intended for tests.
//
// The value types must match the primary key columns (or a prefix of them);
// supported types are: - Datum
//  - bool (converts to DBool)
//  - int (converts to DInt)
//  - string (converts to DString)
func MakePrimaryIndexKey(desc *TableDescriptor, vals ...interface{}) (roachpb.Key, error) {
	index := &desc.PrimaryIndex
	if len(vals) > len(index.ColumnIDs) {
		return nil, errors.Errorf("got %d values, PK has %d columns", len(vals), len(index.ColumnIDs))
	}
	datums := make([]parser.Datum, len(vals))
	for i, v := range vals {
		switch v := v.(type) {
		case bool:
			datums[i] = parser.MakeDBool(parser.DBool(v))
		case int:
			datums[i] = parser.NewDInt(parser.DInt(v))
		case string:
			datums[i] = parser.NewDString(v)
		case parser.Datum:
			datums[i] = v
		default:
			return nil, errors.Errorf("unexpected value type %T", v)
		}
		// Check that the value type matches.
		colID := index.ColumnIDs[i]
		for _, c := range desc.Columns {
			if c.ID == colID {
				colTyp, err := DatumTypeToColumnType(datums[i].ResolvedType())
				if err != nil {
					return nil, err
				}
				if t := colTyp.SemanticType; t != c.Type.SemanticType {
					return nil, errors.Errorf("column %d of type %s, got value of type %s", i, c.Type.SemanticType, t)
				}
				break
			}
		}
	}
	// Create the ColumnID to index in datums slice map needed by
	// MakeIndexKeyPrefix.
	colIDToRowIndex := make(map[ColumnID]int)
	for i := range vals {
		colIDToRowIndex[index.ColumnIDs[i]] = i
	}

	keyPrefix := MakeIndexKeyPrefix(desc, index.ID)
	key, _, err := EncodeIndexKey(desc, index, colIDToRowIndex, datums, keyPrefix)
	if err != nil {
		return nil, err
	}
	return roachpb.Key(key), nil
}
