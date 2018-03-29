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
	"context"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/pkg/errors"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// aliasToVisibleTypeMap maps type aliases to ColumnType_VisibleType variants
// so that the alias is persisted. When adding new column type aliases or new
// VisibleType variants, consider adding to this mapping as well.
var aliasToVisibleTypeMap = map[string]ColumnType_VisibleType{
	coltypes.Bit.Name:      ColumnType_BIT,
	coltypes.Int2.Name:     ColumnType_SMALLINT,
	coltypes.Int4.Name:     ColumnType_INTEGER,
	coltypes.Int8.Name:     ColumnType_BIGINT,
	coltypes.Int64.Name:    ColumnType_BIGINT,
	coltypes.Integer.Name:  ColumnType_INTEGER,
	coltypes.SmallInt.Name: ColumnType_SMALLINT,
	coltypes.BigInt.Name:   ColumnType_BIGINT,

	coltypes.Real.Name:   ColumnType_REAL,
	coltypes.Float4.Name: ColumnType_REAL,
	coltypes.Float8.Name: ColumnType_DOUBLE_PRECISION,
	coltypes.Double.Name: ColumnType_DOUBLE_PRECISION,
}

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	expr tree.Expr,
	expectedType types.T,
	context string,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(evalCtx, expr) {
		return nil, fmt.Errorf("%s expression '%s' may not contain variable sub-expressions",
			context, expr)
	}
	typedExpr, err := tree.TypeCheck(expr, semaCtx, expectedType)
	if err != nil {
		return nil, err
	}
	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value.
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

func populateTypeAttrs(base ColumnType, typ coltypes.T) (ColumnType, error) {
	// Set other attributes of col.Type and perform type-specific verification.
	switch t := typ.(type) {
	case *coltypes.TBool:
	case *coltypes.TInt:
		base.Width = int32(t.Width)
		if val, present := aliasToVisibleTypeMap[t.Name]; present {
			base.VisibleType = val
		}
	case *coltypes.TFloat:
		// If the precision for this float col was intentionally specified as 0, return an error.
		if t.Prec == 0 && t.PrecSpecified {
			return ColumnType{}, errors.New("precision for type float must be at least 1 bit")
		}
		base.Precision = int32(t.Prec)
		if val, present := aliasToVisibleTypeMap[t.Name]; present {
			base.VisibleType = val
		}
	case *coltypes.TDecimal:
		base.Width = int32(t.Scale)
		base.Precision = int32(t.Prec)

		switch {
		case base.Precision == 0 && base.Width > 0:
			// TODO (seif): Find right range for error message.
			return ColumnType{}, errors.New("invalid NUMERIC precision 0")
		case base.Precision < base.Width:
			return ColumnType{}, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				base.Width, base.Precision)
		}
	case *coltypes.TDate:
	case *coltypes.TTime:
	case *coltypes.TTimestamp:
	case *coltypes.TTimestampTZ:
	case *coltypes.TInterval:
	case *coltypes.TUUID:
	case *coltypes.TIPAddr:
	case *coltypes.TJSON:
	case *coltypes.TString:
		base.Width = int32(t.N)
	case *coltypes.TName:
	case *coltypes.TBytes:
	case *coltypes.TCollatedString:
		base.Width = int32(t.N)
	case *coltypes.TArray:
		base.ArrayDimensions = t.Bounds
		var err error
		base, err = populateTypeAttrs(base, t.ParamType)
		if err != nil {
			return ColumnType{}, err
		}
	case *coltypes.TVector:
		switch t.ParamType.(type) {
		case *coltypes.TInt, *coltypes.TOid:
		default:
			return ColumnType{}, errors.Errorf("vectors of type %s are unsupported", t.ParamType)
		}
	case *coltypes.TOid:
	default:
		return ColumnType{}, errors.Errorf("unexpected type %T", t)
	}
	return base, nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
//
// semaCtx and evalCtx can be nil if no default expression is used for the
// column.
//
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeColumnDefDescs(
	d *tree.ColumnTableDef, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) (*ColumnDescriptor, *IndexDescriptor, tree.TypedExpr, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey,
	}

	// Set Type.SemanticType and Type.Locale.
	colDatumType := coltypes.CastTargetToDatumType(d.Type)
	colTyp, err := DatumTypeToColumnType(colDatumType)
	if err != nil {
		return nil, nil, nil, err
	}

	col.Type, err = populateTypeAttrs(colTyp, d.Type)
	if err != nil {
		return nil, nil, nil, err
	}

	if t, ok := d.Type.(*coltypes.TInt); ok {
		if t.IsSerial() {
			if d.HasDefaultExpr() {
				return nil, nil, nil, fmt.Errorf("SERIAL column %q cannot have a default value", d.Name)
			}
			s := "unique_rowid()"
			col.DefaultExpr = &s
		}
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type.
		if _, err := SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, colDatumType, "DEFAULT", semaCtx, evalCtx,
		); err != nil {
			return nil, nil, nil, err
		}
		var t transform.ExprTransformContext
		if err := t.AssertNoAggregationOrWindowing(
			d.DefaultExpr.Expr, "DEFAULT expressions", semaCtx.SearchPath,
		); err != nil {
			return nil, nil, nil, err
		}

		// Type check and simplify: this performs constant folding and reduces the expression.
		typedExpr, err = tree.TypeCheck(d.DefaultExpr.Expr, semaCtx, col.Type.ToDatumType())
		if err != nil {
			return nil, nil, nil, err
		}
		if typedExpr, err = t.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, nil, nil, err
		}
		d.DefaultExpr.Expr = typedExpr

		s := tree.Serialize(d.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	if d.IsComputed() {
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
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

	return col, idx, typedExpr, nil
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
// If a table or index is interleaved, `encoding.interleavedSentinel` is used
// in place of the family id (a varint) to signal the next component of the
// key.  An example of one level of interleaving (a parent):
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
	values []tree.Datum,
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
	values []tree.Datum,
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
			// Each ancestor is separated by an interleaved
			// sentinel (0xfe).
			key = encoding.EncodeInterleavedSentinel(key)
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

// Return the value corresponding to the column. If the column isn't
// present return a NULL value.
func findColumnValue(column ColumnID, colMap map[ColumnID]int, values []tree.Datum) tree.Datum {
	if i, ok := colMap[column]; ok {
		// TODO(pmattis): Need to convert the values[i] value to the type
		// expected by the column.
		return values[i]
	}
	return tree.DNull
}

// EncodeColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []ColumnID,
	directions directions,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findColumnValue(id, colMap, values)
		if val == tree.DNull {
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
	key roachpb.Key,
	types []ColumnType,
	values EncDatumRow,
	dirs []IndexDescriptor_Direction,
	alloc *DatumAlloc,
) (roachpb.Key, error) {
	for i, val := range values {
		encoding := DatumEncoding_ASCENDING_KEY
		if dirs[i] == IndexDescriptor_DESC {
			encoding = DatumEncoding_DESCENDING_KEY
		}
		var err error
		key, err = val.Encode(&types[i], alloc, encoding, key)
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
// If a table or index is interleaved, `encoding.interleavedSentinel` is used
// in place of the family id (a varint) to signal the next component of the
// key.  An example of one level of interleaving (a parent):
// /<parent_table_id>/<parent_index_id>/<field_1>/<field_2>/NullDesc/<table_id>/<index_id>/<field_3>/<family>
//
// Note that ExtraColumnIDs are not encoded, so the result isn't always a
// full index key.
func MakeKeyFromEncDatums(
	types []ColumnType,
	values EncDatumRow,
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	keyPrefix []byte,
	alloc *DatumAlloc,
) (roachpb.Key, error) {
	dirs := index.ColumnDirections
	// Values may be a prefix of the index columns.
	if len(values) > len(dirs) {
		return nil, errors.Errorf("%d values, %d directions", len(values), len(dirs))
	}
	if len(values) != len(types) {
		return nil, errors.Errorf("%d values, %d types", len(values), len(types))
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
			key, err = appendEncDatumsToKey(key, types[:length], values[:length], dirs[:length], alloc)
			if err != nil {
				return nil, err
			}
			types, values, dirs = types[length:], values[length:], dirs[length:]

			// Each ancestor is separated by an interleaved
			// sentinel (0xfe).
			key = encoding.EncodeInterleavedSentinel(key)
		}

		key = encoding.EncodeUvarintAscending(key, uint64(tableDesc.ID))
		key = encoding.EncodeUvarintAscending(key, uint64(index.ID))
	}
	return appendEncDatumsToKey(key, types, values, dirs, alloc)
}

// EncodeDatum encodes a datum (order-preserving encoding, suitable for keys).
func EncodeDatum(b []byte, d tree.Datum) ([]byte, error) {
	if values, ok := d.(*tree.DTuple); ok {
		return EncodeDatums(b, values.D)
	}
	return EncodeTableKey(b, d, encoding.Ascending)
}

// EncodeDatums encodes a Datums (order-preserving).
func EncodeDatums(b []byte, d tree.Datums) ([]byte, error) {
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
func EncodeTableKey(b []byte, val tree.Datum, dir encoding.Direction) ([]byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, errors.Errorf("invalid direction: %d", dir)
	}

	if val == tree.DNull {
		if dir == encoding.Ascending {
			return encoding.EncodeNullAscending(b), nil
		}
		return encoding.EncodeNullDescending(b), nil
	}

	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DBool:
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
	case *tree.DInt:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DFloat:
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, float64(*t)), nil
		}
		return encoding.EncodeFloatDescending(b, float64(*t)), nil
	case *tree.DDecimal:
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, &t.Decimal), nil
		}
		return encoding.EncodeDecimalDescending(b, &t.Decimal), nil
	case *tree.DString:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *tree.DBytes:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(*t)), nil
		}
		return encoding.EncodeStringDescending(b, string(*t)), nil
	case *tree.DDate:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DTime:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(*t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(*t)), nil
	case *tree.DTimestamp:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *tree.DTimestampTZ:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case *tree.DInterval:
		if dir == encoding.Ascending {
			return encoding.EncodeDurationAscending(b, t.Duration)
		}
		return encoding.EncodeDurationDescending(b, t.Duration)
	case *tree.DUuid:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.GetBytes()), nil
		}
		return encoding.EncodeBytesDescending(b, t.GetBytes()), nil
	case *tree.DIPAddr:
		data := t.ToBuffer(nil)
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, data), nil
		}
		return encoding.EncodeBytesDescending(b, data), nil
	case *tree.DTuple:
		for _, datum := range t.D {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *tree.DCollatedString:
		if dir == encoding.Ascending {
			return encoding.EncodeBytesAscending(b, t.Key), nil
		}
		return encoding.EncodeBytesDescending(b, t.Key), nil
	case *tree.DArray:
		for _, datum := range t.Array {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	case *tree.DOid:
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
	appendTo []byte, colID ColumnID, val tree.Datum, scratch []byte,
) ([]byte, error) {
	if val == tree.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DBool:
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bool(*t)), nil
	case *tree.DInt:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DFloat:
		return encoding.EncodeFloatValue(appendTo, uint32(colID), float64(*t)), nil
	case *tree.DDecimal:
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &t.Decimal), nil
	case *tree.DString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *tree.DBytes:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(*t)), nil
	case *tree.DDate:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DTime:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *tree.DTimestamp:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *tree.DTimestampTZ:
		return encoding.EncodeTimeValue(appendTo, uint32(colID), t.Time), nil
	case *tree.DInterval:
		return encoding.EncodeDurationValue(appendTo, uint32(colID), t.Duration), nil
	case *tree.DUuid:
		return encoding.EncodeUUIDValue(appendTo, uint32(colID), t.UUID), nil
	case *tree.DIPAddr:
		return encoding.EncodeIPAddrValue(appendTo, uint32(colID), t.IPAddr), nil
	case *tree.DJSON:
		encoded, err := json.EncodeJSON(scratch, t.JSON)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeJSONValue(appendTo, uint32(colID), encoded), nil
	case *tree.DArray:
		a, err := encodeArray(t, scratch)
		if err != nil {
			return nil, err
		}
		return encoding.EncodeArrayValue(appendTo, uint32(colID), a), nil
	case *tree.DCollatedString:
		return encoding.EncodeBytesValue(appendTo, uint32(colID), []byte(t.Contents)), nil
	case *tree.DOid:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(t.DInt)), nil
	}
	return nil, errors.Errorf("unable to encode table value: %T", val)
}

// GetColumnTypes returns the types of the columns with the given IDs.
func GetColumnTypes(desc *TableDescriptor, columnIDs []ColumnID) ([]ColumnType, error) {
	types := make([]ColumnType, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		types[i] = col.Type
	}
	return types, nil
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
	desc *TableDescriptor, key []byte,
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

		// Consume the interleaved sentinel.
		var ok bool
		key, ok = encoding.DecodeIfInterleavedSentinel(key)
		if !ok {
			return 0, nil, errors.Errorf("invalid interleave key")
		}
	}

	return indexID, key, err
}

// DecodeIndexKey decodes the values that are a part of the specified index
// key (setting vals).
//
// The remaining bytes in the index key are returned which will either be an
// encoded column ID for the primary key index, the primary key suffix for
// non-unique secondary indexes or unique secondary indexes containing NULL or
// empty. If the given descriptor does not match the key, false is returned with
// no error.
func DecodeIndexKey(
	desc *TableDescriptor,
	index *IndexDescriptor,
	types []ColumnType,
	vals []EncDatum,
	colDirs []encoding.Direction,
	key []byte,
) (remainingKey []byte, matches bool, _ error) {
	key, _, _, err := DecodeTableIDIndexID(key)
	if err != nil {
		return nil, false, err
	}
	return DecodeIndexKeyWithoutTableIDIndexIDPrefix(desc, index, types, vals, colDirs, key)
}

// DecodeIndexKeyWithoutTableIDIndexIDPrefix is the same as DecodeIndexKey,
// except it expects its index key is missing its first table id / index id
// key prefix.
func DecodeIndexKeyWithoutTableIDIndexIDPrefix(
	desc *TableDescriptor,
	index *IndexDescriptor,
	types []ColumnType,
	vals []EncDatum,
	colDirs []encoding.Direction,
	key []byte,
) (remainingKey []byte, matches bool, _ error) {
	var decodedTableID ID
	var decodedIndexID IndexID
	var err error

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// Our input key had its first table id / index id chopped off, so
			// don't try to decode those for the first ancestor.
			if i != 0 {
				key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
				if err != nil {
					return nil, false, err
				}
				if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
					return nil, false, nil
				}
			}

			length := int(ancestor.SharedPrefixLen)
			key, err = DecodeKeyVals(types[:length], vals[:length], colDirs[:length], key)
			if err != nil {
				return nil, false, err
			}
			types, vals, colDirs = types[length:], vals[length:], colDirs[length:]

			// Consume the interleaved sentinel.
			var ok bool
			key, ok = encoding.DecodeIfInterleavedSentinel(key)
			if !ok {
				return nil, false, nil
			}
		}

		key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
		if err != nil {
			return nil, false, err
		}
		if decodedTableID != desc.ID || decodedIndexID != index.ID {
			return nil, false, nil
		}
	}

	key, err = DecodeKeyVals(types, vals, colDirs, key)
	if err != nil {
		return nil, false, err
	}

	// We're expecting a column family id next (a varint). If
	// interleavedSentinel is actually next, then this key is for a child
	// table.
	if _, ok := encoding.DecodeIfInterleavedSentinel(key); ok {
		return nil, false, nil
	}

	return key, true, nil
}

// DecodeKeyVals decodes the values that are part of the key. The decoded
// values are stored in the vals. If this slice is nil, the direction
// used will default to encoding.Ascending.
func DecodeKeyVals(
	types []ColumnType, vals []EncDatum, directions []encoding.Direction, key []byte,
) ([]byte, error) {
	if directions != nil && len(directions) != len(vals) {
		return nil, errors.Errorf("encoding directions doesn't parallel vals: %d vs %d.",
			len(directions), len(vals))
	}
	for j := range vals {
		enc := DatumEncoding_ASCENDING_KEY
		if directions != nil && (directions[j] == encoding.Descending) {
			enc = DatumEncoding_DESCENDING_KEY
		}
		var err error
		vals[j], key, err = EncDatumFromBuffer(&types[j], enc, key)
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
	indexID, key, err := DecodeIndexKeyPrefix(tableDesc, entry.Key)
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
	indexTypes, err := GetColumnTypes(tableDesc, index.ColumnIDs)
	if err != nil {
		return nil, err
	}
	values := make([]EncDatum, len(index.ColumnIDs))
	dirs := make([]encoding.Direction, len(index.ColumnIDs))
	for i, dir := range index.ColumnDirections {
		dirs[i], err = dir.ToEncodingDirection()
		if err != nil {
			return nil, err
		}
	}
	if len(index.Interleave.Ancestors) > 0 {
		// TODO(dan): In the interleaved index case, we parse the key twice; once to
		// find the index id so we can look up the descriptor, and once to extract
		// the values. Only parse once.
		var ok bool
		_, ok, err = DecodeIndexKey(tableDesc, index, indexTypes, values, dirs, entry.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("descriptor did not match key")
		}
	} else {
		key, err = DecodeKeyVals(indexTypes, values, dirs, key)
		if err != nil {
			return nil, err
		}
	}

	// Extract the values for index.ExtraColumnIDs
	extraTypes, err := GetColumnTypes(tableDesc, index.ExtraColumnIDs)
	if err != nil {
		return nil, err
	}
	extraValues := make([]EncDatum, len(index.ExtraColumnIDs))
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
	_, err = DecodeKeyVals(extraTypes, extraValues, dirs, extraKey)
	if err != nil {
		return nil, err
	}

	// Encode the index key from its components.
	colMap := make(map[ColumnID]int)
	for i, columnID := range index.ColumnIDs {
		colMap[columnID] = i
	}
	for i, columnID := range index.ExtraColumnIDs {
		colMap[columnID] = i + len(index.ColumnIDs)
	}
	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)

	decodedValues := make([]tree.Datum, len(values)+len(extraValues))
	for i, value := range values {
		err := value.EnsureDecoded(&indexTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[i] = value.Datum
	}
	for i, value := range extraValues {
		err := value.EnsureDecoded(&extraTypes[i], a)
		if err != nil {
			return nil, err
		}
		decodedValues[len(values)+i] = value.Datum
	}
	indexKey, _, err := EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colMap, decodedValues, indexKeyPrefix)
	return indexKey, err
}

const datumAllocSize = 16 // Arbitrary, could be tuned.

// DatumAlloc provides batch allocation of datum pointers, amortizing the cost
// of the allocations.
type DatumAlloc struct {
	dintAlloc         []tree.DInt
	dfloatAlloc       []tree.DFloat
	dstringAlloc      []tree.DString
	dbytesAlloc       []tree.DBytes
	ddecimalAlloc     []tree.DDecimal
	ddateAlloc        []tree.DDate
	dtimeAlloc        []tree.DTime
	dtimestampAlloc   []tree.DTimestamp
	dtimestampTzAlloc []tree.DTimestampTZ
	dintervalAlloc    []tree.DInterval
	duuidAlloc        []tree.DUuid
	dipnetAlloc       []tree.DIPAddr
	djsonAlloc        []tree.DJSON
	doidAlloc         []tree.DOid
	scratch           []byte
	env               tree.CollationEnvironment
}

// NewDInt allocates a DInt.
func (a *DatumAlloc) NewDInt(v tree.DInt) *tree.DInt {
	buf := &a.dintAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DInt, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDFloat allocates a DFloat.
func (a *DatumAlloc) NewDFloat(v tree.DFloat) *tree.DFloat {
	buf := &a.dfloatAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DFloat, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDString allocates a DString.
func (a *DatumAlloc) NewDString(v tree.DString) *tree.DString {
	buf := &a.dstringAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DString, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDName allocates a DName.
func (a *DatumAlloc) NewDName(v tree.DString) tree.Datum {
	return tree.NewDNameFromDString(a.NewDString(v))
}

// NewDBytes allocates a DBytes.
func (a *DatumAlloc) NewDBytes(v tree.DBytes) *tree.DBytes {
	buf := &a.dbytesAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DBytes, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDecimal allocates a DDecimal.
func (a *DatumAlloc) NewDDecimal(v tree.DDecimal) *tree.DDecimal {
	buf := &a.ddecimalAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DDecimal, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDDate allocates a DDate.
func (a *DatumAlloc) NewDDate(v tree.DDate) *tree.DDate {
	buf := &a.ddateAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DDate, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTime allocates a DTime.
func (a *DatumAlloc) NewDTime(v tree.DTime) *tree.DTime {
	buf := &a.dtimeAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTime, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestamp allocates a DTimestamp.
func (a *DatumAlloc) NewDTimestamp(v tree.DTimestamp) *tree.DTimestamp {
	buf := &a.dtimestampAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimestamp, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDTimestampTZ allocates a DTimestampTZ.
func (a *DatumAlloc) NewDTimestampTZ(v tree.DTimestampTZ) *tree.DTimestampTZ {
	buf := &a.dtimestampTzAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DTimestampTZ, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDInterval allocates a DInterval.
func (a *DatumAlloc) NewDInterval(v tree.DInterval) *tree.DInterval {
	buf := &a.dintervalAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DInterval, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDUuid allocates a DUuid.
func (a *DatumAlloc) NewDUuid(v tree.DUuid) *tree.DUuid {
	buf := &a.duuidAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DUuid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDIPAddr allocates a DIPAddr.
func (a *DatumAlloc) NewDIPAddr(v tree.DIPAddr) *tree.DIPAddr {
	buf := &a.dipnetAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DIPAddr, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDJSON allocates a DJSON.
func (a *DatumAlloc) NewDJSON(v tree.DJSON) *tree.DJSON {
	buf := &a.djsonAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DJSON, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// NewDOid allocates a DOid.
func (a *DatumAlloc) NewDOid(v tree.DOid) tree.Datum {
	buf := &a.doidAlloc
	if len(*buf) == 0 {
		*buf = make([]tree.DOid, datumAllocSize)
	}
	r := &(*buf)[0]
	*r = v
	*buf = (*buf)[1:]
	return r
}

// DecodeTableKey decodes a table key/value.
func DecodeTableKey(
	a *DatumAlloc, valType types.T, key []byte, dir encoding.Direction,
) (tree.Datum, []byte, error) {
	if (dir != encoding.Ascending) && (dir != encoding.Descending) {
		return nil, nil, errors.Errorf("invalid direction: %d", dir)
	}
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return tree.DNull, key, nil
	}
	var rkey []byte
	var err error
	switch valType {
	case types.Bool:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		// No need to chunk allocate DBool as MakeDBool returns either
		// tree.DBoolTrue or tree.DBoolFalse.
		return tree.MakeDBool(tree.DBool(i != 0)), rkey, err
	case types.Int:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDInt(tree.DInt(i)), rkey, err
	case types.Float:
		var f float64
		if dir == encoding.Ascending {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		return a.NewDFloat(tree.DFloat(f)), rkey, err
	case types.Decimal:
		var d apd.Decimal
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		dd := a.NewDDecimal(tree.DDecimal{Decimal: d})
		return dd, rkey, err
	case types.String:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDString(tree.DString(r)), rkey, err
	case types.Name:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDName(tree.DString(r)), rkey, err
	case types.JSON:
		return tree.DNull, []byte{}, nil
	case types.Bytes:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return a.NewDBytes(tree.DBytes(r)), rkey, err
	case types.Date:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDDate(tree.DDate(t)), rkey, err
	case types.Time:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDTime(tree.DTime(t)), rkey, err
	case types.Timestamp:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: t}), rkey, err
	case types.TimestampTZ:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: t}), rkey, err
	case types.Interval:
		var d duration.Duration
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		return a.NewDInterval(tree.DInterval{Duration: d}), rkey, err
	case types.UUID:
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
		return a.NewDUuid(tree.DUuid{UUID: u}), rkey, err
	case types.INet:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		if err != nil {
			return nil, nil, err
		}
		var ipAddr ipaddr.IPAddr
		_, err := ipAddr.FromBuffer(r)
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), rkey, err
	case types.Oid:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDOid(tree.MakeDOid(tree.DInt(i))), rkey, err
	default:
		if _, ok := valType.(types.TCollatedString); ok {
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
func DecodeTableValue(a *DatumAlloc, valType types.T, b []byte) (tree.Datum, []byte, error) {
	_, dataOffset, _, typ, err := encoding.DecodeValueTag(b)
	if err != nil {
		return nil, b, err
	}
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		return tree.DNull, b[dataOffset:], nil
	}
	// Bool is special because the value is stored in the value tag.
	if valType != types.Bool {
		b = b[dataOffset:]
	}
	return decodeUntaggedDatum(a, valType, b)
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

func decodeArray(a *DatumAlloc, elementType types.T, b []byte) (tree.Datum, []byte, error) {
	b, _, _, err := encoding.DecodeNonsortingUvarint(b)
	if err != nil {
		return nil, b, err
	}
	header, b, err := decodeArrayHeader(b)
	if err != nil {
		return nil, b, err
	}
	result := tree.DArray{
		Array:    make(tree.Datums, header.length),
		ParamTyp: elementType,
	}
	var val tree.Datum
	for i := uint64(0); i < header.length; i++ {
		if header.isNull(i) {
			result.Array[i] = tree.DNull
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
//
// If t is types.Bool, the value tag must be present, as its value is encoded in
// the tag directly.
func decodeUntaggedDatum(a *DatumAlloc, t types.T, buf []byte) (tree.Datum, []byte, error) {
	switch t {
	case types.Int:
		b, i, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDInt(tree.DInt(i)), b, nil
	case types.String, types.Name:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDString(tree.DString(data)), b, nil
	case types.Bool:
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function.
		b, data, err := encoding.DecodeBoolValue(buf)
		if err != nil {
			return nil, b, err
		}
		return tree.MakeDBool(tree.DBool(data)), b, nil
	case types.Float:
		b, data, err := encoding.DecodeUntaggedFloatValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDFloat(tree.DFloat(data)), b, nil
	case types.Decimal:
		b, data, err := encoding.DecodeUntaggedDecimalValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDecimal(tree.DDecimal{Decimal: data}), b, nil
	case types.Bytes:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDBytes(tree.DBytes(data)), b, nil
	case types.Date:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDDate(tree.DDate(data)), b, nil
	case types.Time:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTime(tree.DTime(data)), b, nil
	case types.Timestamp:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: data}), b, nil
	case types.TimestampTZ:
		b, data, err := encoding.DecodeUntaggedTimeValue(buf)
		if err != nil {
			return nil, b, err
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: data}), b, nil
	case types.Interval:
		b, data, err := encoding.DecodeUntaggedDurationValue(buf)
		return a.NewDInterval(tree.DInterval{Duration: data}), b, err
	case types.UUID:
		b, data, err := encoding.DecodeUntaggedUUIDValue(buf)
		return a.NewDUuid(tree.DUuid{UUID: data}), b, err
	case types.INet:
		b, data, err := encoding.DecodeUntaggedIPAddrValue(buf)
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: data}), b, err
	case types.JSON:
		b, data, err := encoding.DecodeUntaggedBytesValue(buf)
		if err != nil {
			return nil, b, err
		}
		j, err := json.FromEncoding(data)
		if err != nil {
			return nil, b, err
		}
		return a.NewDJSON(tree.DJSON{JSON: j}), b, nil
	case types.Oid:
		b, data, err := encoding.DecodeUntaggedIntValue(buf)
		return a.NewDOid(tree.MakeDOid(tree.DInt(data))), b, err
	default:
		switch typ := t.(type) {
		case types.TCollatedString:
			b, data, err := encoding.DecodeUntaggedBytesValue(buf)
			return tree.NewDCollatedString(string(data), typ.Locale, &a.env), b, err
		case types.TArray:
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

// EncodeInvertedIndexKeys creates a list of inverted index keys
// by concatenating keyPrefix with the encodings of the column in the index.
//
// Returns the key and whether any of the encoded values were NULLs.
func EncodeInvertedIndexKeys(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key [][]byte, err error) {
	if len(index.ColumnIDs) > 1 {
		return nil, pgerror.NewError(pgerror.CodeInternalError,
			"trying to apply inverted index to more than one column")
	}

	var val tree.Datum
	if i, ok := colMap[index.ColumnIDs[0]]; ok {
		val = values[i]
	} else {
		val = tree.DNull
	}

	return EncodeInvertedIndexTableKeys(val, keyPrefix)
}

// EncodeInvertedIndexTableKeys encodes the paths in a JSON `val` and concatenates it with `inKey`and returns
// a list of buffers per path. The encoded values is guaranteed to be lexicographically sortable, but not
// guaranteed to be round-trippable during decoding.
func EncodeInvertedIndexTableKeys(val tree.Datum, inKey []byte) (key [][]byte, err error) {
	if val == tree.DNull {
		return [][]byte{encoding.EncodeNullAscending(inKey)}, nil
	}
	switch t := tree.UnwrapDatum(nil, val).(type) {
	case *tree.DJSON:
		return json.EncodeInvertedIndexKeys(inKey, (t.JSON))
	}
	return nil, pgerror.NewError(pgerror.CodeInternalError, "trying to apply inverted index to non JSON type")
}

// EncodeSecondaryIndex encodes key/values for a secondary index. colMap maps
// ColumnIDs to indices in `values`. This returns a slice of IndexEntry. Forward
// indexes will return one value, while inverted indicies can return multiple values.
func EncodeSecondaryIndex(
	tableDesc *TableDescriptor,
	secondaryIndex *IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
) ([]IndexEntry, error) {
	secondaryIndexKeyPrefix := MakeIndexKeyPrefix(tableDesc, secondaryIndex.ID)

	var containsNull = false
	var secondaryKeys [][]byte
	var err error
	if secondaryIndex.Type == IndexDescriptor_INVERTED {
		secondaryKeys, err = EncodeInvertedIndexKeys(tableDesc, secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
	} else {
		var secondaryIndexKey []byte
		secondaryIndexKey, containsNull, err = EncodeIndexKey(
			tableDesc, secondaryIndex, colMap, values, secondaryIndexKeyPrefix)

		secondaryKeys = [][]byte{secondaryIndexKey}
	}
	if err != nil {
		return []IndexEntry{}, err
	}

	// Add the extra columns - they are encoded in ascending order which is done
	// by passing nil for the encoding directions.
	extraKey, _, err := EncodeColumns(secondaryIndex.ExtraColumnIDs, nil,
		colMap, values, nil)
	if err != nil {
		return []IndexEntry{}, err
	}

	var entries = make([]IndexEntry, len(secondaryKeys))
	for i, key := range secondaryKeys {
		entry := IndexEntry{Key: key}

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
			val := findColumnValue(col.id, colMap, values)
			if val == tree.DNull || (col.isComposite && !val.(tree.CompositeDatum).IsComposite()) {
				continue
			}
			if lastColID > col.id {
				panic(fmt.Errorf("cannot write column id %d after %d", col.id, lastColID))
			}
			colIDDiff := col.id - lastColID
			lastColID = col.id
			entryValue, err = EncodeTableValue(entryValue, colIDDiff, val, nil)
			if err != nil {
				return []IndexEntry{}, err
			}
		}
		entry.Value.SetBytes(entryValue)
		entries[i] = entry
	}

	return entries, nil
}

// EncodeSecondaryIndexes encodes key/values for the secondary indexes. colMap
// maps ColumnIDs to indices in `values`. secondaryIndexEntries is the return
// value (passed as a parameter so the caller can reuse between rows) and is
// expected to be the same length as indexes.
func EncodeSecondaryIndexes(
	tableDesc *TableDescriptor,
	indexes []IndexDescriptor,
	colMap map[ColumnID]int,
	values []tree.Datum,
	secondaryIndexEntries []IndexEntry,
) ([]IndexEntry, error) {
	if len(secondaryIndexEntries) != len(indexes) {
		panic("Length of secondaryIndexEntries is not equal to the number of indexes.")
	}
	for i := range indexes {
		entries, err := EncodeSecondaryIndex(tableDesc, &indexes[i], colMap, values)
		if err != nil {
			return secondaryIndexEntries, err
		}
		secondaryIndexEntries[i] = entries[0]

		// This is specifically for inverted indexes which can have more than one entry
		// associated with them.
		if len(entries) > 1 {
			secondaryIndexEntries = append(secondaryIndexEntries, entries[1:]...)
		}
	}
	return secondaryIndexEntries, nil
}

// CheckColumnType verifies that a given value is compatible
// with the type requested by the column. If the value is a
// placeholder, the type of the placeholder gets populated.
func CheckColumnType(col ColumnDescriptor, typ types.T, pmap *tree.PlaceholderInfo) error {
	if typ == types.Unknown {
		return nil
	}

	// If the value is a placeholder, then the column check above has
	// populated 'colTyp' with a type to assign to it.
	colTyp := col.Type.ToDatumType()
	if p, pok := typ.(types.TPlaceholder); pok {
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

func checkElementType(paramType types.T, columnType ColumnType) error {
	semanticType, err := DatumTypeToColumnSemanticType(paramType)
	if err != nil {
		return err
	}
	if semanticType != *columnType.ArrayContents {
		return errors.Errorf("type of array contents %s doesn't match column type %s",
			paramType, columnType.ArrayContents)
	}
	if cs, ok := paramType.(types.TCollatedString); ok {
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
func MarshalColumnValue(col ColumnDescriptor, val tree.Datum) (roachpb.Value, error) {
	var r roachpb.Value

	if val == tree.DNull {
		return r, nil
	}

	switch col.Type.SemanticType {
	case ColumnType_BOOL:
		if v, ok := val.(*tree.DBool); ok {
			r.SetBool(bool(*v))
			return r, nil
		}
	case ColumnType_INT:
		if v, ok := tree.AsDInt(val); ok {
			r.SetInt(int64(v))
			return r, nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(*tree.DFloat); ok {
			r.SetFloat(float64(*v))
			return r, nil
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*tree.DDecimal); ok {
			err := r.SetDecimal(&v.Decimal)
			return r, err
		}
	case ColumnType_STRING, ColumnType_NAME:
		if v, ok := tree.AsDString(val); ok {
			r.SetString(string(v))
			return r, nil
		}
	case ColumnType_BYTES:
		if v, ok := val.(*tree.DBytes); ok {
			r.SetString(string(*v))
			return r, nil
		}
	case ColumnType_DATE:
		if v, ok := val.(*tree.DDate); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case ColumnType_TIME:
		if v, ok := val.(*tree.DTime); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case ColumnType_TIMESTAMP:
		if v, ok := val.(*tree.DTimestamp); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case ColumnType_TIMESTAMPTZ:
		if v, ok := val.(*tree.DTimestampTZ); ok {
			r.SetTime(v.Time)
			return r, nil
		}
	case ColumnType_INTERVAL:
		if v, ok := val.(*tree.DInterval); ok {
			err := r.SetDuration(v.Duration)
			return r, err
		}
	case ColumnType_UUID:
		if v, ok := val.(*tree.DUuid); ok {
			r.SetBytes(v.GetBytes())
			return r, nil
		}
	case ColumnType_INET:
		if v, ok := val.(*tree.DIPAddr); ok {
			data := v.ToBuffer(nil)
			r.SetBytes(data)
			return r, nil
		}
	case ColumnType_JSON:
		if v, ok := val.(*tree.DJSON); ok {
			data, err := json.EncodeJSON(nil, v.JSON)
			if err != nil {
				return r, err
			}
			r.SetBytes(data)
			return r, nil
		}
	case ColumnType_ARRAY:
		if v, ok := val.(*tree.DArray); ok {
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
		if v, ok := val.(*tree.DCollatedString); ok {
			if v.Locale == *col.Type.Locale {
				r.SetString(v.Contents)
				return r, nil
			}
			return r, fmt.Errorf("locale %q doesn't match locale %q of column %q",
				v.Locale, *col.Type.Locale, col.Name)
		}
	case ColumnType_OID:
		if v, ok := val.(*tree.DOid); ok {
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

func encodeArray(d *tree.DArray, scratch []byte) ([]byte, error) {
	if err := d.Validate(); err != nil {
		return scratch, err
	}
	scratch = scratch[0:0]
	unwrapped := types.UnwrapType(d.ParamTyp)
	elementType, err := parserTypeToEncodingType(unwrapped)

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
		if d.HasNulls && e == tree.DNull {
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

func parserTypeToEncodingType(t types.T) (encoding.Type, error) {
	switch t {
	case types.Int:
		return encoding.Int, nil
	case types.Oid:
		return encoding.Int, nil
	case types.Float:
		return encoding.Float, nil
	case types.Decimal:
		return encoding.Decimal, nil
	case types.Bytes, types.String, types.Name:
		return encoding.Bytes, nil
	case types.Timestamp, types.TimestampTZ:
		return encoding.Time, nil
	// Note: types.Date was incorrectly mapped to encoding.Time when arrays were
	// first introduced. If any 1.1 users used date arrays, they would have been
	// persisted with incorrect elementType values.
	case types.Date, types.Time:
		return encoding.Int, nil
	case types.Interval:
		return encoding.Duration, nil
	case types.Bool:
		return encoding.True, nil
	case types.UUID:
		return encoding.UUID, nil
	case types.INet:
		return encoding.IPAddr, nil
	default:
		if t.FamilyEqual(types.FamCollatedString) {
			return encoding.Bytes, nil
		}
		return 0, errors.Errorf("Don't know encoding type for %s", t)
	}
}

func encodeArrayElement(b []byte, d tree.Datum) ([]byte, error) {
	switch t := d.(type) {
	case *tree.DInt:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DString:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *tree.DBytes:
		bytes := []byte(*t)
		b = encoding.EncodeUntaggedBytesValue(b, bytes)
		return b, nil
	case *tree.DFloat:
		return encoding.EncodeUntaggedFloatValue(b, float64(*t)), nil
	case *tree.DBool:
		return encoding.EncodeBoolValue(b, encoding.NoColumnID, bool(*t)), nil
	case *tree.DDecimal:
		return encoding.EncodeUntaggedDecimalValue(b, &t.Decimal), nil
	case *tree.DDate:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DTime:
		return encoding.EncodeUntaggedIntValue(b, int64(*t)), nil
	case *tree.DTimestamp:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *tree.DTimestampTZ:
		return encoding.EncodeUntaggedTimeValue(b, t.Time), nil
	case *tree.DInterval:
		return encoding.EncodeUntaggedDurationValue(b, t.Duration), nil
	case *tree.DUuid:
		return encoding.EncodeUntaggedUUIDValue(b, t.UUID), nil
	case *tree.DIPAddr:
		return encoding.EncodeUntaggedIPAddrValue(b, t.IPAddr), nil
	case *tree.DOid:
		return encoding.EncodeUntaggedIntValue(b, int64(t.DInt)), nil
	case *tree.DCollatedString:
		return encoding.EncodeUntaggedBytesValue(b, []byte(t.Contents)), nil
	}
	return nil, errors.Errorf("don't know how to encode %s", d)
}

// UnmarshalColumnValue decodes the value from a key-value pair using the type
// expected by the column. An error is returned if the value's type does not
// match the column's type.
func UnmarshalColumnValue(a *DatumAlloc, typ ColumnType, value roachpb.Value) (tree.Datum, error) {
	if value.RawBytes == nil {
		return tree.DNull, nil
	}

	switch typ.SemanticType {
	case ColumnType_BOOL:
		v, err := value.GetBool()
		if err != nil {
			return nil, err
		}
		return tree.MakeDBool(tree.DBool(v)), nil
	case ColumnType_INT:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDInt(tree.DInt(v)), nil
	case ColumnType_FLOAT:
		v, err := value.GetFloat()
		if err != nil {
			return nil, err
		}
		return a.NewDFloat(tree.DFloat(v)), nil
	case ColumnType_DECIMAL:
		v, err := value.GetDecimal()
		if err != nil {
			return nil, err
		}
		dd := a.NewDDecimal(tree.DDecimal{Decimal: v})
		return dd, nil
	case ColumnType_STRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDString(tree.DString(v)), nil
	case ColumnType_BYTES:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDBytes(tree.DBytes(v)), nil
	case ColumnType_DATE:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDDate(tree.DDate(v)), nil
	case ColumnType_TIME:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDTime(tree.DTime(v)), nil
	case ColumnType_TIMESTAMP:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestamp(tree.DTimestamp{Time: v}), nil
	case ColumnType_TIMESTAMPTZ:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return a.NewDTimestampTZ(tree.DTimestampTZ{Time: v}), nil
	case ColumnType_INTERVAL:
		d, err := value.GetDuration()
		if err != nil {
			return nil, err
		}
		return a.NewDInterval(tree.DInterval{Duration: d}), nil
	case ColumnType_COLLATEDSTRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return tree.NewDCollatedString(string(v), *typ.Locale, &a.env), nil
	case ColumnType_UUID:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		u, err := uuid.FromBytes(v)
		if err != nil {
			return nil, err
		}
		return a.NewDUuid(tree.DUuid{UUID: u}), nil
	case ColumnType_INET:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		var ipAddr ipaddr.IPAddr
		_, err = ipAddr.FromBuffer(v)
		if err != nil {
			return nil, err
		}
		return a.NewDIPAddr(tree.DIPAddr{IPAddr: ipAddr}), nil
	case ColumnType_NAME:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return a.NewDName(tree.DString(v)), nil
	case ColumnType_OID:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return a.NewDOid(tree.MakeDOid(tree.DInt(v))), nil
	default:
		return nil, errors.Errorf("unsupported column type: %s", typ.SemanticType)
	}
}

// CheckValueWidth checks that the width (for strings, byte arrays, and
// bit string) and scale (for decimals) of the value fits the specified
// column type. Used by INSERT and UPDATE.
func CheckValueWidth(typ ColumnType, val tree.Datum, name string) error {
	switch typ.SemanticType {
	case ColumnType_STRING:
		if v, ok := tree.AsDString(val); ok {
			if typ.Width > 0 && utf8.RuneCountInString(string(v)) > int(typ.Width) {
				return fmt.Errorf("value too long for type %s (column %q)",
					typ.SQLString(), name)
			}
		}
	case ColumnType_INT:
		if v, ok := tree.AsDInt(val); ok {
			if typ.Width > 0 {

				// Width is defined in bits.
				width := uint(typ.Width - 1)

				// https://www.postgresql.org/docs/9.5/static/datatype-bit.html
				// "bit type data must match the length n exactly; it is an error
				// to attempt to store shorter or longer bit strings. bit varying
				// data is of variable length up to the maximum length n; longer
				// strings will be rejected." Bits are unsigned, so we need to
				// increase the width for the type check below.
				// TODO(nvanbenschoten): Because we do not propagate the "varying"
				if typ.VisibleType == ColumnType_BIT {
					width = uint(typ.Width)
				}

				// We're performing bounds checks inline with Go's implementation of min and max ints in Math.go.
				shifted := v >> width
				if (v >= 0 && shifted > 0) || (v < 0 && shifted < -1) {
					return fmt.Errorf("integer out of range for type %s (column %q)", typ.VisibleType, name)
				}
			}
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*tree.DDecimal); ok {
			if err := tree.LimitDecimalWidth(&v.Decimal, int(typ.Precision), int(typ.Width)); err != nil {
				return errors.Wrapf(err, "type %s (column %q)", typ.SQLString(), name)
			}
		}
	case ColumnType_ARRAY:
		if v, ok := val.(*tree.DArray); ok {
			elementType := *typ.elementColumnType()
			for i := range v.Array {
				if err := CheckValueWidth(elementType, v.Array[i], name); err != nil {
					return err
				}
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
func (desc *TableDescriptor) GetConstraintInfo(
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
func (desc *TableDescriptor) GetConstraintInfoWithLookup(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc *TableDescriptor) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc *TableDescriptor) collectConstraintInfo(
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
			numCols := len(index.ColumnIDs)
			if index.ForeignKey.SharedPrefixLen > 0 {
				numCols = int(index.ForeignKey.SharedPrefixLen)
			}
			detail.Columns = index.ColumnNames[:numCols]
			detail.Index = index

			if tableLookup != nil {
				other, err := tableLookup(index.ForeignKey.Table)
				if err != nil {
					return nil, errors.Wrapf(err, "error resolving table %d referenced in foreign key",
						index.ForeignKey.Table)
				}
				otherIdx, err := other.FindIndexByID(index.ForeignKey.Index)
				if err != nil {
					return nil, errors.Wrapf(err, "error resolving index %d in table %s referenced "+
						"in foreign key", index.ForeignKey.Index, other.Name)
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

			colsUsed, err := c.ColumnsUsed(desc)
			if err != nil {
				return nil, errors.Wrapf(err, "error computing columns used in check constraint %q", c.Name)
			}
			for _, colID := range colsUsed {
				col, err := desc.FindColumnByID(colID)
				if err != nil {
					return nil, errors.Wrapf(err, "error finding column %d in table %s", colID, desc.Name)
				}
				detail.Columns = append(detail.Columns, col.Name)
			}
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
	datums := make([]tree.Datum, len(vals))
	for i, v := range vals {
		switch v := v.(type) {
		case bool:
			datums[i] = tree.MakeDBool(tree.DBool(v))
		case int:
			datums[i] = tree.NewDInt(tree.DInt(v))
		case string:
			datums[i] = tree.NewDString(v)
		case tree.Datum:
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

// IndexKeyEquivSignature parses an index key if and only if the index key
// belongs to a table where its equivalence signature and all its interleave
// ancestors' signatures can be found in validEquivSignatures.
// validEquivSignatures: a map containing equivalence signatures of valid
// ancestors of the desired table and of the desired table itself.
// IndexKeyEquivSignature returns whether or not the index key satisfies the
// above condition, the value mapped to by the desired table (could be a table index),
// and the rest of the key that's not part of the signature.
// It also requires two []byte buffers: one for the signature (signatureBuf)
// and one for the rest of the key (keyRestBuf).
// The equivalence signature defines the equivalence classes for the signature
// of potentially interleaved tables. For example, the equivalence signatures
// for the following interleaved indexes
//    <parent@primary>
//	<child@secondary>
// and index keys
//    <parent index key>:   /<parent table id>/<parent index id>/<val 1>/<val 2>
//    <child index key>:    /<parent table id>/<parent index id>/<val 1>/<val 2>/#/<child table id>/child index id>/<val 3>/<val 4>
// correspond to the equivalence signatures
//    <parent@primary>:	    /<parent table id>/<parent index id>
//    <child@secondary>:    /<parent table id>/<parent index id>/#/<child table id>/<child index id>
// Equivalence signatures allow us to associate an index key with its table
// without having to invoke DecodeIndexKey multiple times.
// IndexKeyEquivSignature will return false if the a table's ancestor's
// signature or the table's signature (table which the index key belongs to) is
// not mapped in validEquivSignatures.
// For example, suppose the given key is
//    /<t2 table id>/<t2 index id>/<val t2>/#/<t3 table id>/<t3 table id>/<val t3>
// and validEquivSignatures contains
//    /<t1 table id>/t1 index id>
//    /<t1 table id>/t1 index id>/#/<t4 table id>/<t4 index id
// IndexKeyEquivSignature will short-circuit and return false once
//    /<t2 table id>/<t2 index id>
// is processed since t2's signature is not specified in validEquivSignatures.
func IndexKeyEquivSignature(
	key []byte, validEquivSignatures map[string]int, signatureBuf []byte, restBuf []byte,
) (tableIdx int, restResult []byte, success bool, err error) {
	signatureBuf = signatureBuf[:0]
	restResult = restBuf[:0]
	for {
		// Well-formed key is guaranteed to to have 2 varints for every
		// ancestor: the TableID and IndexID.
		// We extract these out and add them to our buffer.
		for i := 0; i < 2; i++ {
			idLen, err := encoding.PeekLength(key)
			if err != nil {
				return 0, nil, false, err
			}
			signatureBuf = append(signatureBuf, key[:idLen]...)
			key = key[idLen:]
		}

		// The current signature (either an ancestor table's or the key's)
		// is not one of the validEquivSignatures.
		// We can short-circuit and return false.
		recentTableIdx, found := validEquivSignatures[string(signatureBuf)]
		if !found {
			return 0, nil, false, nil
		}

		var isSentinel bool
		// Peek and discard encoded index values.
		for {
			key, isSentinel = encoding.DecodeIfInterleavedSentinel(key)
			// We stop once the key is empty or if we encounter a
			// sentinel for the next TableID-IndexID pair.
			if len(key) == 0 || isSentinel {
				break
			}
			len, err := encoding.PeekLength(key)
			if err != nil {
				return 0, nil, false, err
			}
			// Append any other bytes (column values initially,
			// then family ID and timestamp) to return.
			restResult = append(restResult, key[:len]...)
			key = key[len:]
		}

		if !isSentinel {
			// The key has been fully decomposed and is valid up to
			// this point.
			// Return the most recent table index from
			// validEquivSignatures.
			return recentTableIdx, restResult, true, nil
		}
		// If there was a sentinel, we know there are more
		// descendant(s).
		// We insert an interleave sentinel and continue extracting the
		// next descendant's IDs.
		signatureBuf = encoding.EncodeInterleavedSentinel(signatureBuf)
	}
}

// TableEquivSignatures returns the equivalence signatures for each interleave
// ancestor and itself. See IndexKeyEquivSignature for more info.
func TableEquivSignatures(
	desc *TableDescriptor, index *IndexDescriptor,
) (signatures [][]byte, err error) {
	// signatures contains the slice reference to the signature of every
	// ancestor of the current table-index.
	// The last slice reference is the given table-index's signature.
	signatures = make([][]byte, len(index.Interleave.Ancestors)+1)
	// fullSignature is the backing byte slice for each individual signature
	// as it buffers each block of table and index IDs.
	// We eagerly allocate 4 bytes for each of the two IDs per ancestor
	// (which can fit Uvarint IDs up to 2^17-1 without another allocation),
	// 1 byte for each interleave sentinel, and 4 bytes each for the given
	// table's and index's ID.
	fullSignature := make([]byte, 0, len(index.Interleave.Ancestors)*9+8)

	// Encode the table's ancestors' TableIDs and IndexIDs.
	for i, ancestor := range index.Interleave.Ancestors {
		fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(ancestor.TableID))
		fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(ancestor.IndexID))
		// Create a reference up to this point for the ancestor's
		// signature.
		signatures[i] = fullSignature
		// Append Interleave sentinel after every ancestor.
		fullSignature = encoding.EncodeInterleavedSentinel(fullSignature)
	}

	// Encode the table's table and index IDs.
	fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(desc.ID))
	fullSignature = encoding.EncodeUvarintAscending(fullSignature, uint64(index.ID))
	// Create a reference for the given table's signature as the last
	// element of signatures.
	signatures[len(signatures)-1] = fullSignature

	return signatures, nil
}

// maxKeyTokens returns the maximum number of key tokens in an index's key,
// including the table ID, index ID, and index column values (including extra
// columns that may be stored in the key).
// It requires knowledge of whether the key will or might contain a NULL value:
// if uncertain, pass in true to 'overestimate' the maxKeyTokens.
//
// In general, a key belonging to an interleaved index grandchild is encoded as:
//
//    /table/index/<parent-pk1>/.../<parent-pkX>/#/table/index/<child-pk1>/.../<child-pkY>/#/table/index/<grandchild-pk1>/.../<grandchild-pkZ>
//
// The part of the key with respect to the grandchild index would be
// the entire key since there are no grand-grandchild table/index IDs or
// <grandgrandchild-pk>. The maximal prefix of the key that belongs to child is
//
//    /table/index/<parent-pk1>/.../<parent-pkX>/#/table/index/<child-pk1>/.../<child-pkY>
//
// and the maximal prefix of the key that belongs to parent is
//
//    /table/index/<parent-pk1>/.../<parent-pkX>
//
// We return the maximum number of <tokens> in this prefix.
func maxKeyTokens(index *IndexDescriptor, containsNull bool) int {
	nTables := len(index.Interleave.Ancestors) + 1
	nKeyCols := len(index.ColumnIDs)

	// Non-unique secondary indexes or unique secondary indexes with a NULL
	// value have additional columns in the key that may appear in a span
	// (e.g. primary key columns not part of the index).
	// See EncodeSecondaryIndex.
	if !index.Unique || containsNull {
		nKeyCols += len(index.ExtraColumnIDs)
	}

	// To illustrate how we compute max # of key tokens, take the
	// key in the example above and let the respective index be child.
	// We'd like to return the number of bytes in
	//
	//    /table/index/<parent-pk1>/.../<parent-pkX>/#/table/index/<child-pk1>/.../<child-pkY>
	// For each table-index, there is
	//    1. table ID
	//    2. index ID
	//    3. interleave sentinel
	// or 3 * nTables.
	// Each <parent-pkX> must be a part of the index's columns (nKeys).
	// Finally, we do not want to include the interleave sentinel for the
	// current index (-1).
	return 3*nTables + nKeyCols - 1
}

// AdjustStartKeyForInterleave adjusts the start key to skip unnecessary
// interleaved sections.
//
// For example, if child is interleaved into parent, a typical parent
// span might look like
//    /1 - /3
// and a typical child span might look like
//    /1/#/2 - /2/#/5
// Suppose the parent span is
//    /1/#/2 - /3
// where the start key is a child's index key. Notice that the first parent
// key read actually starts at /2 since all the parent keys with the prefix
// /1 come before the child key /1/#/2 (and is not read in the span).
// We can thus push forward the start key from /1/#/2 to /2. If the start key
// was /1, we cannot push this forwards since that is the first key we want
// to read.
func AdjustStartKeyForInterleave(index *IndexDescriptor, start roachpb.Key) (roachpb.Key, error) {
	keyTokens, containsNull, err := encoding.DecomposeKeyTokens(start)
	if err != nil {
		return roachpb.Key{}, err
	}
	nIndexTokens := maxKeyTokens(index, containsNull)

	// This is either the index's own key or one of its ancestor's key.
	// Nothing to do.
	if len(keyTokens) <= nIndexTokens {
		return start, nil
	}

	// len(keyTokens) > nIndexTokens, so this must be a child key.
	// Transform /1/#/2 --> /2.
	firstNTokenLen := 0
	for _, token := range keyTokens[:nIndexTokens] {
		firstNTokenLen += len(token)
	}

	return start[:firstNTokenLen].PrefixEnd(), nil
}

// AdjustEndKeyForInterleave returns an exclusive end key. It does two things:
//    - determines the end key based on the prior: inclusive vs exclusive
//    - adjusts the end key to skip unnecessary interleaved sections
//
// For example, the parent span composed from the filter PK >= 1 and PK < 3 is
//    /1 - /3
// This reads all keys up to the first parent key for PK = 3. If parent had
// interleaved tables and keys, it would unnecessarily scan over interleaved
// rows under PK2 (e.g. /2/#/5).
// We can instead "tighten" or adjust the end key from /3 to /2/#.
// DO NOT pass in any keys that have been invoked with PrefixEnd: this may
// cause issues when trying to decode the key tokens.
// AdjustEndKeyForInterleave is idempotent upon successive invocation(s).
func AdjustEndKeyForInterleave(
	table *TableDescriptor, index *IndexDescriptor, end roachpb.Key, inclusive bool,
) (roachpb.Key, error) {
	if index.Type == IndexDescriptor_INVERTED {
		return end.PrefixEnd(), nil
	}

	// To illustrate, suppose we have the interleaved hierarchy
	//    parent
	//	child
	//	  grandchild
	// Suppose our target index is child.
	keyTokens, containsNull, err := encoding.DecomposeKeyTokens(end)
	if err != nil {
		return roachpb.Key{}, err
	}
	nIndexTokens := maxKeyTokens(index, containsNull)

	// Sibling/nibling keys: it is possible for this key to be part
	// of a sibling tree in the interleaved hierarchy, especially after
	// partitioning on range split keys.
	// As such, a sibling may be interpretted as an ancestor (if the sibling
	// has fewer key-encoded columns) or a descendant (if the sibling has
	// more key-encoded columns). Similarly for niblings.
	// This is fine because if the sibling is sorted before or after the
	// current index (child in our example), it is not possible for us to
	// adjust the sibling key such that we add or remove child (the current
	// index's) rows from our span.

	if index.ID != table.PrimaryIndex.ID || len(keyTokens) < nIndexTokens {
		// Case 1: secondary index, parent key or partial child key:
		// Secondary indexes cannot have interleaved rows.
		// We cannot adjust or tighten parent keys with respect to a
		// child index.
		// Partial child keys e.g. /1/#/1 vs /1/#/1/2 cannot have
		// interleaved rows.
		// Nothing to do besides making the end key exclusive if it was
		// initially inclusive.
		if inclusive {
			end = end.PrefixEnd()
		}
		return end, nil
	}

	if len(keyTokens) == nIndexTokens {
		// Case 2: child key

		lastToken := keyTokens[len(keyTokens)-1]
		_, isNotNullDesc := encoding.DecodeIfNotNullDescending(lastToken)
		// If this is the child's key and the last value in the key is
		// NotNullDesc, then it does not need (read: shouldn't) to be
		// tightened.
		// For example, the query with IS NOT NULL may generate
		// the end key
		//    /1/#/NOTNULLDESC
		if isNotNullDesc {
			if inclusive {
				end = end.PrefixEnd()
			}
			return end, nil
		}

		// We only want to UndoPrefixEnd if the end key passed is not
		// inclusive initially.
		if !inclusive {
			lastType := encoding.PeekType(lastToken)
			if lastType == encoding.Bytes || lastType == encoding.BytesDesc || lastType == encoding.Decimal {
				// If the last value is of type Decimals or
				// Bytes then this is more difficult since the
				// escape term is the last value.
				// TODO(richardwu): Figure out how to go back 1
				// logical bytes/decimal value.
				return end, nil
			}

			// We first iterate back to the previous key value
			//    /1/#/1 --> /1/#/0
			undoPrefixEnd, ok := encoding.UndoPrefixEnd(end)
			if !ok {
				return end, nil
			}
			end = undoPrefixEnd
		}

		// /1/#/0 --> /1/#/0/#
		return encoding.EncodeInterleavedSentinel(end), nil
	}

	// len(keyTokens) > nIndexTokens
	// Case 3: tightened child, sibling/nibling, or grandchild key

	// Case 3a: tightened child key
	// This could from a previous invocation of AdjustEndKeyForInterleave.
	// For example, if during index selection the key for child was
	// tightened
	//	/1/#/2 --> /1/#/1/#
	// We don't really want to tighten on '#' again.
	if _, isSentinel := encoding.DecodeIfInterleavedSentinel(keyTokens[nIndexTokens]); isSentinel && len(keyTokens)-1 == nIndexTokens {
		if inclusive {
			end = end.PrefixEnd()
		}
		return end, nil
	}

	// Case 3b/c: sibling/nibling or grandchild key
	// Ideally, we want to form
	//    /1/#/2/#/3 --> /1/#/2/#
	// We truncate up to and including the interleave sentinel (or next
	// sibling/nibling column value) after the last index key token.
	firstNTokenLen := 0
	for _, token := range keyTokens[:nIndexTokens] {
		firstNTokenLen += len(token)
	}

	return end[:firstNTokenLen+1], nil
}
