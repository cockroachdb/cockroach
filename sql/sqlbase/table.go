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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sqlbase

import (
	"fmt"
	"time"
	"unicode/utf8"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

func exprContainsVarsError(context string, Expr parser.Expr) error {
	return fmt.Errorf("%s expression '%s' may not contain variable sub-expressions", context, Expr)
}

func incompatibleExprTypeError(context string, expectedType parser.Datum, actualType parser.Datum) error {
	return fmt.Errorf("incompatible type for %s expression: %s vs %s",
		context, expectedType.Type(), actualType.Type())
}

// SanitizeVarFreeExpr verifies a default expression is valid, has the
// correct type and contains no variable expressions.
func SanitizeVarFreeExpr(expr parser.Expr, expectedType parser.Datum, context string) error {
	if parser.ContainsVars(expr) {
		return exprContainsVarsError(context, expr)
	}
	typedExpr, err := parser.TypeCheck(expr, nil, expectedType)
	if err != nil {
		return err
	}
	if defaultType := typedExpr.ReturnType(); !expectedType.TypeEqual(defaultType) {
		return incompatibleExprTypeError(context, expectedType, defaultType)
	}
	return nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
func MakeColumnDefDescs(d *parser.ColumnTableDef) (*ColumnDescriptor, *IndexDescriptor, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != parser.NotNull && !d.PrimaryKey,
	}

	if d.Nullable.ConstraintName != "" {
		col.NullableConstraintName = string(d.Nullable.ConstraintName)
	}

	var colDatumType parser.Datum
	switch t := d.Type.(type) {
	case *parser.BoolColType:
		col.Type.Kind = ColumnType_BOOL
		colDatumType = parser.TypeBool
	case *parser.IntColType:
		col.Type.Kind = ColumnType_INT
		col.Type.Width = int32(t.N)
		colDatumType = parser.TypeInt
		if t.IsSerial() {
			if d.DefaultExpr.Expr != nil {
				return nil, nil, fmt.Errorf("SERIAL column %q cannot have a default value", col.Name)
			}
			s := "unique_rowid()"
			col.DefaultExpr = &s
		}
	case *parser.FloatColType:
		col.Type.Kind = ColumnType_FLOAT
		col.Type.Precision = int32(t.Prec)
		colDatumType = parser.TypeFloat
	case *parser.DecimalColType:
		col.Type.Kind = ColumnType_DECIMAL
		col.Type.Width = int32(t.Scale)
		col.Type.Precision = int32(t.Prec)
		colDatumType = parser.TypeDecimal
	case *parser.DateColType:
		col.Type.Kind = ColumnType_DATE
		colDatumType = parser.TypeDate
	case *parser.TimestampColType:
		col.Type.Kind = ColumnType_TIMESTAMP
		colDatumType = parser.TypeTimestamp
	case *parser.TimestampTZColType:
		col.Type.Kind = ColumnType_TIMESTAMPTZ
		colDatumType = parser.TypeTimestampTZ
	case *parser.IntervalColType:
		col.Type.Kind = ColumnType_INTERVAL
		colDatumType = parser.TypeInterval
	case *parser.StringColType:
		col.Type.Kind = ColumnType_STRING
		col.Type.Width = int32(t.N)
		colDatumType = parser.TypeString
	case *parser.BytesColType:
		col.Type.Kind = ColumnType_BYTES
		colDatumType = parser.TypeBytes
	default:
		return nil, nil, errors.Errorf("unexpected type %T", t)
	}

	if col.Type.Kind == ColumnType_DECIMAL {
		switch {
		case col.Type.Precision == 0 && col.Type.Width > 0:
			// TODO (seif): Find right range for error message.
			return nil, nil, errors.New("invalid NUMERIC precision 0")
		case col.Type.Precision < col.Type.Width:
			return nil, nil, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				col.Type.Width, col.Type.Precision)
		}
	}

	if d.DefaultExpr.Expr != nil {
		// Verify the default expression type is compatible with the column type.
		if err := SanitizeVarFreeExpr(d.DefaultExpr.Expr, colDatumType, "DEFAULT"); err != nil {
			return nil, nil, err
		}
		var p parser.Parser
		if p.AggregateInExpr(d.DefaultExpr.Expr) {
			return nil, nil, fmt.Errorf("Aggregate functions are not allowed in DEFAULT expressions")
		}
		if d.DefaultExpr.ConstraintName != "" {
			col.DefaultExprConstraintName = string(d.DefaultExpr.ConstraintName)
		}
		s := d.DefaultExpr.Expr.String()
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

// MakeIndexKeyPrefix returns the key prefix used for the index's data.
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
// Note that ImplicitColumnIDs are not encoded, so the result isn't always a
// full index key.
func EncodeIndexKey(
	tableDesc *TableDescriptor,
	index *IndexDescriptor,
	colMap map[ColumnID]int,
	values []parser.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	colIDs := index.ColumnIDs
	dirs := directions(index.ColumnDirections)

	if len(index.Interleave.Ancestors) > 0 {
		for i, ancestor := range index.Interleave.Ancestors {
			// The first ancestor is assumed to already be encoded in keyPrefix.
			if i != 0 {
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.TableID))
				key = encoding.EncodeUvarintAscending(key, uint64(ancestor.IndexID))
			}

			length := int(ancestor.SharedPrefixLen)
			var n bool
			key, n, err = EncodeColumns(colIDs[:length], dirs[:length], colMap, values, key)
			if err != nil {
				return key, containsNull, err
			}
			colIDs, dirs = colIDs[length:], dirs[length:]
			containsNull = containsNull || n

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

// EncodeColumns is a version of EncodeIndexKey that takes ColumnIDs and
// directions explicitly.
func EncodeColumns(
	columnIDs []ColumnID,
	directions directions,
	colMap map[ColumnID]int,
	values []parser.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key = make([]byte, len(keyPrefix), 2*len(keyPrefix))
	copy(key, keyPrefix)

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

// MakeKeyFromEncDatums creates a key by concatenating keyPrefix with the
// encodings of the given EncDatum values.
func MakeKeyFromEncDatums(
	values EncDatumRow,
	directions []IndexDescriptor_Direction,
	keyPrefix []byte,
	alloc *DatumAlloc,
) (key roachpb.Key, err error) {
	if len(values) != len(directions) {
		return nil, errors.Errorf("%d values, %d directions", len(values), len(directions))
	}
	// We know we will append to the key which will cause the capacity to grow
	// so make it bigger from the get-go.
	key = make(roachpb.Key, len(keyPrefix), len(keyPrefix)*2)
	copy(key, keyPrefix)

	for i, val := range values {
		encoding := DatumEncoding_ASCENDING_KEY
		if directions[i] == IndexDescriptor_DESC {
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

// EncodeDatum encodes a datum (order-preserving encoding, suitable for keys).
func EncodeDatum(b []byte, d parser.Datum) ([]byte, error) {
	if values, ok := d.(*parser.DTuple); ok {
		return EncodeDTuple(b, *values)
	}
	return EncodeTableKey(b, d, encoding.Ascending)
}

// EncodeDTuple encodes a DTuple (order-preserving).
func EncodeDTuple(b []byte, d parser.DTuple) ([]byte, error) {
	for _, val := range d {
		var err error
		b, err = EncodeDatum(b, val)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// EncodeTableKey encodes `val` into `b` and returns the new buffer.
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

	switch t := val.(type) {
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
			return encoding.EncodeDecimalAscending(b, &t.Dec), nil
		}
		return encoding.EncodeDecimalDescending(b, &t.Dec), nil
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
	case *parser.DTuple:
		for _, datum := range *t {
			var err error
			b, err = EncodeTableKey(b, datum, dir)
			if err != nil {
				return nil, err
			}
		}
		return b, nil
	}
	return nil, errors.Errorf("unable to encode table key: %T", val)
}

// EncodeTableValue encodes `val` into `appendTo` using DatumEncoding_VALUE and
// returns the new buffer.
func EncodeTableValue(appendTo []byte, colID ColumnID, val parser.Datum) ([]byte, error) {
	if val == parser.DNull {
		return encoding.EncodeNullValue(appendTo, uint32(colID)), nil
	}
	switch t := val.(type) {
	case *parser.DBool:
		return encoding.EncodeBoolValue(appendTo, uint32(colID), bool(*t)), nil
	case *parser.DInt:
		return encoding.EncodeIntValue(appendTo, uint32(colID), int64(*t)), nil
	case *parser.DFloat:
		return encoding.EncodeFloatValue(appendTo, uint32(colID), float64(*t)), nil
	case *parser.DDecimal:
		return encoding.EncodeDecimalValue(appendTo, uint32(colID), &t.Dec), nil
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
	}
	return nil, errors.Errorf("unable to encode table value: %T", val)
}

// MakeKeyVals returns a slice of Datums with the correct types for the given
// columns.
func MakeKeyVals(
	desc *TableDescriptor, columnIDs []ColumnID,
) ([]parser.Datum, error) {
	vals := make([]parser.Datum, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}

		if vals[i] = col.Type.ToDatumType(); vals[i] == nil {
			panic(fmt.Sprintf("unsupported column type: %s", col.Type.Kind))
		}
	}
	return vals, nil
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
func DecodeIndexKeyPrefix(a *DatumAlloc, desc *TableDescriptor, key []byte) (
	indexID IndexID, remaining []byte, err error,
) {
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
			remaining = key
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
	indexID IndexID,
	valTypes, vals []parser.Datum,
	colDirs []encoding.Direction,
	key []byte,
) ([]byte, bool, error) {
	var decodedTableID ID
	var decodedIndexID IndexID
	var err error

	if index, err := desc.FindIndexByID(indexID); err == nil && len(index.Interleave.Ancestors) > 0 {
		for _, ancestor := range index.Interleave.Ancestors {
			key, decodedTableID, decodedIndexID, err = DecodeTableIDIndexID(key)
			if err != nil {
				return nil, false, err
			}
			if decodedTableID != ancestor.TableID || decodedIndexID != ancestor.IndexID {
				return nil, false, nil
			}

			length := int(ancestor.SharedPrefixLen)
			key, err = DecodeKeyVals(a, valTypes[:length], vals[:length], colDirs[:length], key)
			valTypes, vals, colDirs = valTypes[length:], vals[length:], colDirs[length:]

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
	if decodedTableID != desc.ID || decodedIndexID != indexID {
		return nil, false, nil
	}

	key, err = DecodeKeyVals(a, valTypes, vals, colDirs, key)
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

// DecodeKeyVals decodes the values that are part of the key. ValTypes is a
// slice returned from makeKeyVals. The decoded values are stored in the vals
// parameter while the valTypes parameter is unmodified. Note that len(vals) >=
// len(valTypes). The types of the decoded values will match the corresponding
// entry in the valTypes parameter with the exception that a value might also
// be parser.DNull. The remaining bytes in the key after decoding the values
// are returned. A slice of directions can be provided to enforce encoding
// direction on each value in valTypes. If this slice is nil, the direction
// used will default to encoding.Ascending.
func DecodeKeyVals(a *DatumAlloc, valTypes, vals []parser.Datum,
	directions []encoding.Direction, key []byte) ([]byte, error) {
	if directions != nil && len(directions) != len(valTypes) {
		return nil, errors.Errorf("encoding directions doesn't parallel valTypes: %d vs %d.",
			len(directions), len(valTypes))
	}
	for j := range valTypes {
		direction := encoding.Ascending
		if directions != nil {
			direction = directions[j]
		}
		var err error
		vals[j], key, err = DecodeTableKey(a, valTypes[j], key, direction)
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
	a *DatumAlloc,
	tableDesc *TableDescriptor,
	entry client.KeyValue,
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
	valueTypes, err := MakeKeyVals(tableDesc, index.ColumnIDs)
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
	extractedValues := make([]parser.Datum, len(index.ColumnIDs))
	if i, err := tableDesc.FindIndexByID(indexID); err == nil && len(i.Interleave.Ancestors) > 0 {
		// TODO(dan): In the interleaved index case, we parse the key twice; once to
		// find the index id so we can look up the descriptor, and once to extract
		// the values. Only parse once.
		var ok bool
		_, ok, err = DecodeIndexKey(a, tableDesc, indexID, valueTypes, extractedValues, dirs, entry.Key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.Errorf("descriptor did not match key")
		}
	} else {
		key, err = DecodeKeyVals(a, valueTypes, extractedValues, dirs, key)
		if err != nil {
			return nil, err
		}
	}

	// Extract the values for index.ImplicitColumnIDs
	valueTypes, err = MakeKeyVals(tableDesc, index.ImplicitColumnIDs)
	if err != nil {
		return nil, err
	}
	dirs = make([]encoding.Direction, len(index.ImplicitColumnIDs))
	for i := range index.ImplicitColumnIDs {
		// Implicit columns are always encoded Ascending.
		dirs[i] = encoding.Ascending
	}
	extractedImplicitValues := make([]parser.Datum, len(index.ImplicitColumnIDs))
	implicitKey := key
	if index.Unique {
		implicitKey, err = entry.Value.GetBytes()
		if err != nil {
			return nil, err
		}
	}
	_, err = DecodeKeyVals(a, valueTypes, extractedImplicitValues, dirs, implicitKey)
	if err != nil {
		return nil, err
	}

	// Encode the index key from its components.
	extractedValues = append(extractedValues, extractedImplicitValues...)
	colMap := make(map[ColumnID]int)
	for i, columnID := range index.ColumnIDs {
		colMap[columnID] = i
	}
	for i, columnID := range index.ImplicitColumnIDs {
		colMap[columnID] = i + len(index.ColumnIDs)
	}
	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	indexKey, _, err := EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colMap, extractedValues, indexKeyPrefix)
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

// DecodeTableKey decodes a table key/value.
func DecodeTableKey(
	a *DatumAlloc, valType parser.Datum, key []byte, dir encoding.Direction,
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
	switch valType.(type) {
	case *parser.DBool:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		// No need to chunk allocate DBool as MakeDBool returns either
		// parser.DBoolTrue or parser.DBoolFalse.
		return parser.MakeDBool(parser.DBool(i != 0)), rkey, err
	case *parser.DInt:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDInt(parser.DInt(i)), rkey, err
	case *parser.DFloat:
		var f float64
		if dir == encoding.Ascending {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		return a.NewDFloat(parser.DFloat(f)), rkey, err
	case *parser.DDecimal:
		var d *inf.Dec
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		dd := a.NewDDecimal(parser.DDecimal{})
		dd.Set(d)
		return dd, rkey, err
	case *parser.DString:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeUnsafeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeUnsafeStringDescending(key, nil)
		}
		return a.NewDString(parser.DString(r)), rkey, err
	case *parser.DBytes:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return a.NewDBytes(parser.DBytes(r)), rkey, err
	case *parser.DDate:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return a.NewDDate(parser.DDate(t)), rkey, err
	case *parser.DTimestamp:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestamp(parser.DTimestamp{Time: t}), rkey, err
	case *parser.DTimestampTZ:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return a.NewDTimestampTZ(parser.DTimestampTZ{Time: t}), rkey, err
	case *parser.DInterval:
		var d duration.Duration
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		return a.NewDInterval(parser.DInterval{Duration: d}), rkey, err
	default:
		return nil, nil, errors.Errorf("TODO(pmattis): decoded index key: %s", valType.Type())
	}
}

// DecodeTableValue decodes a value encoded by EncodeTableValue.
func DecodeTableValue(a *DatumAlloc, valType parser.Datum, b []byte) (parser.Datum, []byte, error) {
	_, dataOffset, _, typ, err := encoding.DecodeValueTag(b)
	if err != nil {
		return nil, b, err
	}
	if typ == encoding.Null {
		return parser.DNull, b[dataOffset:], nil
	}
	switch valType.(type) {
	case *parser.DBool:
		var x bool
		b, x, err = encoding.DecodeBoolValue(b)
		// No need to chunk allocate DBool as MakeDBool returns either
		// parser.DBoolTrue or parser.DBoolFalse.
		return parser.MakeDBool(parser.DBool(x)), b, err
	case *parser.DInt:
		var i int64
		b, i, err = encoding.DecodeIntValue(b)
		return a.NewDInt(parser.DInt(i)), b, err
	case *parser.DFloat:
		var f float64
		b, f, err = encoding.DecodeFloatValue(b)
		return a.NewDFloat(parser.DFloat(f)), b, err
	case *parser.DDecimal:
		var d *inf.Dec
		b, d, err = encoding.DecodeDecimalValue(b)
		dd := a.NewDDecimal(parser.DDecimal{})
		dd.Set(d)
		return dd, b, err
	case *parser.DString:
		var data []byte
		b, data, err = encoding.DecodeBytesValue(b)
		return a.NewDString(parser.DString(data)), b, err
	case *parser.DBytes:
		var data []byte
		b, data, err = encoding.DecodeBytesValue(b)
		return a.NewDBytes(parser.DBytes(data)), b, err
	case *parser.DDate:
		var i int64
		b, i, err = encoding.DecodeIntValue(b)
		return a.NewDDate(parser.DDate(i)), b, err
	case *parser.DTimestamp:
		var t time.Time
		b, t, err = encoding.DecodeTimeValue(b)
		return a.NewDTimestamp(parser.DTimestamp{Time: t}), b, err
	case *parser.DTimestampTZ:
		var t time.Time
		b, t, err = encoding.DecodeTimeValue(b)
		return a.NewDTimestampTZ(parser.DTimestampTZ{Time: t}), b, err
	case *parser.DInterval:
		var d duration.Duration
		b, d, err = encoding.DecodeDurationValue(b)
		return a.NewDInterval(parser.DInterval{Duration: d}), b, err
	default:
		return nil, nil, errors.Errorf("TODO(pmattis): decoded index value: %s", valType.Type())
	}
}

// IndexEntry represents an encoded key/value for an index entry.
type IndexEntry struct {
	Key   roachpb.Key
	Value roachpb.Value
}

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

	// Add the implicit columns - they are encoded ascendingly which is done by
	// passing nil for the encoding directions.
	extraKey, _, err := EncodeColumns(secondaryIndex.ImplicitColumnIDs, nil,
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
	entry.Key = keys.MakeRowSentinelKey(entry.Key)

	if secondaryIndex.Unique {
		// Note that a unique secondary index that contains a NULL column value
		// will have extraKey appended to the key and stored in the value. We
		// require extraKey to be appended to the key in order to make the key
		// unique. We could potentially get rid of the duplication here but at
		// the expense of complicating scanNode when dealing with unique
		// secondary indexes.
		entry.Value.SetBytes(extraKey)
	} else {
		// The zero value for an index-key is a 0-length bytes value.
		entry.Value.SetBytes([]byte{})
	}

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
func CheckColumnType(col ColumnDescriptor, val parser.Datum, pmap *parser.PlaceholderInfo) error {
	if val == parser.DNull {
		return nil
	}

	var ok bool
	var set parser.Datum
	switch col.Type.Kind {
	case ColumnType_BOOL:
		_, ok = val.(*parser.DBool)
		set = parser.TypeBool
	case ColumnType_INT:
		_, ok = val.(*parser.DInt)
		set = parser.TypeInt
	case ColumnType_FLOAT:
		_, ok = val.(*parser.DFloat)
		set = parser.TypeFloat
	case ColumnType_DECIMAL:
		_, ok = val.(*parser.DDecimal)
		set = parser.TypeDecimal
	case ColumnType_STRING:
		_, ok = val.(*parser.DString)
		set = parser.TypeString
	case ColumnType_BYTES:
		_, ok = val.(*parser.DBytes)
		if !ok {
			_, ok = val.(*parser.DString)
		}
		set = parser.TypeBytes
	case ColumnType_DATE:
		_, ok = val.(*parser.DDate)
		set = parser.TypeDate
	case ColumnType_TIMESTAMP:
		_, ok = val.(*parser.DTimestamp)
		set = parser.TypeTimestamp
	case ColumnType_TIMESTAMPTZ:
		_, ok = val.(*parser.DTimestampTZ)
		set = parser.TypeTimestampTZ
	case ColumnType_INTERVAL:
		_, ok = val.(*parser.DInterval)
		set = parser.TypeInterval
	default:
		return errors.Errorf("unsupported column type: %s", col.Type.Kind)
	}

	// If the value is a placeholder, then the column check above has
	// populated 'set' with a type to assign to it.
	if d, dok := val.(*parser.DPlaceholder); dok {
		if err := pmap.SetType(d.Name(), set); err != nil {
			return fmt.Errorf("cannot infer type for placeholder %s from column %q: %s",
				d.Name(), col.Name, err)
		}
	} else {
		// Not a placeholder; check that the value cast has succeeded.
		if !ok && set == nil {
			return fmt.Errorf("value type %s doesn't match type %s of column %q",
				val.Type(), col.Type.Kind, col.Name)
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

	switch col.Type.Kind {
	case ColumnType_BOOL:
		if v, ok := val.(*parser.DBool); ok {
			r.SetBool(bool(*v))
			return r, nil
		}
	case ColumnType_INT:
		if v, ok := val.(*parser.DInt); ok {
			r.SetInt(int64(*v))
			return r, nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(*parser.DFloat); ok {
			r.SetFloat(float64(*v))
			return r, nil
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*parser.DDecimal); ok {
			err := r.SetDecimal(&v.Dec)
			return r, err
		}
	case ColumnType_STRING:
		if v, ok := val.(*parser.DString); ok {
			r.SetString(string(*v))
			return r, nil
		}
	case ColumnType_BYTES:
		if v, ok := val.(*parser.DBytes); ok {
			r.SetString(string(*v))
			return r, nil
		}
		if v, ok := val.(*parser.DString); ok {
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
	default:
		return r, errors.Errorf("unsupported column type: %s", col.Type.Kind)
	}
	return r, fmt.Errorf("value type %s doesn't match type %s of column %q",
		val.Type(), col.Type.Kind, col.Name)
}

// UnmarshalColumnValue decodes the value from a key-value pair using the type
// expected by the column. An error is returned if the value's type does not
// match the column's type.
func UnmarshalColumnValue(
	a *DatumAlloc, kind ColumnType_Kind, value *roachpb.Value,
) (parser.Datum, error) {
	if value == nil {
		return parser.DNull, nil
	}

	switch kind {
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
		dd := a.NewDDecimal(parser.DDecimal{})
		dd.Set(v)
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
	default:
		return nil, errors.Errorf("unsupported column type: %s", kind)
	}
}

// CheckValueWidth checks that the width (for strings, byte arrays, and
// bit string) and scale (for decimals) of the value fits the specified
// column type. Used by INSERT and UPDATE.
func CheckValueWidth(col ColumnDescriptor, val parser.Datum) error {
	switch col.Type.Kind {
	case ColumnType_STRING:
		if v, ok := val.(*parser.DString); ok {
			if col.Type.Width > 0 && utf8.RuneCountInString(string(*v)) > int(col.Type.Width) {
				return fmt.Errorf("value too long for type %s (column %q)",
					col.Type.SQLString(), col.Name)
			}
		}
	case ColumnType_INT:
		if v, ok := val.(*parser.DInt); ok {
			if col.Type.Width > 0 {
				// https://www.postgresql.org/docs/9.5/static/datatype-bit.html
				// "bit type data must match the length n exactly; it is an error
				// to attempt to store shorter or longer bit strings. bit varying
				// data is of variable length up to the maximum length n; longer
				// strings will be rejected."
				//
				// TODO(nvanbenschoten) Because we do not propagate the "varying"
				// flag on the column type, the best we can do here is conservatively
				// assume the varying bit type and error only on longer bit strings.
				mostSignificantBit := int32(0)
				for bits := uint64(*v); bits != 0; mostSignificantBit++ {
					bits >>= 1
				}
				if mostSignificantBit > col.Type.Width {
					return fmt.Errorf("bit string too long for type %s (column %q)",
						col.Type.SQLString(), col.Name)
				}
			}
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*parser.DDecimal); ok {
			if col.Type.Precision > 0 {
				// http://www.postgresql.org/docs/9.5/static/datatype-numeric.html
				// "If the scale of a value to be stored is greater than
				// the declared scale of the column, the system will round the
				// value to the specified number of fractional digits. Then,
				// if the number of digits to the left of the decimal point
				// exceeds the declared precision minus the declared scale, an
				// error is raised."

				if col.Type.Width > 0 {
					// Rounding half up, as per round_var() in PostgreSQL 9.5.
					v.Dec.Round(&v.Dec, inf.Scale(col.Type.Width), inf.RoundHalfUp)
				}

				// Check that the precision is not exceeded.
				maxDigitsLeft := decimal.PowerOfTenDec(int(col.Type.Precision - col.Type.Width))

				absRounded := &v.Dec
				if absRounded.Sign() == -1 {
					// Only force the allocation on negative decimals.
					absRounded = new(inf.Dec).Neg(&v.Dec)
				}
				if absRounded.Cmp(maxDigitsLeft) != -1 {
					return fmt.Errorf("too many digits for type %s (column %q)",
						col.Type.SQLString(), col.Name)
				}
			}
		}
	}
	return nil
}
