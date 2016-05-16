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
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// MakeTableDesc creates a table descriptor from a CreateTable statement.
func MakeTableDesc(p *parser.CreateTable, parentID ID) (TableDescriptor, error) {
	desc := TableDescriptor{}
	if err := p.Table.NormalizeTableName(""); err != nil {
		return desc, err
	}
	desc.Name = p.Table.Table()
	desc.ParentID = parentID
	desc.FormatVersion = BaseFormatVersion
	// We don't use version 0.
	desc.Version = 1

	var primaryIndexColumnSet map[parser.Name]struct{}
	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			col, idx, err := MakeColumnDefDescs(d)
			if err != nil {
				return desc, err
			}
			desc.AddColumn(*col)
			if idx != nil {
				if err := desc.AddIndex(*idx, d.PrimaryKey); err != nil {
					return desc, err
				}
			}
		case *parser.IndexTableDef:
			idx := IndexDescriptor{
				Name:             string(d.Name),
				StoreColumnNames: d.Storing,
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
		case *parser.UniqueConstraintTableDef:
			idx := IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				StoreColumnNames: d.Storing,
			}
			if err := idx.FillColumns(d.Columns); err != nil {
				return desc, err
			}
			if err := desc.AddIndex(idx, d.PrimaryKey); err != nil {
				return desc, err
			}
			if d.PrimaryKey {
				primaryIndexColumnSet = make(map[parser.Name]struct{})
				for _, c := range d.Columns {
					primaryIndexColumnSet[c.Column] = struct{}{}
				}
			}
		case *parser.CheckConstraintTableDef:
			// CHECK expressions seem to vary across databases. Wikipedia's entry on
			// Check_constraint (https://en.wikipedia.org/wiki/Check_constraint) says
			// that if the constraint refers to a single column only, it is possible to
			// specify the constraint as part of the column definition. Postgres allows
			// specifying them anywhere about any columns, but it moves all constraints to
			// the table level (i.e., columns never have a check constraint themselves). We
			// will adhere to the stricter definition.

			preFn := func(expr parser.Expr) (err error, recurse bool, newExpr parser.Expr) {
				qname, ok := expr.(*parser.QualifiedName)
				if !ok {
					// Not a qname, don't do anything to this node.
					return nil, true, expr
				}

				if err := qname.NormalizeColumnName(); err != nil {
					return err, false, nil
				}

				if qname.IsStar() {
					return fmt.Errorf("* not allowed in constraint %q", d.Expr.String()), false, nil
				}
				col, err := desc.FindActiveColumnByName(qname.Column())
				if err != nil {
					return fmt.Errorf("column %q not found for constraint %q", qname.String(), d.Expr.String()), false, nil
				}
				// Convert to a dummy datum of the correct type.
				return nil, false, col.Type.ToDatumType()
			}

			expr, err := parser.SimpleVisit(d.Expr, preFn)
			if err != nil {
				return desc, err
			}
			if typedExpr, err := expr.TypeCheck(nil, parser.TypeBool); err != nil {
				return desc, err
			} else if typ := typedExpr.ReturnType(); !typ.TypeEqual(parser.TypeBool) {
				return desc, fmt.Errorf("argument of CHECK must be type bool, not type %s", typ.Type())
			}
			check := &TableDescriptor_CheckConstraint{Expr: d.Expr.String()}
			if len(d.Name) > 0 {
				check.Name = string(d.Name)
			}
			desc.Checks = append(desc.Checks, check)

		default:
			return desc, util.Errorf("unsupported table def: %T", def)
		}
	}

	if primaryIndexColumnSet != nil {
		// Primary index columns are not nullable.
		for i := range desc.Columns {
			if _, ok := primaryIndexColumnSet[parser.Name(desc.Columns[i].Name)]; ok {
				desc.Columns[i].Nullable = false
			}
		}
	}

	return desc, nil
}

func defaultContainsPlaceholdersError(typedExpr parser.TypedExpr) error {
	return fmt.Errorf("default expression '%s' may not contain placeholders", typedExpr)
}

func incompatibleColumnDefaultTypeError(colDatumType parser.Datum, defaultType parser.Datum) error {
	return fmt.Errorf("incompatible column type and default expression: %s vs %s",
		colDatumType.Type(), defaultType.Type())
}

// SanitizeDefaultExpr verifies a default expression is valid and has the
// correct type.
func SanitizeDefaultExpr(expr parser.Expr, colDatumType parser.Datum) error {
	typedExpr, err := parser.TypeCheck(expr, nil, colDatumType)
	if err != nil {
		return err
	}
	if defaultType := typedExpr.ReturnType(); !colDatumType.TypeEqual(defaultType) {
		return incompatibleColumnDefaultTypeError(colDatumType, defaultType)
	}
	if parser.ContainsVars(typedExpr) {
		return defaultContainsPlaceholdersError(typedExpr)
	}
	return nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
func MakeColumnDefDescs(d *parser.ColumnTableDef) (*ColumnDescriptor, *IndexDescriptor, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable != parser.NotNull && !d.PrimaryKey,
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
		return nil, nil, util.Errorf("unexpected type %T", t)
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

	if d.DefaultExpr != nil {
		// Verify the default expression type is compatible with the column type.
		if err := SanitizeDefaultExpr(d.DefaultExpr, colDatumType); err != nil {
			return nil, nil, err
		}
		s := d.DefaultExpr.String()
		col.DefaultExpr = &s
	}

	var idx *IndexDescriptor
	if d.PrimaryKey || d.Unique {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
	}

	return col, idx, nil
}

// EncodeIndexKey doesn't deal with ImplicitColumnIDs, so it doesn't always produce
// a full index key.
func EncodeIndexKey(index *IndexDescriptor, colMap map[ColumnID]int,
	values []parser.Datum, indexKey []byte) ([]byte, bool, error) {
	dirs := make([]encoding.Direction, 0, len(index.ColumnIDs))
	for _, dir := range index.ColumnDirections {
		convertedDir, err := dir.ToEncodingDirection()
		if err != nil {
			return nil, false, err
		}
		dirs = append(dirs, convertedDir)
	}
	return EncodeColumns(index.ColumnIDs, dirs, colMap, values, indexKey)
}

// EncodeColumns is a version of EncodeIndexKey that takes ColumnIDs and
// directions explicitly.
func EncodeColumns(
	columnIDs []ColumnID,
	directions []encoding.Direction,
	colMap map[ColumnID]int,
	values []parser.Datum,
	indexKey []byte,
) ([]byte, bool, error) {
	var key []byte
	var containsNull bool
	key = append(key, indexKey...)

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

		var err error
		if key, err = EncodeTableKey(key, val, directions[colIdx]); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
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
		return nil, util.Errorf("invalid direction: %d", dir)
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
	return nil, util.Errorf("unable to encode table key: %T", val)
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

// DecodeIndexKeyPrefix decodes the prefix of an index key and returns the
// index id and a slice for the rest of the key.
func DecodeIndexKeyPrefix(desc *TableDescriptor, key []byte) (
	IndexID, []byte, error,
) {
	if encoding.PeekType(key) != encoding.Int {
		return 0, nil, util.Errorf("%s: invalid key prefix: %q", desc.Name, key)
	}

	key, tableID, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return 0, nil, err
	}
	key, indexID, err := encoding.DecodeUvarintAscending(key)
	if err != nil {
		return 0, nil, err
	}

	if ID(tableID) != desc.ID {
		return IndexID(indexID), nil,
			util.Errorf("%s: unexpected table ID: %d != %d", desc.Name, desc.ID, tableID)
	}

	return IndexID(indexID), key, nil
}

// DecodeIndexKey decodes the values that are a part of the specified index
// key. ValTypes is a slice returned from makeKeyVals. The remaining bytes in the
// index key are returned which will either be an encoded column ID for the
// primary key index, the primary key suffix for non-unique secondary indexes
// or unique secondary indexes containing NULL or empty.
func DecodeIndexKey(
	a *DatumAlloc,
	desc *TableDescriptor,
	indexID IndexID,
	valTypes, vals []parser.Datum,
	colDirs []encoding.Direction,
	key []byte,
) ([]byte, error) {
	decodedIndexID, remaining, err := DecodeIndexKeyPrefix(desc, key)
	if err != nil {
		return nil, err
	}

	if decodedIndexID != indexID {
		return nil, util.Errorf("%s: unexpected index ID: %d != %d", desc.Name, indexID, decodedIndexID)
	}
	return DecodeKeyVals(a, valTypes, vals, colDirs, remaining)
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
		return nil, util.Errorf("encoding directions doesn't parallel valTypes: %d vs %d.",
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
func ExtractIndexKey(
	a *DatumAlloc,
	tableDesc *TableDescriptor,
	entry client.KeyValue,
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
	key, err = DecodeKeyVals(a, valueTypes, extractedValues, dirs, key)
	if err != nil {
		return nil, err
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
	indexKeyPrefix := MakeIndexKeyPrefix(tableDesc.ID, tableDesc.PrimaryIndex.ID)
	indexKey, _, err := EncodeIndexKey(&tableDesc.PrimaryIndex, colMap, extractedValues, indexKeyPrefix)
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
		return nil, nil, util.Errorf("invalid direction: %d", dir)
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
		return nil, nil, util.Errorf("TODO(pmattis): decoded index key: %s", valType.Type())
	}
}

// IndexEntry represents an encoded key/value for an index entry.
type IndexEntry struct {
	Key   roachpb.Key
	Value []byte
}

// EncodeSecondaryIndex encodes key/values for a secondary index. colMap maps
// ColumnIDs to indices in `values`.
func EncodeSecondaryIndex(
	tableID ID,
	secondaryIndex IndexDescriptor,
	colMap map[ColumnID]int,
	values []parser.Datum,
) (IndexEntry, error) {
	secondaryIndexKeyPrefix := MakeIndexKeyPrefix(tableID, secondaryIndex.ID)
	secondaryIndexKey, containsNull, err := EncodeIndexKey(
		&secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
	if err != nil {
		return IndexEntry{}, err
	}

	// Add the implicit columns - they are encoded ascendingly.
	implicitDirs := make([]encoding.Direction, 0, len(secondaryIndex.ImplicitColumnIDs))
	for range secondaryIndex.ImplicitColumnIDs {
		implicitDirs = append(implicitDirs, encoding.Ascending)
	}
	extraKey, _, err := EncodeColumns(secondaryIndex.ImplicitColumnIDs, implicitDirs,
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
	entry.Key = keys.MakeNonColumnKey(entry.Key)

	if secondaryIndex.Unique {
		// Note that a unique secondary index that contains a NULL column value
		// will have extraKey appended to the key and stored in the value. We
		// require extraKey to be appended to the key in order to make the key
		// unique. We could potentially get rid of the duplication here but at
		// the expense of complicating scanNode when dealing with unique
		// secondary indexes.
		entry.Value = extraKey
	}

	return entry, nil
}

// EncodeSecondaryIndexes encodes key/values for the secondary indexes. colMap
// maps ColumnIDs to indices in `values`.
func EncodeSecondaryIndexes(
	tableID ID,
	indexes []IndexDescriptor,
	colMap map[ColumnID]int,
	values []parser.Datum,
) ([]IndexEntry, error) {
	var secondaryIndexEntries []IndexEntry
	for _, secondaryIndex := range indexes {
		entry, err := EncodeSecondaryIndex(tableID, secondaryIndex, colMap, values)
		if err != nil {
			return nil, err
		}
		secondaryIndexEntries = append(secondaryIndexEntries, entry)
	}
	return secondaryIndexEntries, nil
}

// CheckColumnType verifies that a given value is compatible
// with the type requested by the column. If the value is a
// placeholder, the type of the placeholder gets populated.
func CheckColumnType(col ColumnDescriptor, val parser.Datum, args parser.MapArgs) error {
	if val == parser.DNull {
		return nil
	}

	var ok bool
	var err error
	var set parser.Datum
	switch col.Type.Kind {
	case ColumnType_BOOL:
		_, ok = val.(*parser.DBool)
		set, err = args.SetInferredType(val, parser.TypeBool)
	case ColumnType_INT:
		_, ok = val.(*parser.DInt)
		set, err = args.SetInferredType(val, parser.TypeInt)
	case ColumnType_FLOAT:
		_, ok = val.(*parser.DFloat)
		set, err = args.SetInferredType(val, parser.TypeFloat)
	case ColumnType_DECIMAL:
		_, ok = val.(*parser.DDecimal)
		set, err = args.SetInferredType(val, parser.TypeDecimal)
	case ColumnType_STRING:
		_, ok = val.(*parser.DString)
		set, err = args.SetInferredType(val, parser.TypeString)
	case ColumnType_BYTES:
		_, ok = val.(*parser.DBytes)
		if !ok {
			_, ok = val.(*parser.DString)
		}
		set, err = args.SetInferredType(val, parser.TypeBytes)
	case ColumnType_DATE:
		_, ok = val.(*parser.DDate)
		set, err = args.SetInferredType(val, parser.TypeDate)
	case ColumnType_TIMESTAMP:
		_, ok = val.(*parser.DTimestamp)
		set, err = args.SetInferredType(val, parser.TypeTimestamp)
	case ColumnType_TIMESTAMPTZ:
		_, ok = val.(*parser.DTimestampTZ)
		set, err = args.SetInferredType(val, parser.TypeTimestampTZ)
	case ColumnType_INTERVAL:
		_, ok = val.(*parser.DInterval)
		set, err = args.SetInferredType(val, parser.TypeInterval)
	default:
		return util.Errorf("unsupported column type: %s", col.Type.Kind)
	}
	// Check that the value cast has succeeded.
	// We ignore the case where it has failed because val was a DArg,
	// which is signalled by SetInferredType returning a non-nil assignment.
	if !ok && set == nil {
		return fmt.Errorf("value type %s doesn't match type %s of column %q",
			val.Type(), col.Type.Kind, col.Name)
	}
	return err
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
			if *v {
				r.SetInt(1)
			} else {
				r.SetInt(0)
			}
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
		return r, util.Errorf("unsupported column type: %s", col.Type.Kind)
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
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return parser.MakeDBool(parser.DBool(v != 0)), nil
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
		return nil, util.Errorf("unsupported column type: %s", kind)
	}
}

// CheckValueWidth checks that the width (for strings/byte arrays) and
// scale (for decimals) of the value fits the specified column type.
// Used by INSERT and UPDATE.
func CheckValueWidth(col ColumnDescriptor, val parser.Datum) error {
	switch col.Type.Kind {
	case ColumnType_STRING:
		if v, ok := val.(*parser.DString); ok {
			if col.Type.Width > 0 && utf8.RuneCountInString(string(*v)) > int(col.Type.Width) {
				return fmt.Errorf("value too long for type %s (column %q)",
					col.Type.SQLString(), col.Name)
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
