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

package sql

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"time"
	"unicode/utf8"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/encoding"
)

var testDisableTableLeases bool

// TestDisableTableLeases disables table leases and returns
// a function that can be used to enable it.
func TestDisableTableLeases() func() {
	testDisableTableLeases = true
	return func() {
		testDisableTableLeases = false
	}
}

// tableKey implements descriptorKey.
type tableKey struct {
	parentID ID
	name     string
}

func (tk tableKey) Key() roachpb.Key {
	return MakeNameMetadataKey(tk.parentID, tk.name)
}

func (tk tableKey) Name() string {
	return tk.name
}

func makeTableDesc(p *parser.CreateTable, parentID ID) (TableDescriptor, error) {
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
			col, idx, err := makeColumnDefDescs(d)
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
			if err := idx.fillColumns(d.Columns); err != nil {
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
			if err := idx.fillColumns(d.Columns); err != nil {
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

func makeColumnDefDescs(d *parser.ColumnTableDef) (*ColumnDescriptor, *IndexDescriptor, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable != parser.NotNull && !d.PrimaryKey,
	}

	var colDatumType parser.Datum
	switch t := d.Type.(type) {
	case *parser.BoolType:
		col.Type.Kind = ColumnType_BOOL
		colDatumType = parser.DummyBool
	case *parser.IntType:
		col.Type.Kind = ColumnType_INT
		col.Type.Width = int32(t.N)
		colDatumType = parser.DummyInt
	case *parser.FloatType:
		col.Type.Kind = ColumnType_FLOAT
		col.Type.Precision = int32(t.Prec)
		colDatumType = parser.DummyFloat
	case *parser.DecimalType:
		col.Type.Kind = ColumnType_DECIMAL
		col.Type.Width = int32(t.Scale)
		col.Type.Precision = int32(t.Prec)
		colDatumType = parser.DummyDecimal
	case *parser.DateType:
		col.Type.Kind = ColumnType_DATE
		colDatumType = parser.DummyDate
	case *parser.TimestampType:
		col.Type.Kind = ColumnType_TIMESTAMP
		colDatumType = parser.DummyTimestamp
	case *parser.IntervalType:
		col.Type.Kind = ColumnType_INTERVAL
		colDatumType = parser.DummyInterval
	case *parser.StringType:
		col.Type.Kind = ColumnType_STRING
		col.Type.Width = int32(t.N)
		colDatumType = parser.DummyString
	case *parser.BytesType:
		col.Type.Kind = ColumnType_BYTES
		colDatumType = parser.DummyBytes
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
		defaultType, err := d.DefaultExpr.TypeCheck(nil)
		if err != nil {
			return nil, nil, err
		}
		if colDatumType != defaultType {
			return nil, nil, fmt.Errorf("incompatible column type and default expression: %s vs %s",
				col.Type.Kind, defaultType.Type())
		}

		s := d.DefaultExpr.String()
		col.DefaultExpr = &s
	}

	if d.CheckExpr != nil {
		// TODO(guanqun): add more checks here.
		s := d.CheckExpr.String()
		col.CheckExpr = &s
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

func (p *planner) getTableDesc(qname *parser.QualifiedName) (TableDescriptor, *roachpb.Error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return TableDescriptor{}, roachpb.NewError(err)
	}
	dbDesc, pErr := p.getDatabaseDesc(qname.Database())
	if pErr != nil {
		return TableDescriptor{}, pErr
	}

	desc := TableDescriptor{}
	if pErr := p.getDescriptor(tableKey{dbDesc.ID, qname.Table()}, &desc); pErr != nil {
		return TableDescriptor{}, pErr
	}
	return desc, nil
}

// get the table descriptor for the ID passed in using the planner's txn.
func getTableDescFromID(txn *client.Txn, id ID) (*TableDescriptor, *roachpb.Error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if pErr := txn.GetProto(descKey, desc); pErr != nil {
		return nil, pErr
	}
	tableDesc := desc.GetTable()
	if tableDesc == nil {
		return nil, roachpb.NewErrorf("ID %d is not a table", id)
	}
	return tableDesc, nil
}

// getTableLease acquires a lease for the specified table. The lease will be
// released when the planner closes. Note that a shallow copy of the table
// descriptor is returned. It is safe to mutate fields of the returned
// descriptor, but the values those fields point to should not be modified.
func (p *planner) getTableLease(qname *parser.QualifiedName) (TableDescriptor, *roachpb.Error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return TableDescriptor{}, roachpb.NewError(err)
	}

	if qname.Database() == systemDB.Name || testDisableTableLeases {
		// We don't go through the normal lease mechanism for system tables. The
		// system.lease and system.descriptor table, in particular, are problematic
		// because they are used for acquiring leases itself, creating a
		// chicken&egg problem.
		return p.getTableDesc(qname)
	}

	tableID, pErr := p.getTableID(qname)
	if pErr != nil {
		return TableDescriptor{}, pErr
	}

	var lease *LeaseState
	found := false
	for _, lease = range p.leases {
		if lease.TableDescriptor.ID == tableID {
			found = true
			break
		}
	}
	if !found {
		var pErr *roachpb.Error
		lease, pErr = p.leaseMgr.Acquire(p.txn, tableID, 0)
		if pErr != nil {
			return TableDescriptor{}, pErr
		}
		p.leases = append(p.leases, lease)
	}

	return lease.TableDescriptor, nil
}

// getTableID retrieves the table ID for the specified table. It uses the
// descriptor cache to perform lookups, falling back to the KV store when
// necessary.
func (p *planner) getTableID(qname *parser.QualifiedName) (ID, *roachpb.Error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return 0, roachpb.NewError(err)
	}

	dbID, pErr := p.getDatabaseID(qname.Database())
	if pErr != nil {
		return 0, pErr
	}

	// Lookup the ID of the table in the cache. The use of the cache might cause
	// the usage of a recently renamed table, but that's a race that could occur
	// anyways.
	nameKey := tableKey{dbID, qname.Table()}
	key := nameKey.Key()
	if nameVal := p.systemConfig.GetValue(key); nameVal != nil {
		id, err := nameVal.GetInt()
		return ID(id), roachpb.NewError(err)
	}

	gr, pErr := p.txn.Get(key)
	if pErr != nil {
		return 0, pErr
	}
	if !gr.Exists() {
		return 0, roachpb.NewErrorf("table %q does not exist", nameKey.Name())
	}
	return ID(gr.ValueInt()), nil
}

func (p *planner) getTableNames(dbDesc *DatabaseDescriptor) (parser.QualifiedNames, *roachpb.Error) {
	prefix := MakeNameMetadataKey(dbDesc.ID, "")
	sr, pErr := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if pErr != nil {
		return nil, pErr
	}

	var qualifiedNames parser.QualifiedNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeStringAscending(
			bytes.TrimPrefix(row.Key, prefix), nil)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		qname := &parser.QualifiedName{
			Base:     parser.Name(dbDesc.Name),
			Indirect: parser.Indirection{parser.NameIndirection(tableName)},
		}
		if err := qname.NormalizeTableName(""); err != nil {
			return nil, roachpb.NewError(err)
		}
		qualifiedNames = append(qualifiedNames, qname)
	}
	return qualifiedNames, nil
}

// encodeIndexKey doesn't deal with ImplicitColumnIDs, so it doesn't always produce
// a full index key.
func encodeIndexKey(index *IndexDescriptor, colMap map[ColumnID]int,
	values []parser.Datum, indexKey []byte) ([]byte, bool, error) {
	dirs := make([]encoding.Direction, 0, len(index.ColumnIDs))
	for _, dir := range index.ColumnDirections {
		convertedDir, err := dir.toEncodingDirection()
		if err != nil {
			return nil, false, err
		}
		dirs = append(dirs, convertedDir)
	}
	return encodeColumns(index.ColumnIDs, dirs, colMap, values, indexKey)
}

// Version of encodeIndexKey that takes ColumnIDs and directions explicitly.
func encodeColumns(columnIDs []ColumnID, directions []encoding.Direction, colMap map[ColumnID]int,
	values []parser.Datum, indexKey []byte) ([]byte, bool, error) {
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
		if key, err = encodeTableKey(key, val, directions[colIdx]); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

func encodeDatum(b []byte, d parser.Datum) ([]byte, error) {
	if values, ok := d.(parser.DTuple); ok {
		return encodeDTuple(b, values)
	}
	return encodeTableKey(b, d, encoding.Ascending)
}

func encodeDTuple(b []byte, d parser.DTuple) ([]byte, error) {
	for _, val := range d {
		var err error
		b, err = encodeDatum(b, val)
		if err != nil {
			return nil, err
		}
	}
	return b, nil
}

// Encodes `val` into `b` and returns the new buffer.
func encodeTableKey(b []byte, val parser.Datum, dir encoding.Direction) ([]byte, error) {
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
	case parser.DBool:
		var x int64
		if t {
			x = 1
		} else {
			x = 0
		}
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, x), nil
		}
		return encoding.EncodeVarintDescending(b, x), nil
	case parser.DInt:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(t)), nil
	case parser.DFloat:
		if dir == encoding.Ascending {
			return encoding.EncodeFloatAscending(b, float64(t)), nil
		}
		return encoding.EncodeFloatDescending(b, float64(t)), nil
	case *parser.DDecimal:
		if dir == encoding.Ascending {
			return encoding.EncodeDecimalAscending(b, &t.Dec), nil
		}
		return encoding.EncodeDecimalDescending(b, &t.Dec), nil
	case parser.DString:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(t)), nil
		}
		return encoding.EncodeStringDescending(b, string(t)), nil
	case parser.DBytes:
		if dir == encoding.Ascending {
			return encoding.EncodeStringAscending(b, string(t)), nil
		}
		return encoding.EncodeStringDescending(b, string(t)), nil
	case parser.DDate:
		if dir == encoding.Ascending {
			return encoding.EncodeVarintAscending(b, int64(t)), nil
		}
		return encoding.EncodeVarintDescending(b, int64(t)), nil
	case parser.DTimestamp:
		if dir == encoding.Ascending {
			return encoding.EncodeTimeAscending(b, t.Time), nil
		}
		return encoding.EncodeTimeDescending(b, t.Time), nil
	case parser.DInterval:
		if dir == encoding.Ascending {
			return encoding.EncodeDurationAscending(b, t.Duration)
		}
		return encoding.EncodeDurationDescending(b, t.Duration)
	}
	return nil, util.Errorf("unable to encode table key: %T", val)
}

func makeKeyVals(desc *TableDescriptor, columnIDs []ColumnID) ([]parser.Datum, error) {
	vals := make([]parser.Datum, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		switch col.Type.Kind {
		case ColumnType_BOOL:
			vals[i] = parser.DummyBool
		case ColumnType_INT:
			vals[i] = parser.DummyInt
		case ColumnType_FLOAT:
			vals[i] = parser.DummyFloat
		case ColumnType_DECIMAL:
			vals[i] = parser.DummyDecimal
		case ColumnType_STRING:
			vals[i] = parser.DummyString
		case ColumnType_BYTES:
			vals[i] = parser.DummyBytes
		case ColumnType_DATE:
			vals[i] = parser.DummyDate
		case ColumnType_TIMESTAMP:
			vals[i] = parser.DummyTimestamp
		case ColumnType_INTERVAL:
			vals[i] = parser.DummyInterval
		default:
			return nil, util.Errorf("TODO(pmattis): decoded index key: %s", col.Type.Kind)
		}
	}
	return vals, nil
}

func decodeIndexKeyPrefix(desc *TableDescriptor, key []byte) (IndexID, []byte, error) {
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
		return IndexID(indexID), nil, util.Errorf("%s: unexpected table ID: %d != %d", desc.Name, desc.ID, tableID)
	}

	return IndexID(indexID), key, nil
}

// decodeIndexKey decodes the values that are a part of the specified index
// key. ValTypes is a slice returned from makeKeyVals. The remaining bytes in the
// index key are returned which will either be an encoded column ID for the
// primary key index, the primary key suffix for non-unique secondary indexes
// or unique secondary indexes containing NULL or empty.
func decodeIndexKey(desc *TableDescriptor, indexID IndexID,
	valTypes, vals []parser.Datum, colDirs []encoding.Direction, key []byte) ([]byte, error) {
	decodedIndexID, remaining, err := decodeIndexKeyPrefix(desc, key)
	if err != nil {
		return nil, err
	}

	if decodedIndexID != indexID {
		return nil, util.Errorf("%s: unexpected index ID: %d != %d", desc.Name, indexID, decodedIndexID)
	}
	return decodeKeyVals(valTypes, vals, colDirs, remaining)
}

// decodeKeyVals decodes the values that are part of the key. ValTypes is a
// slice returned from makeKeyVals. The decoded values are stored in the vals
// parameter while the valTypes parameter is unmodified. Note that len(vals) >=
// len(valTypes). The types of the decoded values will match the corresponding
// entry in the valTypes parameter with the exception that a value might also
// be parser.DNull. The remaining bytes in the key after decoding the values
// are returned. A slice of directions can be provided to enforce encoding
// direction on each value in valTypes. If this slice is nil, the direction
// used will default to encoding.Ascending.
func decodeKeyVals(valTypes, vals []parser.Datum, directions []encoding.Direction,
	key []byte) ([]byte, error) {
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
		vals[j], key, err = decodeTableKey(valTypes[j], key, direction)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func decodeTableKey(valType parser.Datum, key []byte, dir encoding.Direction) (
	parser.Datum, []byte, error) {
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
	case parser.DBool:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return parser.DBool(i != 0), rkey, err
	case parser.DInt:
		var i int64
		if dir == encoding.Ascending {
			rkey, i, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, i, err = encoding.DecodeVarintDescending(key)
		}
		return parser.DInt(i), rkey, err
	case parser.DFloat:
		var f float64
		if dir == encoding.Ascending {
			rkey, f, err = encoding.DecodeFloatAscending(key)
		} else {
			rkey, f, err = encoding.DecodeFloatDescending(key)
		}
		return parser.DFloat(f), rkey, err
	case *parser.DDecimal:
		var d *inf.Dec
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDecimalAscending(key, nil)
		} else {
			rkey, d, err = encoding.DecodeDecimalDescending(key, nil)
		}
		dd := &parser.DDecimal{}
		dd.Set(d)
		return dd, rkey, err
	case parser.DString:
		var r string
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeStringAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeStringDescending(key, nil)
		}
		return parser.DString(r), rkey, err
	case parser.DBytes:
		var r []byte
		if dir == encoding.Ascending {
			rkey, r, err = encoding.DecodeBytesAscending(key, nil)
		} else {
			rkey, r, err = encoding.DecodeBytesDescending(key, nil)
		}
		return parser.DBytes(r), rkey, err
	case parser.DDate:
		var t int64
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeVarintAscending(key)
		} else {
			rkey, t, err = encoding.DecodeVarintDescending(key)
		}
		return parser.DDate(t), rkey, err
	case parser.DTimestamp:
		var t time.Time
		if dir == encoding.Ascending {
			rkey, t, err = encoding.DecodeTimeAscending(key)
		} else {
			rkey, t, err = encoding.DecodeTimeDescending(key)
		}
		return parser.DTimestamp{Time: t}, rkey, err
	case parser.DInterval:
		var d duration.Duration
		if dir == encoding.Ascending {
			rkey, d, err = encoding.DecodeDurationAscending(key)
		} else {
			rkey, d, err = encoding.DecodeDurationDescending(key)
		}
		return parser.DInterval{Duration: d}, rkey, err
	default:
		return nil, nil, util.Errorf("TODO(pmattis): decoded index key: %s", valType.Type())
	}
}

type indexEntry struct {
	key   roachpb.Key
	value []byte
}

// colMap maps ColumnIds to indexes in `values`.
func encodeSecondaryIndexes(tableID ID, indexes []IndexDescriptor,
	colMap map[ColumnID]int, values []parser.Datum) ([]indexEntry, error) {
	var secondaryIndexEntries []indexEntry
	for _, secondaryIndex := range indexes {
		secondaryIndexKeyPrefix := MakeIndexKeyPrefix(tableID, secondaryIndex.ID)
		secondaryIndexKey, containsNull, err := encodeIndexKey(
			&secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
		if err != nil {
			return nil, err
		}

		// Add the implicit columns - they are encoded ascendingly.
		implicitDirs := make([]encoding.Direction, 0, len(secondaryIndex.ImplicitColumnIDs))
		for range secondaryIndex.ImplicitColumnIDs {
			implicitDirs = append(implicitDirs, encoding.Ascending)
		}
		extraKey, _, err := encodeColumns(secondaryIndex.ImplicitColumnIDs, implicitDirs,
			colMap, values, nil)
		if err != nil {
			return nil, err
		}

		entry := indexEntry{key: secondaryIndexKey}

		if !secondaryIndex.Unique || containsNull {
			// If the index is not unique or it contains a NULL value, append
			// extraKey to the key in order to make it unique.
			entry.key = append(entry.key, extraKey...)
		}

		// Index keys are considered "sentinel" keys in that they do not have a
		// column ID suffix.
		entry.key = keys.MakeNonColumnKey(entry.key)

		if secondaryIndex.Unique {
			// Note that a unique secondary index that contains a NULL column value
			// will have extraKey appended to the key and stored in the value. We
			// require extraKey to be appended to the key in order to make the key
			// unique. We could potentially get rid of the duplication here but at
			// the expense of complicating scanNode when dealing with unique
			// secondary indexes.
			entry.value = extraKey
		}

		secondaryIndexEntries = append(secondaryIndexEntries, entry)
	}
	return secondaryIndexEntries, nil
}

// marshalColumnValue returns a Go primitive value equivalent of val, of the
// type expected by col. If val's type is incompatible with col, or if
// col's type is not yet implemented, an error is returned.
func marshalColumnValue(col ColumnDescriptor, val parser.Datum, args parser.MapArgs) (interface{}, error) {
	if val == parser.DNull {
		return nil, nil
	}

	switch col.Type.Kind {
	case ColumnType_BOOL:
		if v, ok := val.(parser.DBool); ok {
			return bool(v), nil
		}
		if set, err := args.SetInferredType(val, parser.DummyBool); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_INT:
		if v, ok := val.(parser.DInt); ok {
			return int64(v), nil
		}
		if set, err := args.SetInferredType(val, parser.DummyInt); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(parser.DFloat); ok {
			return float64(v), nil
		}
		if set, err := args.SetInferredType(val, parser.DummyFloat); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_DECIMAL:
		if v, ok := val.(*parser.DDecimal); ok {
			return v.Dec, nil
		}
		if set, err := args.SetInferredType(val, parser.DummyDecimal); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_STRING:
		if v, ok := val.(parser.DString); ok {
			return string(v), nil
		}
		if set, err := args.SetInferredType(val, parser.DummyString); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_BYTES:
		if v, ok := val.(parser.DBytes); ok {
			return string(v), nil
		}
		if v, ok := val.(parser.DString); ok {
			return string(v), nil
		}
		if set, err := args.SetInferredType(val, parser.DummyBytes); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_DATE:
		if v, ok := val.(parser.DDate); ok {
			return int64(v), nil
		}
		if set, err := args.SetInferredType(val, parser.DummyDate); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_TIMESTAMP:
		if v, ok := val.(parser.DTimestamp); ok {
			return v.Time, nil
		}
		if set, err := args.SetInferredType(val, parser.DummyTimestamp); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	case ColumnType_INTERVAL:
		if v, ok := val.(parser.DInterval); ok {
			return v.Duration, nil
		}
		if set, err := args.SetInferredType(val, parser.DummyInterval); err != nil {
			return nil, err
		} else if set != nil {
			return nil, nil
		}
	default:
		return nil, util.Errorf("unsupported column type: %s", col.Type.Kind)
	}
	return nil, fmt.Errorf("value type %s doesn't match type %s of column %q",
		val.Type(), col.Type.Kind, col.Name)
}

// unmarshalColumnValue decodes the value from a key-value pair using the type
// expected by the column. An error is returned if the value's type does not
// match the column's type.
func unmarshalColumnValue(kind ColumnType_Kind, value *roachpb.Value) (parser.Datum, error) {
	if value == nil {
		return parser.DNull, nil
	}

	switch kind {
	case ColumnType_BOOL:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return parser.DBool(v != 0), nil
	case ColumnType_INT:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return parser.DInt(v), nil
	case ColumnType_FLOAT:
		v, err := value.GetFloat()
		if err != nil {
			return nil, err
		}
		return parser.DFloat(v), nil
	case ColumnType_DECIMAL:
		v, err := value.GetDecimal()
		if err != nil {
			return nil, err
		}
		dd := &parser.DDecimal{}
		dd.Set(v)
		return dd, nil
	case ColumnType_STRING:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return parser.DString(v), nil
	case ColumnType_BYTES:
		v, err := value.GetBytes()
		if err != nil {
			return nil, err
		}
		return parser.DBytes(v), nil
	case ColumnType_DATE:
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return parser.DDate(v), nil
	case ColumnType_TIMESTAMP:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return parser.DTimestamp{Time: v}, nil
	case ColumnType_INTERVAL:
		d, err := value.GetDuration()
		if err != nil {
			return nil, err
		}
		return parser.DInterval{Duration: d}, nil
	default:
		return nil, util.Errorf("unsupported column type: %s", kind)
	}
}

// checkValueWidth checks that the width (for strings/byte arrays) and
// scale (for decimals) of the value fits the specified column type.
// Used by INSERT and UPDATE.
func checkValueWidth(col ColumnDescriptor, val parser.Datum) error {
	switch col.Type.Kind {
	case ColumnType_STRING:
		if v, ok := val.(parser.DString); ok {
			if col.Type.Width > 0 && utf8.RuneCountInString(string(v)) > int(col.Type.Width) {
				return fmt.Errorf("value too long for type %s (column %q)", col.Type.SQLString(), col.Name)
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
				maxDigitsLeft := big.NewInt(10)
				maxDigitsLeft.Exp(maxDigitsLeft, big.NewInt(int64(col.Type.Precision-col.Type.Width)), nil)

				var absRounded inf.Dec
				absRounded.Abs(&v.Dec)
				if absRounded.Cmp(inf.NewDecBig(maxDigitsLeft, 0)) != -1 {
					return fmt.Errorf("too many digits for type %s (column %q)", col.Type.SQLString(), col.Name)
				}
			}
		}
	}
	return nil
}
