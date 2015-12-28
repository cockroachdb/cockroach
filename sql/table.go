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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/gogo/protobuf/proto"
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
		default:
			return desc, util.Errorf("unsupported table def: %T", def)
		}
	}
	return desc, nil
}

func makeColumnDefDescs(d *parser.ColumnTableDef) (*ColumnDescriptor, *IndexDescriptor, error) {
	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: (d.Nullable != parser.NotNull),
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

func (p *planner) getTableDesc(qname *parser.QualifiedName) (*TableDescriptor, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}
	dbDesc, err := p.getDatabaseDesc(qname.Database())
	if err != nil {
		return nil, err
	}

	desc := TableDescriptor{}
	if err := p.getDescriptor(tableKey{dbDesc.ID, qname.Table()}, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

// get the table descriptor for the ID passed in using the planner's txn.
func getTableDescFromID(txn *client.Txn, id ID) (*TableDescriptor, error) {
	desc := &Descriptor{}
	descKey := MakeDescMetadataKey(id)

	if err := txn.GetProto(descKey, desc); err != nil {
		return nil, err
	}
	tableDesc := desc.GetTable()
	if tableDesc == nil {
		return nil, util.Errorf("ID %d is not a table", id)
	}
	return tableDesc, nil
}

// getTableLease acquires a lease for the specified table. The lease will be
// released when the planner closes.
func (p *planner) getTableLease(qname *parser.QualifiedName) (*TableDescriptor, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return nil, err
	}

	if qname.Database() == SystemDB.Name || testDisableTableLeases {
		// We don't go through the normal lease mechanism for system tables. The
		// system.lease and system.descriptor table, in particular, are problematic
		// because they are used for acquiring leases itself, creating a
		// chicken&egg problem.
		return p.getTableDesc(qname)
	}

	tableID, err := p.getTableID(qname)
	if err != nil {
		return nil, err
	}

	if p.leases == nil {
		p.leases = make(map[ID]*LeaseState)
	}

	lease, ok := p.leases[tableID]
	if !ok {
		var err error
		lease, err = p.leaseMgr.Acquire(p.txn, tableID, 0)
		if err != nil {
			return nil, err
		}
		p.leases[tableID] = lease
	}

	return proto.Clone(&lease.TableDescriptor).(*TableDescriptor), nil
}

// getTableID retrieves the table ID for the specified table. It uses the
// descriptor cache to perform lookups, falling back to the KV store when
// necessary.
func (p *planner) getTableID(qname *parser.QualifiedName) (ID, error) {
	if err := qname.NormalizeTableName(p.session.Database); err != nil {
		return 0, err
	}

	// Lookup the database in the cache first, falling back to the KV store if it
	// isn't present. The cache might cause the usage of a recently renamed
	// database, but that's a race that could occur anyways.
	dbDesc, err := p.getCachedDatabaseDesc(qname.Database())
	if err != nil {
		dbDesc, err = p.getDatabaseDesc(qname.Database())
		if err != nil {
			return 0, err
		}
	}

	// Lookup the ID of the table in the cache. The use of the cache might cause
	// the usage of a recently renamed table, but that's a race that could occur
	// anyways.
	nameKey := tableKey{dbDesc.ID, qname.Table()}
	key := nameKey.Key()
	if nameVal := p.systemConfig.GetValue(key); nameVal != nil {
		id, err := nameVal.GetInt()
		return ID(id), err
	}

	gr, err := p.txn.Get(key)
	if err != nil {
		return 0, err
	}
	if !gr.Exists() {
		return 0, fmt.Errorf("table %q does not exist", nameKey.Name())
	}
	return ID(gr.ValueInt()), nil
}

func (p *planner) getTableNames(dbDesc *DatabaseDescriptor) (parser.QualifiedNames, error) {
	prefix := MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var qualifiedNames parser.QualifiedNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeString(
			bytes.TrimPrefix(row.Key, prefix), nil, encoding.Ascending)
		if err != nil {
			return nil, err
		}
		qname := &parser.QualifiedName{
			Base:     parser.Name(dbDesc.Name),
			Indirect: parser.Indirection{parser.NameIndirection(tableName)},
		}
		if err := qname.NormalizeTableName(""); err != nil {
			return nil, err
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
		convertedDir, err := dir.ToEncodingDirection()
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

// Encodes `val` into `b` and returns the new buffer.
func encodeTableKey(b []byte, val parser.Datum, dir encoding.Direction) ([]byte, error) {
	if val == parser.DNull {
		return encoding.EncodeNull(b, dir), nil
	}

	switch t := val.(type) {
	case parser.DBool:
		if t {
			return encoding.EncodeVarint(b, 1, dir), nil
		}
		return encoding.EncodeVarint(b, 0, dir), nil
	case parser.DInt:
		return encoding.EncodeVarint(b, int64(t), dir), nil
	case parser.DFloat:
		return encoding.EncodeFloat(b, float64(t), dir), nil
	case parser.DString:
		return encoding.EncodeString(b, string(t), dir), nil
	case parser.DBytes:
		return encoding.EncodeString(b, string(t), dir), nil
	case parser.DDate:
		return encoding.EncodeVarint(b, int64(t), dir), nil
	case parser.DTimestamp:
		return encoding.EncodeTime(b, t.Time, dir), nil
	case parser.DInterval:
		return encoding.EncodeVarint(b, int64(t.Duration), dir), nil
	}
	return nil, fmt.Errorf("unable to encode table key: %T", val)
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

	key, tableID, err := encoding.DecodeUvarint(key, encoding.Ascending)
	if err != nil {
		return 0, nil, err
	}
	key, indexID, err := encoding.DecodeUvarint(key, encoding.Ascending)
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
// are returned.
func decodeKeyVals(valTypes, vals []parser.Datum, directions []encoding.Direction,
	key []byte) ([]byte, error) {
	if len(directions) != len(valTypes) {
		return nil, util.Errorf("encoding directions doesn't parallel valTypes: %d vs %d.",
			len(directions), len(valTypes))
	}
	for j := range valTypes {
		var err error
		vals[j], key, err = decodeTableKey(valTypes[j], key, directions[j])
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func decodeTableKey(valType parser.Datum, key []byte, dir encoding.Direction) (
	parser.Datum, []byte, error) {
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return parser.DNull, key, nil
	}
	switch valType.(type) {
	case parser.DBool:
		rkey, i, err := encoding.DecodeVarint(key, dir)
		return parser.DBool(i != 0), rkey, err
	case parser.DInt:
		rkey, i, err := encoding.DecodeVarint(key, dir)
		return parser.DInt(i), rkey, err
	case parser.DFloat:
		rkey, f, err := encoding.DecodeFloat(key, nil, dir)
		return parser.DFloat(f), rkey, err
	case parser.DString:
		rkey, r, err := encoding.DecodeString(key, nil, dir)
		return parser.DString(r), rkey, err
	case parser.DBytes:
		rkey, r, err := encoding.DecodeString(key, nil, dir)
		return parser.DBytes(r), rkey, err
	case parser.DDate:
		rkey, t, err := encoding.DecodeVarint(key, dir)
		return parser.DDate(t), rkey, err
	case parser.DTimestamp:
		rkey, t, err := encoding.DecodeTime(key, dir)
		return parser.DTimestamp{Time: t}, rkey, err
	case parser.DInterval:
		rkey, d, err := encoding.DecodeVarint(key, dir)
		return parser.DInterval{Duration: time.Duration(d)}, rkey, err
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
func marshalColumnValue(col ColumnDescriptor, val parser.Datum) (interface{}, error) {
	if val == parser.DNull {
		return nil, nil
	}

	switch col.Type.Kind {
	case ColumnType_BOOL:
		if v, ok := val.(parser.DBool); ok {
			return bool(v), nil
		}
	case ColumnType_INT:
		if v, ok := val.(parser.DInt); ok {
			return int64(v), nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(parser.DFloat); ok {
			return float64(v), nil
		}
	case ColumnType_STRING:
		if v, ok := val.(parser.DString); ok {
			return string(v), nil
		}
	case ColumnType_BYTES:
		if v, ok := val.(parser.DBytes); ok {
			return string(v), nil
		}
	case ColumnType_DATE:
		if v, ok := val.(parser.DDate); ok {
			return int64(v), nil
		}
	case ColumnType_TIMESTAMP:
		if v, ok := val.(parser.DTimestamp); ok {
			return v.Time, nil
		}
	case ColumnType_INTERVAL:
		if v, ok := val.(parser.DInterval); ok {
			return v.Duration, nil
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
		v, err := value.GetInt()
		if err != nil {
			return nil, err
		}
		return parser.DInterval{Duration: time.Duration(v)}, nil
	default:
		return nil, util.Errorf("unsupported column type: %s", kind)
	}
}
