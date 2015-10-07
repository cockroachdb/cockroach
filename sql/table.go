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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

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
				ColumnNames:      d.Columns,
				StoreColumnNames: d.Storing,
			}
			if err := desc.AddIndex(idx, false); err != nil {
				return desc, err
			}
		case *parser.UniqueConstraintTableDef:
			idx := IndexDescriptor{
				Name:             string(d.Name),
				Unique:           true,
				ColumnNames:      d.Columns,
				StoreColumnNames: d.Storing,
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
		defaultType, err := parser.TypeCheckExpr(d.DefaultExpr)
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
			Unique:      true,
			ColumnNames: []string{string(d.Name)},
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

func (p *planner) getTableNames(dbDesc *DatabaseDescriptor) (parser.QualifiedNames, error) {
	prefix := MakeNameMetadataKey(dbDesc.ID, "")
	sr, err := p.txn.Scan(prefix, prefix.PrefixEnd(), 0)
	if err != nil {
		return nil, err
	}

	var qualifiedNames parser.QualifiedNames
	for _, row := range sr {
		_, tableName, err := encoding.DecodeString(bytes.TrimPrefix(row.Key, prefix), nil)
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

func encodeIndexKey(columnIDs []ColumnID, colMap map[ColumnID]int,
	values []parser.Datum, indexKey []byte) ([]byte, bool, error) {
	var key []byte
	var containsNull bool
	key = append(key, indexKey...)

	for _, id := range columnIDs {
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
		if key, err = encodeTableKey(key, val); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

func encodeTableKey(b []byte, val parser.Datum) ([]byte, error) {
	if val == parser.DNull {
		return encoding.EncodeNull(b), nil
	}

	switch t := val.(type) {
	case parser.DBool:
		if t {
			return encoding.EncodeVarint(b, 1), nil
		}
		return encoding.EncodeVarint(b, 0), nil
	case parser.DInt:
		return encoding.EncodeVarint(b, int64(t)), nil
	case parser.DFloat:
		return encoding.EncodeFloat(b, float64(t)), nil
	case parser.DString:
		return encoding.EncodeString(b, string(t)), nil
	case parser.DBytes:
		return encoding.EncodeString(b, string(t)), nil
	case parser.DDate:
		return encoding.EncodeTime(b, t.Time), nil
	case parser.DTimestamp:
		return encoding.EncodeTime(b, t.Time), nil
	case parser.DInterval:
		return encoding.EncodeVarint(b, int64(t.Duration)), nil
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
	if !bytes.HasPrefix(key, keys.TableDataPrefix) {
		return 0, nil, util.Errorf("%s: invalid key prefix: %q", desc.Name, key)
	}

	key = bytes.TrimPrefix(key, keys.TableDataPrefix)
	key, tableID, err := encoding.DecodeUvarint(key)
	if err != nil {
		return 0, nil, err
	}
	key, indexID, err := encoding.DecodeUvarint(key)
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
func decodeIndexKey(desc *TableDescriptor, index IndexDescriptor, valTypes, vals []parser.Datum, key []byte) ([]byte, error) {
	indexID, remaining, err := decodeIndexKeyPrefix(desc, key)
	if err != nil {
		return nil, err
	}

	if indexID != index.ID {
		return nil, util.Errorf("%s: unexpected index ID: %d != %d", desc.Name, index.ID, indexID)
	}

	return decodeKeyVals(valTypes, vals, remaining)
}

// decodeKeyVals decodes the values that are part of the key. ValTypes is a
// slice returned from makeKeyVals. The decoded values are stored in the vals
// parameter while the valTypes parameter is unmodified. Note that len(vals) >=
// len(valTypes). The types of the decoded values will match the corresponding
// entry in the valTypes parameter with the exception that a value might also
// be parser.DNull. The remaining bytes in the key after decoding the values
// are returned.
func decodeKeyVals(valTypes, vals []parser.Datum, key []byte) ([]byte, error) {
	for j := range valTypes {
		var err error
		vals[j], key, err = decodeTableKey(valTypes[j], key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func decodeTableKey(valType parser.Datum, key []byte) (parser.Datum, []byte, error) {
	var isNull bool
	if key, isNull = encoding.DecodeIfNull(key); isNull {
		return parser.DNull, key, nil
	}
	switch valType.(type) {
	case parser.DBool:
		rkey, i, err := encoding.DecodeVarint(key)
		return parser.DBool(i != 0), rkey, err
	case parser.DInt:
		rkey, i, err := encoding.DecodeVarint(key)
		return parser.DInt(i), rkey, err
	case parser.DFloat:
		rkey, f, err := encoding.DecodeFloat(key, nil)
		return parser.DFloat(f), rkey, err
	case parser.DString:
		rkey, r, err := encoding.DecodeString(key, nil)
		return parser.DString(r), rkey, err
	case parser.DBytes:
		rkey, r, err := encoding.DecodeString(key, nil)
		return parser.DBytes(r), rkey, err
	case parser.DDate:
		rkey, t, err := encoding.DecodeTime(key)
		return parser.DDate{Time: t}, rkey, err
	case parser.DTimestamp:
		rkey, t, err := encoding.DecodeTime(key)
		return parser.DTimestamp{Time: t}, rkey, err
	case parser.DInterval:
		rkey, d, err := encoding.DecodeVarint(key)
		return parser.DInterval{Duration: time.Duration(d)}, rkey, err
	default:
		return nil, nil, util.Errorf("TODO(pmattis): decoded index key: %s", valType.Type())
	}
}

type indexEntry struct {
	key   []byte
	value []byte
}

func encodeSecondaryIndexes(tableID ID, indexes []IndexDescriptor,
	colMap map[ColumnID]int, values []parser.Datum) ([]indexEntry, error) {
	var secondaryIndexEntries []indexEntry
	for _, secondaryIndex := range indexes {
		secondaryIndexKeyPrefix := MakeIndexKeyPrefix(tableID, secondaryIndex.ID)
		secondaryIndexKey, containsNull, err := encodeIndexKey(
			secondaryIndex.ColumnIDs, colMap, values, secondaryIndexKeyPrefix)
		if err != nil {
			return nil, err
		}

		extraKey, _, err := encodeIndexKey(secondaryIndex.ImplicitColumnIDs, colMap, values, nil)
		if err != nil {
			return nil, err
		}

		entry := indexEntry{key: secondaryIndexKey}

		if !secondaryIndex.Unique || containsNull {
			// If the index is not unique or it contains a NULL value, append
			// extraKey to the key in order to make it unique.
			entry.key = append(entry.key, extraKey...)
		}
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
			return v.Time, nil
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
		v, err := value.GetBytesChecked()
		if err != nil {
			return nil, err
		}
		return parser.DString(v), nil
	case ColumnType_BYTES:
		v, err := value.GetBytesChecked()
		if err != nil {
			return nil, err
		}
		return parser.DBytes(v), nil
	case ColumnType_DATE:
		v, err := value.GetTime()
		if err != nil {
			return nil, err
		}
		return parser.DDate{Time: v}, nil
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
