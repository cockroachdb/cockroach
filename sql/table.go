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

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// tableKey implements descriptorKey.
type tableKey struct {
	parentID ID
	name     string
}

func (tk tableKey) Key() proto.Key {
	return MakeNameMetadataKey(tk.parentID, tk.name)
}

func (tk tableKey) Name() string {
	return tk.name
}

func makeTableDesc(p *parser.CreateTable) (TableDescriptor, error) {
	desc := TableDescriptor{}
	if err := p.Table.NormalizeTableName(""); err != nil {
		return desc, err
	}
	desc.Name = p.Table.Table()

	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			col := ColumnDescriptor{
				Name:     string(d.Name),
				Nullable: (d.Nullable != parser.NotNull),
			}
			switch t := d.Type.(type) {
			case *parser.BitType:
				col.Type.Kind = ColumnType_BIT
				col.Type.Width = int32(t.N)
			case *parser.BoolType:
				col.Type.Kind = ColumnType_BOOL
			case *parser.IntType:
				col.Type.Kind = ColumnType_INT
				col.Type.Width = int32(t.N)
			case *parser.FloatType:
				col.Type.Kind = ColumnType_FLOAT
				col.Type.Precision = int32(t.Prec)
			case *parser.DecimalType:
				col.Type.Kind = ColumnType_DECIMAL
				col.Type.Width = int32(t.Scale)
				col.Type.Precision = int32(t.Prec)
			case *parser.DateType:
				col.Type.Kind = ColumnType_DATE
			case *parser.TimeType:
				col.Type.Kind = ColumnType_TIME
			case *parser.TimestampType:
				col.Type.Kind = ColumnType_TIMESTAMP
			case *parser.CharType:
				col.Type.Kind = ColumnType_CHAR
				col.Type.Width = int32(t.N)
			case *parser.TextType:
				col.Type.Kind = ColumnType_TEXT
			case *parser.BlobType:
				col.Type.Kind = ColumnType_BLOB
			default:
				panic(fmt.Sprintf("unexpected type %T", t))
			}
			desc.Columns = append(desc.Columns, col)

			// Create any associated index.
			if d.PrimaryKey || d.Unique {
				index := IndexDescriptor{
					Unique:      true,
					ColumnNames: []string{string(d.Name)},
				}
				if d.PrimaryKey {
					index.Name = PrimaryKeyIndexName
					desc.PrimaryIndex = index
				} else {
					desc.Indexes = append(desc.Indexes, index)
				}
			}
		case *parser.IndexTableDef:
			index := IndexDescriptor{
				Name:        string(d.Name),
				Unique:      d.Unique,
				ColumnNames: d.Columns,
			}
			if d.PrimaryKey {
				// Only override the index name if it hasn't been set by the user.
				if index.Name == "" {
					index.Name = PrimaryKeyIndexName
				}
				desc.PrimaryIndex = index
			} else {
				desc.Indexes = append(desc.Indexes, index)
			}
		default:
			return desc, fmt.Errorf("unsupported table def: %T", def)
		}
	}
	return desc, nil
}

func (p *planner) getTableDesc(qname *parser.QualifiedName) (
	*TableDescriptor, error) {
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
		_, tableName := encoding.DecodeBytes(bytes.TrimPrefix(row.Key, prefix), nil)
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
		// TODO(tamird,pmattis): This is a hack; we should have proper nil encoding.
		return encoding.EncodeBytes(b, nil), nil
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
		return encoding.EncodeNumericFloat(b, float64(t)), nil
	case parser.DString:
		return encoding.EncodeBytes(b, []byte(t)), nil
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
		case ColumnType_BIT, ColumnType_INT:
			vals[i] = parser.DInt(0)
		case ColumnType_FLOAT:
			vals[i] = parser.DFloat(0)
		case ColumnType_CHAR, ColumnType_TEXT,
			ColumnType_BLOB:
			vals[i] = parser.DString("")
		default:
			return nil, util.Errorf("TODO(pmattis): decoded index key: %s", col.Type.Kind)
		}
	}
	return vals, nil
}

// decodeIndexKey decodes the values that are a part of the specified index
// key. Vals is a slice returned from makeKeyVals. The remaining bytes in the
// index key are returned which will either be an encoded column ID for the
// primary key index, the primary key suffix for non-unique secondary indexes
// or unique secondary indexes containing NULL or empty.
func decodeIndexKey(desc *TableDescriptor,
	index IndexDescriptor, vals []parser.Datum, key []byte) ([]byte, error) {
	if !bytes.HasPrefix(key, keys.TableDataPrefix) {
		return nil, fmt.Errorf("%s: invalid key prefix: %q", desc.Name, key)
	}
	key = bytes.TrimPrefix(key, keys.TableDataPrefix)

	var tableID uint64
	key, tableID = encoding.DecodeUvarint(key)
	if ID(tableID) != desc.ID {
		return nil, fmt.Errorf("%s: unexpected table ID: %d != %d", desc.Name, desc.ID, tableID)
	}

	var indexID uint64
	key, indexID = encoding.DecodeUvarint(key)
	if IndexID(indexID) != index.ID {
		return nil, fmt.Errorf("%s: unexpected index ID: %d != %d", desc.Name, index.ID, indexID)
	}

	return decodeKeyVals(vals, key)
}

// decodeKeyVals decodes the values that are part of the key. Vals is a slice
// returned from makeKeyVals. The remaining bytes in the key after decoding the
// values are returned.
func decodeKeyVals(vals []parser.Datum, key []byte) ([]byte, error) {
	for j := range vals {
		switch vals[j].(type) {
		case parser.DInt:
			var i int64
			key, i = encoding.DecodeVarint(key)
			vals[j] = parser.DInt(i)
		case parser.DFloat:
			var f float64
			key, f = encoding.DecodeNumericFloat(key)
			vals[j] = parser.DFloat(f)
		case parser.DString:
			var r []byte
			key, r = encoding.DecodeBytes(key, nil)
			vals[j] = parser.DString(r)
		default:
			return nil, util.Errorf("TODO(pmattis): decoded index key: %s", vals[j].Type())
		}
	}

	return key, nil
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

// convertDatum returns a Go primitive value equivalent of val, of the
// type expected by col. If val's type is incompatible with col, or if
// col's type is not yet implemented, an error is returned.
func convertDatum(col ColumnDescriptor, val parser.Datum) (interface{}, error) {
	if val == parser.DNull {
		return nil, nil
	}

	switch col.Type.Kind {
	case ColumnType_BOOL:
		if v, ok := val.(parser.DBool); ok {
			return bool(v), nil
		}
	case ColumnType_BIT, ColumnType_INT:
		if v, ok := val.(parser.DInt); ok {
			return int64(v), nil
		}
	case ColumnType_FLOAT:
		if v, ok := val.(parser.DFloat); ok {
			return float64(v), nil
		}
	// case ColumnType_DECIMAL:
	// case ColumnType_DATE:
	// case ColumnType_TIME:
	// case ColumnType_TIMESTAMP:
	case ColumnType_CHAR, ColumnType_TEXT, ColumnType_BLOB:
		if v, ok := val.(parser.DString); ok {
			return string(v), nil
		}
	default:
		return nil, fmt.Errorf("unsupported type: %s", val.Type())
	}
	return nil, fmt.Errorf("value type %s doesn't match type %s of column %q",
		val.Type(), col.Type.Kind, col.Name)
}
