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
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
)

func makeTableDesc(p *parser.CreateTable) (structured.TableDescriptor, error) {
	desc := structured.TableDescriptor{}
	desc.Name = p.Table.String()
	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			col := structured.ColumnDescriptor{
				Name:     string(d.Name),
				Nullable: (d.Nullable != parser.NotNull),
			}
			switch t := d.Type.(type) {
			case *parser.BitType:
				col.Type.Kind = structured.ColumnType_BIT
				col.Type.Width = int32(t.N)
			case *parser.IntType:
				col.Type.Kind = structured.ColumnType_INT
				col.Type.Width = int32(t.N)
			case *parser.FloatType:
				col.Type.Kind = structured.ColumnType_FLOAT
				col.Type.Precision = int32(t.Prec)
			case *parser.DecimalType:
				col.Type.Kind = structured.ColumnType_DECIMAL
				col.Type.Width = int32(t.Scale)
				col.Type.Precision = int32(t.Prec)
			case *parser.DateType:
				col.Type.Kind = structured.ColumnType_DATE
			case *parser.TimeType:
				col.Type.Kind = structured.ColumnType_TIME
			case *parser.TimestampType:
				col.Type.Kind = structured.ColumnType_TIMESTAMP
			case *parser.CharType:
				col.Type.Kind = structured.ColumnType_CHAR
				col.Type.Width = int32(t.N)
			case *parser.TextType:
				col.Type.Kind = structured.ColumnType_TEXT
			case *parser.BlobType:
				col.Type.Kind = structured.ColumnType_BLOB
			}
			desc.Columns = append(desc.Columns, col)

			// Create any associated index.
			if d.PrimaryKey || d.Unique {
				index := structured.IndexDescriptor{
					Unique:      true,
					ColumnNames: []string{string(d.Name)},
				}
				if d.PrimaryKey {
					index.Name = structured.PrimaryKeyIndexName
					desc.PrimaryIndex = index
				} else {
					desc.Indexes = append(desc.Indexes, index)
				}
			}
		case *parser.IndexTableDef:
			index := structured.IndexDescriptor{
				Name:        string(d.Name),
				Unique:      d.Unique,
				ColumnNames: d.Columns,
			}
			if d.PrimaryKey {
				// Only override the index name if it hasn't been set by the user.
				if index.Name == "" {
					index.Name = structured.PrimaryKeyIndexName
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
	*structured.TableDescriptor, error) {
	if err := p.normalizeTableName(qname); err != nil {
		return nil, err
	}
	dbDesc, err := p.getDatabaseDesc(qname.Database())
	if err != nil {
		return nil, err
	}

	nameKey := keys.MakeNameMetadataKey(dbDesc.ID, qname.Table())
	desc := structured.TableDescriptor{}
	if err := p.getDescriptor(nameKey, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

func encodeTablePrefix(tableID uint32) []byte {
	var key []byte
	key = append(key, keys.TableDataPrefix...)
	key = encoding.EncodeUvarint(key, uint64(tableID))
	return key
}

func encodeIndexKeyPrefix(tableID, indexID uint32) []byte {
	var key []byte
	key = append(key, keys.TableDataPrefix...)
	key = encoding.EncodeUvarint(key, uint64(tableID))
	key = encoding.EncodeUvarint(key, uint64(indexID))
	return key
}

func encodeIndexKey(index structured.IndexDescriptor,
	colMap map[uint32]int, values []parser.Datum, indexKey []byte) ([]byte, error) {
	var key []byte
	key = append(key, indexKey...)

	for i, id := range index.ColumnIDs {
		j, ok := colMap[id]
		if !ok {
			return nil, fmt.Errorf("missing \"%s\" primary key column",
				index.ColumnNames[i])
		}
		// TOOD(pmattis): Need to convert the values[i] value to the type expected by
		// the column.
		var err error
		key, err = encodeTableKey(key, values[j])
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func encodeColumnKey(col structured.ColumnDescriptor, primaryKey []byte) []byte {
	var key []byte
	key = append(key, primaryKey...)
	return encoding.EncodeUvarint(key, uint64(col.ID))
}

func encodeTableKey(b []byte, v parser.Datum) ([]byte, error) {
	switch t := v.(type) {
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
	case parser.DNull:
		// TODO(tamird,pmattis): This is a hack; we should have proper nil encoding.
		return encoding.EncodeBytes(b, nil), nil
	}
	return nil, fmt.Errorf("unable to encode table key: %T", v)
}

func decodeIndexKey(desc *structured.TableDescriptor,
	index structured.IndexDescriptor, vals map[string]parser.Datum, key []byte) ([]byte, error) {
	if !bytes.HasPrefix(key, keys.TableDataPrefix) {
		return nil, fmt.Errorf("%s: invalid key prefix: %q", desc.Name, key)
	}
	key = bytes.TrimPrefix(key, keys.TableDataPrefix)

	var tableID uint64
	key, tableID = encoding.DecodeUvarint(key)
	if uint32(tableID) != desc.ID {
		return nil, fmt.Errorf("%s: unexpected table ID: %d != %d", desc.Name, desc.ID, tableID)
	}

	var indexID uint64
	key, indexID = encoding.DecodeUvarint(key)
	if uint32(indexID) != index.ID {
		return nil, fmt.Errorf("%s: unexpected index ID: %d != %d", desc.Name, index.ID, indexID)
	}

	for _, id := range index.ColumnIDs {
		col, err := desc.FindColumnByID(id)
		if err != nil {
			return nil, err
		}
		switch col.Type.Kind {
		case structured.ColumnType_BIT, structured.ColumnType_INT:
			var i int64
			key, i = encoding.DecodeVarint(key)
			vals[col.Name] = parser.DInt(i)
		case structured.ColumnType_FLOAT:
			var f float64
			key, f = encoding.DecodeNumericFloat(key)
			vals[col.Name] = parser.DFloat(f)
		case structured.ColumnType_CHAR, structured.ColumnType_TEXT,
			structured.ColumnType_BLOB:
			var r []byte
			key, r = encoding.DecodeBytes(key, nil)
			vals[col.Name] = parser.DString(r)
		default:
			return nil, util.Errorf("TODO(pmattis): decoded index key: %s", col.Type.Kind)
		}
	}

	return key, nil
}

type indexEntry struct {
	key   []byte
	value []byte
}

func encodeSecondaryIndexes(tableID uint32, indexes []structured.IndexDescriptor,
	colMap map[uint32]int, values []parser.Datum, primaryIndexKeySuffix []byte) ([]indexEntry, error) {
	var secondaryIndexEntries []indexEntry
	for _, secondaryIndex := range indexes {
		secondaryIndexKeyPrefix := encodeIndexKeyPrefix(tableID, secondaryIndex.ID)
		secondaryIndexKey, err := encodeIndexKey(secondaryIndex, colMap, values, secondaryIndexKeyPrefix)
		if err != nil {
			return nil, err
		}

		if secondaryIndex.Unique {
			secondaryIndexEntries = append(secondaryIndexEntries, indexEntry{
				key:   secondaryIndexKey,
				value: primaryIndexKeySuffix,
			})
		} else {
			secondaryIndexEntries = append(secondaryIndexEntries, indexEntry{
				key:   append(secondaryIndexKey, primaryIndexKeySuffix...),
				value: nil,
			})
		}
	}
	return secondaryIndexEntries, nil
}

// TODO(tamird): make this not panic. Not critical, since a panic here
// will just tank a single goroutine on the server and be silently
// swallowed.
func prepareVal(col structured.ColumnDescriptor, val parser.Expr) (interface{}, error) {
	if _, ok := val.(parser.DNull); ok {
		return nil, nil
	}

	switch col.Type.Kind {
	case structured.ColumnType_BIT:
		return bool(val.(parser.DBool)), nil
	case structured.ColumnType_INT:
		return int64(val.(parser.DInt)), nil
	case structured.ColumnType_FLOAT:
		return float64(val.(parser.DFloat)), nil
	// case structured.ColumnType_DECIMAL:
	// case structured.ColumnType_DATE:
	// case structured.ColumnType_TIME:
	// case structured.ColumnType_TIMESTAMP:
	case structured.ColumnType_CHAR, structured.ColumnType_TEXT, structured.ColumnType_BLOB:
		return string(val.(parser.DString)), nil
	}
	return nil, fmt.Errorf("Unsupported type: %T", val)
}
