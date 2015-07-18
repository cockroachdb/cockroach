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

package sqlserver

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser2"
	"github.com/cockroachdb/cockroach/structured"
)

func makeTableDesc(p *parser2.CreateTable) (structured.TableDescriptor, error) {
	desc := structured.TableDescriptor{}
	desc.Name = p.Table.String()

	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser2.ColumnTableDef:
			col := structured.ColumnDescriptor{
				Name:     d.Name,
				Nullable: (d.Nullable != parser2.NotNull),
			}
			switch t := d.Type.(type) {
			case *parser2.BitType:
				col.Type.Kind = structured.ColumnType_BIT
				col.Type.Width = int32(t.N)
			case *parser2.IntType:
				col.Type.Kind = structured.ColumnType_INT
				col.Type.Width = int32(t.N)
			case *parser2.FloatType:
				col.Type.Kind = structured.ColumnType_FLOAT
				col.Type.Precision = int32(t.Prec)
			case *parser2.DecimalType:
				col.Type.Kind = structured.ColumnType_DECIMAL
				col.Type.Width = int32(t.Scale)
				col.Type.Precision = int32(t.Prec)
			case *parser2.DateType:
				col.Type.Kind = structured.ColumnType_DATE
			case *parser2.TimeType:
				col.Type.Kind = structured.ColumnType_TIME
			case *parser2.TimestampType:
				col.Type.Kind = structured.ColumnType_TIMESTAMP
			case *parser2.CharType:
				col.Type.Kind = structured.ColumnType_CHAR
				col.Type.Width = int32(t.N)
			case *parser2.TextType:
				col.Type.Kind = structured.ColumnType_TEXT
			case *parser2.BlobType:
				col.Type.Kind = structured.ColumnType_BLOB
			}
			desc.Columns = append(desc.Columns, col)

			// Create any associated index.
			if d.PrimaryKey || d.Unique {
				index := structured.IndexDescriptor{
					Unique:      true,
					ColumnNames: []string{d.Name},
				}
				if d.PrimaryKey {
					index.Name = "primary"
				}
				desc.Indexes = append(desc.Indexes, index)
			}
		case *parser2.IndexTableDef:
			index := structured.IndexDescriptor{
				Name:        d.Name,
				Unique:      d.Unique,
				ColumnNames: d.Columns,
			}
			desc.Indexes = append(desc.Indexes, index)
		default:
			return desc, fmt.Errorf("unsupported table def: %T", def)
		}
	}
	return desc, nil
}
