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

package driver

import (
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
)

func makeSchema(p *parser.CreateTable) (structured.TableSchema, error) {
	s := structured.TableSchema{}
	s.Name = p.Table.String()

	for _, def := range p.Defs {
		switch d := def.(type) {
		case *parser.ColumnTableDef:
			col := structured.Column{
				Name:     d.Name,
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
				col.Type.Width = int32(t.N)
				col.Type.Precision = int32(t.Prec)
			case *parser.DecimalType:
				col.Type.Kind = structured.ColumnType_DECIMAL
				col.Type.Width = int32(t.N)
				col.Type.Precision = int32(t.Prec)
			case *parser.DateType:
				col.Type.Kind = structured.ColumnType_DATE
			case *parser.TimeType:
				col.Type.Kind = structured.ColumnType_TIME
			case *parser.DateTimeType:
				col.Type.Kind = structured.ColumnType_DATETIME
			case *parser.TimestampType:
				col.Type.Kind = structured.ColumnType_TIMESTAMP
			case *parser.CharType:
				col.Type.Kind = structured.ColumnType_CHAR
				col.Type.Width = int32(t.N)
			case *parser.BinaryType:
				col.Type.Kind = structured.ColumnType_BINARY
				col.Type.Width = int32(t.N)
			case *parser.TextType:
				col.Type.Kind = structured.ColumnType_TEXT
			case *parser.BlobType:
				col.Type.Kind = structured.ColumnType_BLOB
			case *parser.EnumType:
				col.Type.Kind = structured.ColumnType_ENUM
				col.Type.Vals = t.Vals
			case *parser.SetType:
				col.Type.Kind = structured.ColumnType_SET
				col.Type.Vals = t.Vals
			}
			s.Columns = append(s.Columns, col)

			// Create any associated index.
			if d.PrimaryKey || d.Unique {
				index := structured.TableSchema_IndexByName{
					Index: structured.Index{
						Unique: true,
					},
					ColumnNames: []string{d.Name},
				}
				if d.PrimaryKey {
					index.Name = "primary"
				}
				s.Indexes = append(s.Indexes, index)
			}
		case *parser.IndexTableDef:
			index := structured.TableSchema_IndexByName{
				Index: structured.Index{
					Name:   d.Name,
					Unique: d.Unique,
				},
				ColumnNames: d.Columns,
			}
			s.Indexes = append(s.Indexes, index)
		default:
			return s, fmt.Errorf("unsupported table def: %T", def)
		}
	}
	return s, nil
}
