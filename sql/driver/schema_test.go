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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestMakeSchemaColumns(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		sqlType  string
		colType  structured.ColumnType
		nullable bool
	}{
		{
			"BIT(1)",
			structured.ColumnType{Kind: structured.ColumnType_BIT, Width: 1},
			true,
		},
		{
			"INT(2)",
			structured.ColumnType{Kind: structured.ColumnType_INT, Width: 2},
			true,
		},
		{
			"FLOAT(3,4)",
			structured.ColumnType{Kind: structured.ColumnType_FLOAT, Width: 3, Precision: 4},
			true,
		},
		{
			"DECIMAL(5,6)",
			structured.ColumnType{Kind: structured.ColumnType_DECIMAL, Width: 5, Precision: 6},
			true,
		},
		{
			"DATE",
			structured.ColumnType{Kind: structured.ColumnType_DATE},
			true,
		},
		{
			"TIME",
			structured.ColumnType{Kind: structured.ColumnType_TIME},
			true,
		},
		{
			"DATETIME",
			structured.ColumnType{Kind: structured.ColumnType_DATETIME},
			true,
		},
		{
			"TIMESTAMP",
			structured.ColumnType{Kind: structured.ColumnType_TIMESTAMP},
			true,
		},
		{
			"CHAR",
			structured.ColumnType{Kind: structured.ColumnType_CHAR},
			true,
		},
		{
			"TEXT",
			structured.ColumnType{Kind: structured.ColumnType_TEXT},
			true,
		},
		{
			"BLOB",
			structured.ColumnType{Kind: structured.ColumnType_BLOB},
			true,
		},
		{
			"ENUM(a)",
			structured.ColumnType{Kind: structured.ColumnType_ENUM, Vals: []string{"a"}},
			true,
		},
		{
			"SET(b)",
			structured.ColumnType{Kind: structured.ColumnType_SET, Vals: []string{"b"}},
			true,
		},
		{
			"INT NOT NULL",
			structured.ColumnType{Kind: structured.ColumnType_INT},
			false,
		},
		{
			"INT NULL",
			structured.ColumnType{Kind: structured.ColumnType_INT},
			true,
		},
	}
	for i, d := range testData {
		stmt, err := parser.Parse("CREATE TABLE test (a " + d.sqlType + ")")
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		schema, err := makeSchema(stmt.(*parser.CreateTable))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if !reflect.DeepEqual(d.colType, schema.Columns[0].Type) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.colType, schema.Columns[0])
		}
		if d.nullable != schema.Columns[0].Nullable {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.nullable, schema.Columns[0].Nullable)
		}
	}
}

func TestMakeSchemaIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		sql   string
		index structured.TableSchema_IndexByName
	}{
		{
			"a INT PRIMARY KEY",
			structured.TableSchema_IndexByName{
				Index: structured.Index{
					Name:   "primary",
					Unique: true,
				},
				ColumnNames: []string{"a"},
			},
		},
		{
			"a INT UNIQUE",
			structured.TableSchema_IndexByName{
				Index: structured.Index{
					Name:   "",
					Unique: true,
				},
				ColumnNames: []string{"a"},
			},
		},
		{
			"a INT, b INT, INDEX c (a, b)",
			structured.TableSchema_IndexByName{
				Index: structured.Index{
					Name:   "c",
					Unique: false,
				},
				ColumnNames: []string{"a", "b"},
			},
		},
		{
			"a INT, b INT, UNIQUE INDEX c (a, b)",
			structured.TableSchema_IndexByName{
				Index: structured.Index{
					Name:   "c",
					Unique: true,
				},
				ColumnNames: []string{"a", "b"},
			},
		},
		{
			"a INT, b INT, PRIMARY KEY (a, b)",
			structured.TableSchema_IndexByName{
				Index: structured.Index{
					Name:   "primary",
					Unique: true,
				},
				ColumnNames: []string{"a", "b"},
			},
		},
	}
	for i, d := range testData {
		stmt, err := parser.Parse("CREATE TABLE test (" + d.sql + ")")
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		schema, err := makeSchema(stmt.(*parser.CreateTable))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if !reflect.DeepEqual(d.index, schema.Indexes[0]) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.index, schema.Indexes[0])
		}
	}
}
