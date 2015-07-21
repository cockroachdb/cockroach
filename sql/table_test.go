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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestMakeTableDescColumns(t *testing.T) {
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
			"INT",
			structured.ColumnType{Kind: structured.ColumnType_INT},
			true,
		},
		{
			"FLOAT(3)",
			structured.ColumnType{Kind: structured.ColumnType_FLOAT, Precision: 3},
			true,
		},
		{
			"DECIMAL(5,6)",
			structured.ColumnType{Kind: structured.ColumnType_DECIMAL, Precision: 5, Width: 6},
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
		schema, err := makeTableDesc(stmt[0].(*parser.CreateTable))
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

func TestMakeTableDescIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		sql   string
		index structured.IndexDescriptor
	}{
		{
			"a INT PRIMARY KEY",
			structured.IndexDescriptor{
				Name:        "primary",
				Unique:      true,
				ColumnNames: []string{"a"},
			},
		},
		{
			"a INT UNIQUE",
			structured.IndexDescriptor{
				Name:        "",
				Unique:      true,
				ColumnNames: []string{"a"},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c INDEX (a, b)",
			structured.IndexDescriptor{
				Name:        "c",
				Unique:      false,
				ColumnNames: []string{"a", "b"},
			},
		},
		{
			"a INT, b INT, CONSTRAINT c UNIQUE (a, b)",
			structured.IndexDescriptor{
				Name:        "c",
				Unique:      true,
				ColumnNames: []string{"a", "b"},
			},
		},
		{
			"a INT, b INT, PRIMARY KEY (a, b)",
			structured.IndexDescriptor{
				Name:        "",
				Unique:      true,
				ColumnNames: []string{"a", "b"},
			},
		},
	}
	for i, d := range testData {
		stmt, err := parser.Parse("CREATE TABLE test (" + d.sql + ")")
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		schema, err := makeTableDesc(stmt[0].(*parser.CreateTable))
		if err != nil {
			t.Fatalf("%d: %v", i, err)
		}
		if !reflect.DeepEqual(d.index, schema.Indexes[0]) {
			t.Fatalf("%d: expected %+v, but got %+v", i, d.index, schema.Indexes[0])
		}
	}
}
