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

package tree_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestParseColumnType(t *testing.T) {
	testData := []struct {
		str          string
		expectedType *types.T
	}{
		{"BIT", types.MakeBit(1)},
		{"VARBIT", types.MakeVarBit(0)},
		{"BIT(2)", types.MakeBit(2)},
		{"VARBIT(2)", types.MakeVarBit(2)},
		{"BOOL", types.Bool},
		{"INT2", types.Int2},
		{"INT4", types.Int4},
		{"INT8", types.Int},
		{"FLOAT4", types.Float4},
		{"FLOAT8", types.Float},
		{"DECIMAL", types.Decimal},
		{"DECIMAL(8)", types.MakeDecimal(8, 0)},
		{"DECIMAL(9,10)", types.MakeDecimal(9, 10)},
		{"UUID", types.Uuid},
		{"INET", types.INet},
		{"DATE", types.Date},
		{"JSONB", types.Jsonb},
		{"TIME", types.Time},
		{"TIMESTAMP", types.Timestamp},
		{"TIMESTAMPTZ", types.TimestampTZ},
		{"INTERVAL", types.Interval},
		{"STRING", types.String},
		{"CHAR", types.MakeChar(1)},
		{"CHAR(11)", types.MakeChar(11)},
		{"VARCHAR", types.VarChar},
		{"VARCHAR(2)", types.MakeVarChar(2)},
		{`"char"`, types.MakeQChar(0)},
		{"BYTES", types.Bytes},
		{"STRING COLLATE da", types.MakeCollatedString("da", 0)},
		{"CHAR COLLATE de", types.MakeCollatedChar("de", 1)},
		{"CHAR(11) COLLATE de", types.MakeCollatedChar("de", 11)},
		{"VARCHAR COLLATE en", types.MakeCollatedVarChar("en", 0)},
		{"VARCHAR(2) COLLATE en", types.MakeCollatedVarChar("en", 2)},
	}
	for i, d := range testData {
		t.Run(d.str, func(t *testing.T) {
			sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			if sql != stmt.AST.String() {
				t.Errorf("%d: expected %s, but got %s", i, sql, stmt.AST)
			}
			createTable, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("%d: expected tree.CreateTable, but got %T", i, stmt)
			}
			columnDef, ok2 := createTable.Defs[0].(*tree.ColumnTableDef)
			if !ok2 {
				t.Fatalf("%d: expected tree.ColumnTableDef, but got %T", i, createTable.Defs[0])
			}
			if !reflect.DeepEqual(d.expectedType, columnDef.Type) {
				t.Fatalf("%d: expected %s, but got %s", i, d.expectedType, columnDef.Type)
			}
		})
	}
}

func TestParseColumnTypeAliases(t *testing.T) {
	testData := []struct {
		str          string
		expectedStr  string
		expectedType *types.T
	}{
		// FLOAT has always been FLOAT8
		{"FLOAT", "CREATE TABLE a (b FLOAT8)", types.Float},
		// A "naked" INT is 64 bits, for historical compatibility.
		{"INT", "CREATE TABLE a (b INT8)", types.Int},
		{"INTEGER", "CREATE TABLE a (b INT8)", types.Int},
	}
	for i, d := range testData {
		t.Run(d.str, func(t *testing.T) {
			sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatalf("%d: %s", i, err)
			}
			if d.expectedStr != stmt.AST.String() {
				t.Errorf("%d: expected %s, but got %s", i, d.expectedStr, stmt.AST)
			}
			createTable, ok := stmt.AST.(*tree.CreateTable)
			if !ok {
				t.Fatalf("%d: expected tree.CreateTable, but got %T", i, stmt.AST)
			}
			columnDef, ok2 := createTable.Defs[0].(*tree.ColumnTableDef)
			if !ok2 {
				t.Fatalf("%d: expected tree.ColumnTableDef, but got %T", i, createTable.Defs[0])
			}
			if !reflect.DeepEqual(d.expectedType, columnDef.Type) {
				t.Fatalf("%d: expected %s, but got %s", i, d.expectedType, columnDef.Type)
			}
		})
	}
}
