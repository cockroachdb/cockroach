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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestParseColumnType(t *testing.T) {
	testData := []struct {
		str          string
		norm         string
		expectedType coltypes.T
	}{
		{"BIT", "", &coltypes.TInt{IsBit: true, Width: 1}},
		{"BIT(2)", "", &coltypes.TInt{IsBit: true, Width: 2, ExplicitWidth: true}},
		{"BOOL", "", &coltypes.TBool{}},
		{"BOOLEAN", "BOOL", &coltypes.TBool{}},
		{"SMALLINT", "INT2", &coltypes.TInt{Width: 16}},
		{"BIGINT", "INT8", &coltypes.TInt{Width: 64}},
		{"INTEGER", "INT", &coltypes.TInt{Width: 0}},
		{"INT", "", &coltypes.TInt{Width: 0}},
		{"INT2", "", &coltypes.TInt{Width: 16}},
		{"INT4", "", &coltypes.TInt{Width: 32}},
		{"INT8", "", &coltypes.TInt{Width: 64}},
		{"INT64", "INT8", &coltypes.TInt{Width: 64}},
		{"REAL", "FLOAT4", &coltypes.TFloat{Width: 32}},
		{"DOUBLE PRECISION", "FLOAT8", &coltypes.TFloat{Width: 64}},
		{"FLOAT", "FLOAT8", &coltypes.TFloat{Width: 64}},
		{"FLOAT4", "", &coltypes.TFloat{Width: 32}},
		{"FLOAT8", "", &coltypes.TFloat{Width: 64}},
		{"FLOAT(10)", "FLOAT4", &coltypes.TFloat{Width: 32}},
		{"FLOAT(40)", "FLOAT8", &coltypes.TFloat{Width: 64}},
		{"DEC", "DECIMAL", &coltypes.TDecimal{}},
		{"DECIMAL", "", &coltypes.TDecimal{}},
		{"NUMERIC", "DECIMAL", &coltypes.TDecimal{}},
		{"NUMERIC(8)", "DECIMAL(8)", &coltypes.TDecimal{Prec: 8}},
		{"NUMERIC(9,10)", "DECIMAL(9,10)", &coltypes.TDecimal{Prec: 9, Scale: 10}},
		{"UUID", "", &coltypes.TUUID{}},
		{"INET", "", &coltypes.TIPAddr{}},
		{"DATE", "", &coltypes.TDate{}},
		{"TIME", "", &coltypes.TTime{}},
		{"TIMESTAMP", "", &coltypes.TTimestamp{}},
		{"TIMESTAMPTZ", "", &coltypes.TTimestampTZ{}},
		{"TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", &coltypes.TTimestampTZ{}},
		{"INTERVAL", "", &coltypes.TInterval{}},
		{"STRING", "", &coltypes.TString{Name: "STRING"}},
		{"CHAR", "", &coltypes.TString{Name: "CHAR"}},
		{"VARCHAR", "", &coltypes.TString{Name: "VARCHAR"}},
		{"CHAR(11)", "", &coltypes.TString{Name: "CHAR", N: 11}},
		{"TEXT", "", &coltypes.TString{Name: "TEXT"}},
		{"BLOB", "BYTES", &coltypes.TBytes{}},
		{"BYTES", "", &coltypes.TBytes{}},
		{"BYTEA", "BYTES", &coltypes.TBytes{}},
		{"STRING COLLATE da", "", &coltypes.TCollatedString{Name: "STRING", Locale: "da"}},
		{"CHAR COLLATE de", "", &coltypes.TCollatedString{Name: "CHAR", Locale: "de"}},
		{"VARCHAR COLLATE en", "", &coltypes.TCollatedString{Name: "VARCHAR", Locale: "en"}},
		{"CHAR(11) COLLATE en", "", &coltypes.TCollatedString{Name: "CHAR", N: 11, Locale: "en"}},
	}
	for i, d := range testData {
		sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
		stmt, err := parser.ParseOne(sql)
		if err != nil {
			t.Errorf("%d: %s", i, err)
			continue
		}
		normType := d.norm
		if normType == "" {
			normType = d.str
		}
		sqlExpected := fmt.Sprintf("CREATE TABLE a (b %s)", normType)
		if sqlExpected != stmt.String() {
			t.Errorf("%d: expected %s, but got %s", i, sqlExpected, stmt)
		}
		createTable, ok := stmt.(*tree.CreateTable)
		if !ok {
			t.Errorf("%d: expected tree.CreateTable, but got %T", i, stmt)
			continue
		}
		columnDef, ok2 := createTable.Defs[0].(*tree.ColumnTableDef)
		if !ok2 {
			t.Errorf("%d: expected tree.ColumnTableDef, but got %T", i, createTable.Defs[0])
			continue
		}
		if !reflect.DeepEqual(d.expectedType, columnDef.Type) {
			t.Errorf("%d: expected %s, but got %s", i, d.expectedType, columnDef.Type)
			continue
		}
	}
}
