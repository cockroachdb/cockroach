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
		expectedType coltypes.T
	}{
		{"BIT", &coltypes.TInt{Name: "BIT", Width: 1, ImplicitWidth: true}},
		{"BIT(2)", &coltypes.TInt{Name: "BIT", Width: 2}},
		{"BOOL", &coltypes.TBool{Name: "BOOL"}},
		{"BOOLEAN", &coltypes.TBool{Name: "BOOLEAN"}},
		{"SMALLINT", &coltypes.TInt{Name: "SMALLINT", Width: 16, ImplicitWidth: true}},
		{"BIGINT", &coltypes.TInt{Name: "BIGINT"}},
		{"INTEGER", &coltypes.TInt{Name: "INTEGER"}},
		{"INT", &coltypes.TInt{Name: "INT"}},
		{"INT2", &coltypes.TInt{Name: "INT2", Width: 16, ImplicitWidth: true}},
		{"INT4", &coltypes.TInt{Name: "INT4", Width: 32, ImplicitWidth: true}},
		{"INT8", &coltypes.TInt{Name: "INT8"}},
		{"INT64", &coltypes.TInt{Name: "INT64"}},
		{"REAL", &coltypes.TFloat{Name: "REAL", Width: 32}},
		{"DOUBLE PRECISION", &coltypes.TFloat{Name: "DOUBLE PRECISION", Width: 64}},
		{"FLOAT", &coltypes.TFloat{Name: "FLOAT", Width: 64}},
		{"FLOAT4", &coltypes.TFloat{Name: "FLOAT4", Width: 32}},
		{"FLOAT8", &coltypes.TFloat{Name: "FLOAT8", Width: 64}},
		{"FLOAT(4)", &coltypes.TFloat{Name: "FLOAT", Width: 64, Prec: 4, PrecSpecified: true}},
		{"DEC", &coltypes.TDecimal{Name: "DEC"}},
		{"DECIMAL", &coltypes.TDecimal{Name: "DECIMAL"}},
		{"NUMERIC", &coltypes.TDecimal{Name: "NUMERIC"}},
		{"NUMERIC(8)", &coltypes.TDecimal{Name: "NUMERIC", Prec: 8}},
		{"NUMERIC(9,10)", &coltypes.TDecimal{Name: "NUMERIC", Prec: 9, Scale: 10}},
		{"UUID", &coltypes.TUUID{}},
		{"INET", &coltypes.TIPAddr{Name: "INET"}},
		{"DATE", &coltypes.TDate{}},
		{"TIME", &coltypes.TTime{}},
		{"TIMESTAMP", &coltypes.TTimestamp{}},
		{"TIMESTAMP WITH TIME ZONE", &coltypes.TTimestampTZ{}},
		{"INTERVAL", &coltypes.TInterval{}},
		{"STRING", &coltypes.TString{Name: "STRING"}},
		{"CHAR", &coltypes.TString{Name: "CHAR"}},
		{"VARCHAR", &coltypes.TString{Name: "VARCHAR"}},
		{"CHAR(11)", &coltypes.TString{Name: "CHAR", N: 11}},
		{"TEXT", &coltypes.TString{Name: "TEXT"}},
		{"BLOB", &coltypes.TBytes{Name: "BLOB"}},
		{"BYTES", &coltypes.TBytes{Name: "BYTES"}},
		{"BYTEA", &coltypes.TBytes{Name: "BYTEA"}},
		{"STRING COLLATE da", &coltypes.TCollatedString{Name: "STRING", Locale: "da"}},
		{"CHAR COLLATE de", &coltypes.TCollatedString{Name: "CHAR", Locale: "de"}},
		{"VARCHAR COLLATE en", &coltypes.TCollatedString{Name: "VARCHAR", Locale: "en"}},
		{"CHAR(11) COLLATE en", &coltypes.TCollatedString{Name: "CHAR", N: 11, Locale: "en"}},
	}
	for i, d := range testData {
		sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
		stmt, err := parser.ParseOne(sql)
		if err != nil {
			t.Errorf("%d: %s", i, err)
			continue
		}
		if sql != stmt.String() {
			t.Errorf("%d: expected %s, but got %s", i, sql, stmt)
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
