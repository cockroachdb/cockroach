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
		{"BOOL", &coltypes.TBool{}},
		{"INT", &coltypes.TInt{}},
		{"INT2", &coltypes.TInt{Width: 16}},
		{"INT4", &coltypes.TInt{Width: 32}},
		{"INT8", &coltypes.TInt{Width: 64}},
		{"FLOAT4", &coltypes.TFloat{Short: true}},
		{"FLOAT8", &coltypes.TFloat{}},
		{"DECIMAL", &coltypes.TDecimal{}},
		{"DECIMAL(8)", &coltypes.TDecimal{Prec: 8}},
		{"DECIMAL(9,10)", &coltypes.TDecimal{Prec: 9, Scale: 10}},
		{"UUID", &coltypes.TUUID{}},
		{"INET", &coltypes.TIPAddr{}},
		{"DATE", &coltypes.TDate{}},
		{"JSONB", &coltypes.TJSON{}},
		{"TIME", &coltypes.TTime{}},
		{"TIMESTAMP", &coltypes.TTimestamp{}},
		{"TIMESTAMPTZ", &coltypes.TTimestampTZ{}},
		{"INTERVAL", &coltypes.TInterval{}},
		{"STRING", &coltypes.TString{Variant: coltypes.TStringVariantSTRING}},
		{"CHAR", &coltypes.TString{Variant: coltypes.TStringVariantCHAR, N: 1}},
		{"CHAR(11)", &coltypes.TString{Variant: coltypes.TStringVariantCHAR, N: 11}},
		{"VARCHAR", &coltypes.TString{Variant: coltypes.TStringVariantVARCHAR}},
		{"VARCHAR(2)", &coltypes.TString{Variant: coltypes.TStringVariantVARCHAR, N: 2}},
		{`"char"`, &coltypes.TString{Variant: coltypes.TStringVariantQCHAR}},
		{"BYTES", &coltypes.TBytes{}},
		{"STRING COLLATE da", &coltypes.TCollatedString{TString: *coltypes.String, Locale: "da"}},
		{"CHAR COLLATE de", &coltypes.TCollatedString{TString: *coltypes.Char, Locale: "de"}},
		{"CHAR(11) COLLATE de", &coltypes.TCollatedString{TString: coltypes.TString{Variant: coltypes.TStringVariantCHAR, N: 11}, Locale: "de"}},
		{"VARCHAR COLLATE en", &coltypes.TCollatedString{TString: *coltypes.VarChar, Locale: "en"}},
		{"VARCHAR(2) COLLATE en", &coltypes.TCollatedString{TString: coltypes.TString{Variant: coltypes.TStringVariantVARCHAR, N: 2}, Locale: "en"}},
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
