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

package parser

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
)

func TestParseColumnType(t *testing.T) {
	testData := []struct {
		str          string
		expectedType coltypes.ColumnType
	}{
		{"BIT", &coltypes.IntColType{Name: "BIT", Width: 1, ImplicitWidth: true}},
		{"BIT(2)", &coltypes.IntColType{Name: "BIT", Width: 2}},
		{"BOOL", &coltypes.BoolColType{Name: "BOOL"}},
		{"BOOLEAN", &coltypes.BoolColType{Name: "BOOLEAN"}},
		{"SMALLINT", &coltypes.IntColType{Name: "SMALLINT", Width: 16, ImplicitWidth: true}},
		{"BIGINT", &coltypes.IntColType{Name: "BIGINT"}},
		{"INTEGER", &coltypes.IntColType{Name: "INTEGER"}},
		{"INT", &coltypes.IntColType{Name: "INT"}},
		{"INT2", &coltypes.IntColType{Name: "INT2", Width: 16, ImplicitWidth: true}},
		{"INT4", &coltypes.IntColType{Name: "INT4", Width: 32, ImplicitWidth: true}},
		{"INT8", &coltypes.IntColType{Name: "INT8"}},
		{"INT64", &coltypes.IntColType{Name: "INT64"}},
		{"REAL", &coltypes.FloatColType{Name: "REAL", Width: 32}},
		{"DOUBLE PRECISION", &coltypes.FloatColType{Name: "DOUBLE PRECISION", Width: 64}},
		{"FLOAT", &coltypes.FloatColType{Name: "FLOAT", Width: 64}},
		{"FLOAT4", &coltypes.FloatColType{Name: "FLOAT4", Width: 32}},
		{"FLOAT8", &coltypes.FloatColType{Name: "FLOAT8", Width: 64}},
		{"FLOAT(4)", &coltypes.FloatColType{Name: "FLOAT", Width: 64, Prec: 4, PrecSpecified: true}},
		{"DEC", &coltypes.DecimalColType{Name: "DEC"}},
		{"DECIMAL", &coltypes.DecimalColType{Name: "DECIMAL"}},
		{"NUMERIC", &coltypes.DecimalColType{Name: "NUMERIC"}},
		{"NUMERIC(8)", &coltypes.DecimalColType{Name: "NUMERIC", Prec: 8}},
		{"NUMERIC(9,10)", &coltypes.DecimalColType{Name: "NUMERIC", Prec: 9, Scale: 10}},
		{"UUID", &coltypes.UUIDColType{}},
		{"INET", &coltypes.IPAddrColType{Name: "INET"}},
		{"DATE", &coltypes.DateColType{}},
		{"TIMESTAMP", &coltypes.TimestampColType{}},
		{"TIMESTAMP WITH TIME ZONE", &coltypes.TimestampTZColType{}},
		{"INTERVAL", &coltypes.IntervalColType{}},
		{"STRING", &coltypes.StringColType{Name: "STRING"}},
		{"CHAR", &coltypes.StringColType{Name: "CHAR"}},
		{"VARCHAR", &coltypes.StringColType{Name: "VARCHAR"}},
		{"CHAR(11)", &coltypes.StringColType{Name: "CHAR", N: 11}},
		{"TEXT", &coltypes.StringColType{Name: "TEXT"}},
		{"BLOB", &coltypes.BytesColType{Name: "BLOB"}},
		{"BYTES", &coltypes.BytesColType{Name: "BYTES"}},
		{"BYTEA", &coltypes.BytesColType{Name: "BYTEA"}},
		{"STRING COLLATE da", &coltypes.CollatedStringColType{Name: "STRING", Locale: "da"}},
		{"CHAR COLLATE de", &coltypes.CollatedStringColType{Name: "CHAR", Locale: "de"}},
		{"VARCHAR COLLATE en", &coltypes.CollatedStringColType{Name: "VARCHAR", Locale: "en"}},
		{"CHAR(11) COLLATE en", &coltypes.CollatedStringColType{Name: "CHAR", N: 11, Locale: "en"}},
	}
	for i, d := range testData {
		sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
		stmt, err := ParseOne(sql)
		if err != nil {
			t.Errorf("%d: %s", i, err)
			continue
		}
		if sql != stmt.String() {
			t.Errorf("%d: expected %s, but got %s", i, sql, stmt)
		}
		createTable, ok := stmt.(*CreateTable)
		if !ok {
			t.Errorf("%d: expected CreateTable, but got %T", i, stmt)
			continue
		}
		columnDef, ok2 := createTable.Defs[0].(*ColumnTableDef)
		if !ok2 {
			t.Errorf("%d: expected ColumnTableDef, but got %T", i, createTable.Defs[0])
			continue
		}
		if !reflect.DeepEqual(d.expectedType, columnDef.Type) {
			t.Errorf("%d: expected %s, but got %s", i, d.expectedType, columnDef.Type)
			continue
		}
	}
}
