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
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"fmt"
	"reflect"
	"testing"
)

func TestParseColumnType(t *testing.T) {
	testData := []struct {
		str          string
		expectedType ColumnType
	}{
		{"BIT", &IntColType{Name: "BIT", N: 1, ImplicitWidth: true}},
		{"BIT(2)", &IntColType{Name: "BIT", N: 2}},
		{"BOOL", &BoolColType{Name: "BOOL"}},
		{"BOOLEAN", &BoolColType{Name: "BOOLEAN"}},
		{"SMALLINT", &IntColType{Name: "SMALLINT"}},
		{"BIGINT", &IntColType{Name: "BIGINT"}},
		{"INTEGER", &IntColType{Name: "INTEGER"}},
		{"INT", &IntColType{Name: "INT"}},
		{"INT64", &IntColType{Name: "INT64"}},
		{"REAL", &FloatColType{Name: "REAL"}},
		{"DOUBLE PRECISION", &FloatColType{Name: "DOUBLE PRECISION"}},
		{"FLOAT", &FloatColType{Name: "FLOAT"}},
		{"FLOAT(4)", &FloatColType{Name: "FLOAT", Prec: 4}},
		{"DEC", &DecimalColType{Name: "DEC"}},
		{"DECIMAL", &DecimalColType{Name: "DECIMAL"}},
		{"NUMERIC", &DecimalColType{Name: "NUMERIC"}},
		{"NUMERIC(8)", &DecimalColType{Name: "NUMERIC", Prec: 8}},
		{"NUMERIC(9,10)", &DecimalColType{Name: "NUMERIC", Prec: 9, Scale: 10}},
		{"DATE", &DateColType{}},
		{"TIMESTAMP", &TimestampColType{}},
		{"TIMESTAMP WITH TIME ZONE", &TimestampTZColType{}},
		{"INTERVAL", &IntervalColType{}},
		{"STRING", &StringColType{Name: "STRING"}},
		{"CHAR", &StringColType{Name: "CHAR"}},
		{"VARCHAR", &StringColType{Name: "VARCHAR"}},
		{"CHAR(11)", &StringColType{Name: "CHAR", N: 11}},
		{"TEXT", &StringColType{Name: "TEXT"}},
		{"BLOB", &BytesColType{Name: "BLOB"}},
		{"BYTES", &BytesColType{Name: "BYTES"}},
		{"BYTEA", &BytesColType{Name: "BYTEA"}},
	}
	for i, d := range testData {
		sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
		stmt, err := ParseOneTraditional(sql)
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
