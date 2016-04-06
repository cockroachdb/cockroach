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
		{"BIT", &IntType{Name: "BIT"}},
		{"BIT(2)", &IntType{Name: "BIT", N: 2}},
		{"BOOL", &BoolType{Name: "BOOL"}},
		{"BOOLEAN", &BoolType{Name: "BOOLEAN"}},
		{"SMALLINT", &IntType{Name: "SMALLINT"}},
		{"BIGINT", &IntType{Name: "BIGINT"}},
		{"INTEGER", &IntType{Name: "INTEGER"}},
		{"INT", &IntType{Name: "INT"}},
		{"INT64", &IntType{Name: "INT64"}},
		{"REAL", &FloatType{Name: "REAL"}},
		{"DOUBLE PRECISION", &FloatType{Name: "DOUBLE PRECISION"}},
		{"FLOAT", &FloatType{Name: "FLOAT"}},
		{"FLOAT(4)", &FloatType{Name: "FLOAT", Prec: 4}},
		{"DEC", &DecimalType{Name: "DEC"}},
		{"DECIMAL", &DecimalType{Name: "DECIMAL"}},
		{"NUMERIC", &DecimalType{Name: "NUMERIC"}},
		{"NUMERIC(8)", &DecimalType{Name: "NUMERIC", Prec: 8}},
		{"NUMERIC(9,10)", &DecimalType{Name: "NUMERIC", Prec: 9, Scale: 10}},
		{"DATE", &DateType{}},
		{"TIMESTAMP", &TimestampType{}},
		{"TIMESTAMPTZ", &TimestampType{withZone: true}},
		{"INTERVAL", &IntervalType{}},
		{"STRING", &StringType{Name: "STRING"}},
		{"CHAR", &StringType{Name: "CHAR"}},
		{"VARCHAR", &StringType{Name: "VARCHAR"}},
		{"CHAR(11)", &StringType{Name: "CHAR", N: 11}},
		{"TEXT", &StringType{Name: "TEXT"}},
		{"BLOB", &BytesType{Name: "BLOB"}},
		{"BYTES", &BytesType{Name: "BYTES"}},
		{"BYTEA", &BytesType{Name: "BYTEA"}},
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
