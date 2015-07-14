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

package parser2

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
		{"BIT", &BitType{}},
		{"BIT(2)", &BitType{N: 2}},
		{"BOOLEAN", &BoolType{}},
		{"SMALLINT", &IntType{Name: astSmallInt}},
		{"BIGINT", &IntType{Name: astBigInt}},
		{"INTEGER", &IntType{Name: astInteger}},
		{"REAL", &FloatType{Name: astReal}},
		{"DOUBLE PRECISION", &FloatType{Name: astDouble}},
		{"FLOAT", &FloatType{Name: astFloat}},
		{"FLOAT(4)", &FloatType{Name: astFloat, Prec: 4}},
		{"DECIMAL", &DecimalType{Name: astDecimal}},
		{"NUMERIC", &DecimalType{Name: astNumeric}},
		{"NUMERIC(8)", &DecimalType{Name: astNumeric, Prec: 8}},
		{"NUMERIC(9,10)", &DecimalType{Name: astNumeric, Prec: 9, Scale: 10}},
		{"DATE", &DateType{}},
		{"TIME", &TimeType{}},
		{"TIMESTAMP", &TimestampType{}},
		{"CHAR", &CharType{Name: astChar}},
		{"VARCHAR", &CharType{Name: astVarChar}},
		{"CHAR(11)", &CharType{Name: astChar, N: 11}},
		{"TEXT", &TextType{}},
		{"BLOB", &BlobType{}},
	}
	for i, d := range testData {
		sql := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
		stmts, err := Parse(sql)
		if err != nil {
			t.Errorf("%d: %s", i, err)
			continue
		}
		if sql != stmts.String() {
			t.Errorf("%d: expected %s, but got %s", i, sql, stmts)
		}
		createTable, ok := stmts[0].(*CreateTable)
		if !ok {
			t.Errorf("%d: expected CreateTable, but got %T", i, stmts[0])
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
