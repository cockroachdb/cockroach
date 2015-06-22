// Copyright 2014 The Cockroach Authors.
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
		{"BIT", &BitType{N: -1}},
		{"BIT(2)", &BitType{N: 2}},
		{"TINYINT", &IntType{Name: "TINYINT", N: -1}},
		{"SMALLINT", &IntType{Name: "SMALLINT", N: -1}},
		{"MEDIUMINT", &IntType{Name: "MEDIUMINT", N: -1}},
		{"BIGINT", &IntType{Name: "BIGINT", N: -1}},
		{"INTEGER", &IntType{Name: "INTEGER", N: -1}},
		{"INT UNSIGNED", &IntType{Name: "INT", N: -1, Unsigned: true}},
		{"INT(3) UNSIGNED", &IntType{Name: "INT", N: 3, Unsigned: true}},
		{"REAL", &FloatType{Name: "REAL", N: -1, Prec: -1}},
		{"DOUBLE", &FloatType{Name: "DOUBLE", N: -1, Prec: -1}},
		{"FLOAT", &FloatType{Name: "FLOAT", N: -1, Prec: -1}},
		{"FLOAT(4,5)", &FloatType{Name: "FLOAT", N: 4, Prec: 5}},
		{"FLOAT UNSIGNED", &FloatType{Name: "FLOAT", N: -1, Prec: -1, Unsigned: true}},
		{"FLOAT(6,7) UNSIGNED", &FloatType{Name: "FLOAT", N: 6, Prec: 7, Unsigned: true}},
		{"DECIMAL", &DecimalType{Name: "DECIMAL", N: -1, Prec: -1}},
		{"NUMERIC", &DecimalType{Name: "NUMERIC", N: -1, Prec: -1}},
		{"NUMERIC(8)", &DecimalType{Name: "NUMERIC", N: 8, Prec: -1}},
		{"NUMERIC(9,10)", &DecimalType{Name: "NUMERIC", N: 9, Prec: 10}},
		{"DATE", &DateType{}},
		{"TIME", &TimeType{}},
		{"DATETIME", &DateTimeType{}},
		{"TIMESTAMP", &TimestampType{}},
		{"CHAR", &CharType{Name: "CHAR", N: -1}},
		{"VARCHAR", &CharType{Name: "VARCHAR", N: -1}},
		{"CHAR(11)", &CharType{Name: "CHAR", N: 11}},
		{"BINARY", &BinaryType{Name: "BINARY", N: -1}},
		{"VARBINARY", &BinaryType{Name: "VARBINARY", N: -1}},
		{"BINARY(12)", &BinaryType{Name: "BINARY", N: 12}},
		{"TEXT", &TextType{Name: "TEXT"}},
		{"TINYTEXT", &TextType{Name: "TINYTEXT"}},
		{"MEDIUMTEXT", &TextType{Name: "MEDIUMTEXT"}},
		{"LONGTEXT", &TextType{Name: "LONGTEXT"}},
		{"BLOB", &BlobType{Name: "BLOB"}},
		{"TINYBLOB", &BlobType{Name: "TINYBLOB"}},
		{"MEDIUMBLOB", &BlobType{Name: "MEDIUMBLOB"}},
		{"LONGBLOB", &BlobType{Name: "LONGBLOB"}},
		{"ENUM(c)", &EnumType{Vals: []string{"c"}}},
		{"ENUM(c,d,e)", &EnumType{Vals: []string{"c", "d", "e"}}},
		{"SET(f)", &SetType{Vals: []string{"f"}}},
		{"SET(g,h,i)", &SetType{Vals: []string{"g", "h", "i"}}},
	}
	for i, d := range testData {
		stmt := fmt.Sprintf("CREATE TABLE a (b %s)", d.str)
		tree, err := Parse(stmt)
		if err != nil {
			t.Errorf("%d: %s", i, err)
			continue
		}
		if stmt != tree.String() {
			t.Errorf("%d: expected %s, but got %s", i, stmt, tree)
		}
		createTable, ok := tree.(*CreateTable)
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
