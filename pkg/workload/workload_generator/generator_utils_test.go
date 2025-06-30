// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file

// generator_utils_test.go
package workload_generator

import (
	"math/rand"
	"testing"
)

// Test min with various inputs
func TestMin(t *testing.T) {
	tests := []struct {
		vals []int
		want int
	}{
		{[]int{}, 1},
		{[]int{5}, 5},
		{[]int{3, 1, 4, 2}, 1},
		{[]int{-1, -5, 0}, -5},
	}
	for _, tt := range tests {
		if got := min(tt.vals); got != tt.want {
			t.Errorf("min(%v) = %d; want %d", tt.vals, got, tt.want)
		}
	}
}

// Test parseFK splits table.column correctly
func TestParseFK(t *testing.T) {
	table, col := parseFK("users.id")
	expectedTable := "users"
	expectedCol := "id"
	if table != expectedTable || col != expectedCol {
		t.Errorf("parseFK(\"users.id\") = (%q, %q); want (\"%s\",\"%s\")", table, col, expectedTable, expectedCol)
	}
}

// Test isLiteralDefault logic
func TestIsLiteralDefault(t *testing.T) {
	tests := []struct {
		expr string
		want bool
	}{
		{"123", true},
		{"'hello'", true},
		{"TRUE", true},
		{"false", true},
		{"(42)", true},
		{"now()", false},
		{"a + b", false},
	}
	for _, tt := range tests {
		if got := isLiteralDefault(tt.expr); got != tt.want {
			t.Errorf("isLiteralDefault(%q) = %v; want %v", tt.expr, got, tt.want)
		}
	}
}

// Test mapSQLType for various SQL types
func TestMapSQLType(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	tests := []struct {
		sql      string
		col      *Column
		wantType string
		wantArg  string // key that must exist in args
	}{
		{"int", &Column{IsPrimaryKey: true}, "sequence", "seed"},
		{"integer", &Column{}, "integer", "min"},
		{"uuid", &Column{}, "uuid", "seed"},
		{"bit(3)", &Column{}, "bit", "size"},
		{"bytea", &Column{}, "bytes", "size"},
		{"varchar(10)", &Column{}, "string", "min"},
		{"char(5)", &Column{}, "string", "min"},
		{"text", &Column{}, "string", "min"},
		{"decimal(5,2)", &Column{}, "float", "min"},
		{"numeric", &Column{}, "float", "min"},
		{"date", &Column{}, "date", "start"},
		{"timestamp", &Column{}, "timestamp", "format"},
		{"boolean", &Column{}, "bool", "seed"},
		{"boolean", &Column{}, "bool", "seed"},
		{"float", &Column{}, "float", "min"},
	}
	for _, tt := range tests {
		typ, args := mapSQLType(tt.sql, tt.col, rng)
		if typ != tt.wantType {
			t.Errorf("mapSQLType(%q) type = %q; want %q", tt.sql, typ, tt.wantType)
		}
		if _, ok := args[tt.wantArg]; !ok {
			t.Errorf("mapSQLType(%q) args missing key %q; got %v", tt.sql, tt.wantArg, args)
		}
	}
}

// Test fanoutProduct computes the cascaded product correctly
func TestFanoutProduct(t *testing.T) {
	// child has one FK to parent with fanout=2
	child := ColumnMeta{HasForeignKey: true, Fanout: 2, FK: "parent.id"}
	parent := ColumnMeta{HasForeignKey: false}
	schema := Schema{
		"parent": {TableBlock{Columns: map[string]ColumnMeta{"id": parent}}},
	}
	prod := fanoutProduct(child, schema)
	if prod != 2 {
		t.Errorf("fanoutProduct = %d; want 2", prod)
	}
}

// Test recordFKSeed populates all namespace variants
func TestRecordFKSeed(t *testing.T) {
	fkSeed := make(map[[2]string]int)
	recordFKSeed("tbl", "col", 99, "db", fkSeed)
	cases := [][2]string{{"tbl", "col"}, {"public__tbl", "col"}, {"db__public__tbl", "col"}}
	for _, k := range cases {
		if got, ok := fkSeed[k]; !ok || got != 99 {
			t.Errorf("fkSeed[%v] = %v, exists=%v; want 99,true", k, got, ok)
		}
	}
}
