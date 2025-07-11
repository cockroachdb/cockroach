// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// utils_test.go
package workload_generator

import (
	"math/rand"
	"testing"

	asset "github.com/stretchr/testify/assert"
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
		wantType GeneratorType
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

// helper to build a minimal TableSchema
func makeTableSchema(
	name string,
	cols []string,
	pk []string,
	ucs [][]string,
	fks [][3]interface{},
	origTable string,
	tableNo int,
) *TableSchema {
	t := &TableSchema{
		Columns:           make(map[string]*Column, len(cols)),
		ColumnOrder:       cols,
		PrimaryKeys:       pk,
		UniqueConstraints: ucs,
		ForeignKeys:       fks,
		OriginalTable:     origTable,
		TableNumber:       tableNo,
	}
	for _, c := range cols {
		t.Columns[c] = &Column{Name: c}
	}
	return t
}

func TestBuildInitialBlocks(t *testing.T) {
	// Define a simple schema with two columns and one unique constraint
	tbl := makeTableSchema(
		"test",
		[]string{"id", "name"},
		[]string{"id"},
		[][]string{{"name"}},
		nil,
		"public.test",
		3,
	)
	allSchemas := map[string]*TableSchema{"test": tbl}
	baseRows := 50
	rng := rand.New(rand.NewSource(1))
	blocks, fkSeed := buildInitialBlocks(allSchemas, "db", rng, baseRows)

	// One block for "test"
	blks, ok := blocks["test"]
	if !ok || len(blks) != 1 {
		t.Fatalf("expected one block for test; got %v", blks)
	}
	blk := blks[0]

	// Count and metadata
	if blk.Count != baseRows {
		t.Errorf("block.Count = %d; want %d", blk.Count, baseRows)
	}
	if blk.TableNumber != 3 {
		t.Errorf("block.TableNumber = %d; want %d", blk.TableNumber, 3)
	}

	// Replace reflect.DeepEqual with asset.Equal
	asset.Equal(t, []string{"id"}, blk.PK, "block.PK")
	asset.Equal(t, [][]string{{"name"}}, blk.Unique, "block.Unique")
	asset.Equal(t, []string{"id", "name"}, blk.ColumnOrder, "block.ColumnOrder")

	// ColumnMeta created for each column
	if len(blk.Columns) != 2 {
		t.Errorf("got %d columns; want 2", len(blk.Columns))
	}

	// fkSeed should have 3 entries per column (table, public__table, db__public__table)
	wantSeeds := len(tbl.Columns) * 3
	if len(fkSeed) != wantSeeds {
		t.Errorf("len(fkSeed) = %d; want %d", len(fkSeed), wantSeeds)
	}
}

func TestWireForeignKeysAndAdjustFanout(t *testing.T) {
	// Parent schema
	parent := makeTableSchema("parent", []string{"id"}, []string{"id"}, nil, nil, "public.parent", 1)
	// Child schema with a foreign key from cid -> parent.id
	fks := [][3]interface{}{{[]string{"cid"}, "parent", []string{"id"}}}
	child := makeTableSchema("child", []string{"cid"}, []string{"cid"}, nil, fks, "public.child", 2)
	all := map[string]*TableSchema{"parent": parent, "child": child}
	base := 10
	rng := rand.New(rand.NewSource(2))
	blocks, fkSeed := buildInitialBlocks(all, "db", rng, base)
	// Wire and adjust
	wireForeignKeys(blocks, all, fkSeed, rng)
	adjustFanoutForPureFKPKs(blocks)
	// Verify child column meta
	cblk := blocks["child"][0]
	cm, ok := cblk.Columns["cid"]
	if !ok {
		t.Fatal("missing ColumnMeta for cid")
	}
	if !cm.HasForeignKey {
		t.Error("expected HasForeignKey=true")
	}
	if cm.Fanout != 1 {
		t.Errorf("after adjust, Fanout = %d; want 1", cm.Fanout)
	}
}

func TestComputeRowCounts(t *testing.T) {
	// Single table with one FK column
	cmFK := ColumnMeta{HasForeignKey: true, Fanout: 4, FK: "parent.id"}
	cmNonFK := ColumnMeta{HasForeignKey: false}
	// Parent block for lookup
	parentBlk := TableBlock{Columns: map[string]ColumnMeta{"id": cmNonFK}}
	schema := Schema{
		"child":  {TableBlock{Count: 0, Columns: map[string]ColumnMeta{"cid": cmFK}}},
		"parent": {parentBlk},
	}
	computeRowCounts(schema, 5)
	// child Count should be 5 * 4
	childBlk := schema["child"][0]
	if childBlk.Count != 20 {
		t.Errorf("childBlk.Count = %d; want 20", childBlk.Count)
	}
}

func TestBuildWorkloadSchema(t *testing.T) {
	// Combine above into one call
	parent := makeTableSchema("parent", []string{"id"}, []string{"id"}, nil, nil, "public.parent", 0)
	fks := [][3]interface{}{{[]string{"cid"}, "parent", []string{"id"}}}
	child := makeTableSchema("child", []string{"cid"}, []string{"cid"}, nil, fks, "public.child", 1)
	all := map[string]*TableSchema{"parent": parent, "child": child}
	base := 7
	schema := buildWorkloadSchema(all, "db", base)
	// parent block unchanged (no FK)
	pblk := schema["parent"][0]
	if pblk.Count != base {
		t.Errorf("parent Count = %d; want %d", pblk.Count, base)
	}
	// child block should be base (after adjust for pure-FK PKs fanout=1)
	cblk := schema["child"][0]
	if cblk.Count != base {
		t.Errorf("child Count = %d; want %d", cblk.Count, base)
	}
}

// Test makeColumnMeta default and nullability branches
func TestMakeColumnMeta_DefaultAndNull(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	t.Run("Case 1: Default literal and nullable", func(t *testing.T) {
		col1 := &Column{ColType: "int", Default: "42", IsNullable: true, IsPrimaryKey: false}
		cm1, _, _ := buildColumnMeta("tbl", "db", col1, rng)
		if cm1.Default != "42" || cm1.DefaultProb != 0.2 {
			t.Errorf("makeColumnMeta default; got (%s, %f), want (42, 0.2)", cm1.Default, cm1.DefaultProb)
		}
		if pct, ok := cm1.Args["null_pct"].(float64); !ok || pct != 0.1 {
			t.Errorf("makeColumnMeta null_pct; got %v, want 0.1", cm1.Args["null_pct"])
		}
	})
	t.Run("Case 2: Primary key should be non-nullable", func(t *testing.T) {
		col2 := &Column{ColType: "int", Default: "", IsNullable: true, IsPrimaryKey: true}
		cm2, _, _ := buildColumnMeta("tbl", "db", col2, rng)
		if cm2.Default != "" || cm2.DefaultProb != 0 {
			t.Errorf("makeColumnMeta PK default; got (%s, %f), want (empty, 0)", cm2.Default, cm2.DefaultProb)
		}
		if pct2, ok2 := cm2.Args["null_pct"].(float64); !ok2 || pct2 != 0.0 {
			t.Errorf("makeColumnMeta PK null_pct; got %v, want 0.0", cm2.Args["null_pct"])
		}
	})
}
