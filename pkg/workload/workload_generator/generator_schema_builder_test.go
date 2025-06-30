// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file

// generator_schema_builder_test.go
package workload_generator

import (
	"math/rand"
	"reflect"
	"testing"
)

// helper to build a minimal TableSchema
func makeTableSchema(name string, cols []string, pk []string, ucs [][]string, fks [][3]interface{}, origTable string, tableNo int) *TableSchema {
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
	if !reflect.DeepEqual(blk.PK, []string{"id"}) {
		t.Errorf("block.PK = %v; want [id]", blk.PK)
	}
	if !reflect.DeepEqual(blk.Unique, [][]string{{"name"}}) {
		t.Errorf("block.Unique = %v; want [[name]]", blk.Unique)
	}
	if !reflect.DeepEqual(blk.ColumnOrder, []string{"id", "name"}) {
		t.Errorf("block.ColumnOrder = %v; want [id name]", blk.ColumnOrder)
	}
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
	// Case 1: Default literal and nullable
	col1 := &Column{ColType: "int", Default: "42", IsNullable: true, IsPrimaryKey: false}
	cm1, _, _ := makeColumnMeta("tbl", "db", col1, rng)
	if cm1.Default != "42" || cm1.DefaultProb != 0.2 {
		t.Errorf("makeColumnMeta default; got (%s, %f), want (42, 0.2)", cm1.Default, cm1.DefaultProb)
	}
	if pct, ok := cm1.Args["null_pct"].(float64); !ok || pct != 0.1 {
		t.Errorf("makeColumnMeta null_pct; got %v, want 0.1", cm1.Args["null_pct"])
	}

	// Case 2: Primary key should be non-nullable
	col2 := &Column{ColType: "int", Default: "", IsNullable: true, IsPrimaryKey: true}
	cm2, _, _ := makeColumnMeta("tbl", "db", col2, rng)
	if cm2.Default != "" || cm2.DefaultProb != 0 {
		t.Errorf("makeColumnMeta PK default; got (%s, %f), want (empty, 0)", cm2.Default, cm2.DefaultProb)
	}
	if pct2, ok2 := cm2.Args["null_pct"].(float64); !ok2 || pct2 != 0.0 {
		t.Errorf("makeColumnMeta PK null_pct; got %v, want 0.0", cm2.Args["null_pct"])
	}
}
