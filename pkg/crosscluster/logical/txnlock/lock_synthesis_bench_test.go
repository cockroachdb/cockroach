// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type benchSpec struct {
	name                string
	makeRows            func(n int) []ldrdecoder.DecodedRow
	makeLockSynthesizer func(b *testing.B) *LockSynthesizer
}

var benchSpecs = []benchSpec{
	{name: "basic", makeRows: makeBasicBenchRows, makeLockSynthesizer: makeBasicLockSynthesizer},
	{name: "fk", makeRows: makeFKBenchRows, makeLockSynthesizer: makeFKLockSynthesizer},
}

// makeBasicBenchRows builds a batch of n rows modeling the schema:
//
//	CREATE TABLE t (id INT PRIMARY KEY, val INT UNIQUE)
//
// Each row is an update that forms a dependency chain via the unique constraint
// on val: row i's new val equals row (i-1)'s old val. This produces n-1
// dependency edges that the topological sort must resolve.
func makeBasicBenchRows(n int) []ldrdecoder.DecodedRow {
	tableID := descpb.ID(100)
	rows := make([]ldrdecoder.DecodedRow, n)
	for i := range rows {
		id := tree.NewDInt(tree.DInt(i + 1))
		// Row 0: val (n+1) → (n+100), freeing val=(n+1).
		// Row i>0: val (n+1-i) → (n+1-(i-1)), taking the value freed by row i-1.
		oldVal := tree.NewDInt(tree.DInt(n + 1 - i))
		newVal := tree.NewDInt(tree.DInt(n + 100 - i))
		if i > 0 {
			newVal = tree.NewDInt(tree.DInt(n + 1 - (i - 1)))
		}
		rows[i] = ldrdecoder.DecodedRow{
			TableID: tableID,
			Row:     tree.Datums{id, newVal},
			PrevRow: tree.Datums{id, oldVal},
		}
	}
	return rows
}

func makeBasicLockSynthesizer(b *testing.B) *LockSynthesizer {
	tableID := descpb.ID(100)
	pkMixin, err := constraintMixin(tableID, 1)
	if err != nil {
		b.Fatal(err)
	}
	ucMixin, err := constraintMixin(tableID, 2)
	if err != nil {
		b.Fatal(err)
	}

	return &LockSynthesizer{
		tableConstraints: map[descpb.ID]*tableConstraints{
			tableID: {
				evalCtx: &eval.Context{},
				PrimaryKeyConstraint: columnSet{
					columns: []int32{0},
					mixin:   pkMixin,
				},
				UniqueConstraints: []columnSet{{
					columns: []int32{1},
					mixin:   ucMixin,
				}},
			},
		},
	}
}

// makeFKBenchRows builds n/2 parent + n/2 child update rows modeling:
//
//	CREATE TABLE parent (id INT PRIMARY KEY, val INT)
//	CREATE TABLE child (
//	  id INT PRIMARY KEY, parent_id INT REFERENCES parent(id), data INT
//	)
//
// Each parent update changes val, producing inbound FK write locks. Each child
// update changes parent_id, producing outbound FK read locks on both old and
// new values. The child's new parent_id references the corresponding parent's
// new id, creating FK dependency edges for the topological sort.
func makeFKBenchRows(n int) []ldrdecoder.DecodedRow {
	parentTableID := descpb.ID(100)
	childTableID := descpb.ID(200)
	rows := make([]ldrdecoder.DecodedRow, 0, n)
	for i := range n / 2 {
		oldFK := tree.NewDInt(tree.DInt(i + 100))
		newFK := tree.NewDInt(tree.DInt(i + 200))
		rows = append(rows, ldrdecoder.DecodedRow{
			TableID: parentTableID,
			Row:     tree.Datums{tree.NewDInt(tree.DInt(i + 1)), newFK},
			PrevRow: tree.Datums{tree.NewDInt(tree.DInt(i + 1)), oldFK},
		}, ldrdecoder.DecodedRow{
			TableID: childTableID,
			Row:     tree.Datums{tree.NewDInt(tree.DInt(n/2 + i + 1)), newFK, tree.NewDInt(0)},
			PrevRow: tree.Datums{tree.NewDInt(tree.DInt(n/2 + i + 1)), oldFK, tree.NewDInt(0)},
		})
	}
	return rows
}

func makeFKLockSynthesizer(b *testing.B) *LockSynthesizer {
	parentTableID := descpb.ID(100)
	childTableID := descpb.ID(200)

	parentPKMixin, err := constraintMixin(parentTableID, 1)
	if err != nil {
		b.Fatal(err)
	}
	childPKMixin, err := constraintMixin(childTableID, 1)
	if err != nil {
		b.Fatal(err)
	}
	// Use the child table ID as origin for the FK mixin so both sides match.
	fkMixin, err := constraintMixin(childTableID, 2)
	if err != nil {
		b.Fatal(err)
	}

	return &LockSynthesizer{
		tableConstraints: map[descpb.ID]*tableConstraints{
			parentTableID: {
				evalCtx: &eval.Context{},
				PrimaryKeyConstraint: columnSet{
					columns: []int32{0},
					mixin:   parentPKMixin,
				},
				OutboundForeignKeyConstraints: map[uint64]columnSet{},
				InboundForeignKeyConstraints: map[uint64]columnSet{
					fkMixin: {columns: []int32{0}, mixin: fkMixin},
				},
			},
			childTableID: {
				evalCtx: &eval.Context{},
				PrimaryKeyConstraint: columnSet{
					columns: []int32{0},
					mixin:   childPKMixin,
				},
				OutboundForeignKeyConstraints: map[uint64]columnSet{
					fkMixin: {columns: []int32{1}, mixin: fkMixin},
				},
				InboundForeignKeyConstraints: map[uint64]columnSet{},
			},
		},
	}
}

func BenchmarkDeriveLocks(b *testing.B) {
	for _, spec := range benchSpecs {
		ls := spec.makeLockSynthesizer(b)
		for _, n := range []int{2, 10, 50} {
			rows := spec.makeRows(n)
			ctx := context.Background()
			b.Run(fmt.Sprintf("rows=%d/%s", n, spec.name), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if _, err := ls.DeriveLocks(ctx, rows); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
