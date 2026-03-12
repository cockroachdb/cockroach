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

// makeBenchRows builds a batch of n rows modeling the schema:
//
//	CREATE TABLE t (id INT PRIMARY KEY, val INT UNIQUE)
//
// Each row is an update that forms a dependency chain via the unique constraint
// on val: row i's new val equals row (i-1)'s old val. This produces n-1
// dependency edges that the topological sort must resolve.
func makeBenchRows(tableID descpb.ID, n int) []ldrdecoder.DecodedRow {
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

func BenchmarkDeriveLocks(b *testing.B) {
	tableID := descpb.ID(100)

	pkMixin, err := uniqueIndexMixin(tableID, 1)
	if err != nil {
		b.Fatal(err)
	}
	ucMixin, err := uniqueIndexMixin(tableID, 2)
	if err != nil {
		b.Fatal(err)
	}

	ls := &LockSynthesizer{
		tableConstraints: map[descpb.ID]*tableConstraints{
			tableID: {
				evalCtx: &eval.Context{GlobalState: &eval.GlobalState{}},
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

	for _, n := range []int{2, 10, 50} {
		rows := makeBenchRows(tableID, n)
		ctx := context.Background()
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := ls.DeriveLocks(ctx, rows); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
