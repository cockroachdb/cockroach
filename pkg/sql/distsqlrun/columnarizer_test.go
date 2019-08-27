// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestColumnarizerResetsInternalBatch(t *testing.T) {
	defer leaktest.AfterTest(t)
	typs := []types.T{*types.Int}
	// There will be at least two batches of rows so that we can see whether the
	// internal batch is reset.
	nRows := coldata.BatchSize * 2
	nCols := len(typs)
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := NewRepeatableRowSource(typs, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Cfg:     &ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	c, err := newColumnarizer(ctx, flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}
	c.Init()
	foundRows := 0
	for {
		batch := c.Next(ctx)
		require.Nil(t, batch.Selection(), "columnarizer didn't reset the internal batch")
		if batch.Length() == 0 {
			break
		}
		foundRows += int(batch.Length())
		// The "meat" of the test - we're updating the batch that the columnarizer
		// owns.
		batch.SetSelection(true)
	}
	require.Equal(t, nRows, foundRows)
}

func BenchmarkColumnarize(b *testing.B) {
	types := []types.T{*types.Int, *types.Int}
	nRows := 10000
	nCols := 2
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Cfg:     &ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))

	c, err := newColumnarizer(ctx, flowCtx, 0, input)
	if err != nil {
		b.Fatal(err)
	}
	c.Init()
	for i := 0; i < b.N; i++ {
		foundRows := 0
		for {
			batch := c.Next(ctx)
			if batch.Length() == 0 {
				break
			}
			foundRows += int(batch.Length())
		}
		if foundRows != nRows {
			b.Fatalf("found %d rows, expected %d", foundRows, nRows)
		}
		input.Reset()
	}
}
