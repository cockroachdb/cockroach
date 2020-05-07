// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestColumnarizerResetsInternalBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	typs := []*types.T{types.Int}
	// There will be at least two batches of rows so that we can see whether the
	// internal batch is reset.
	nRows := coldata.BatchSize() * 2
	nCols := len(typs)
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := execinfra.NewRepeatableRowSource(typs, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}
	c.Init()
	foundRows := 0
	for {
		batch := c.Next(ctx)
		require.Nil(t, batch.Selection(), "Columnarizer didn't reset the internal batch")
		if batch.Length() == 0 {
			break
		}
		foundRows += batch.Length()
		// The "meat" of the test - we're updating the batch that the Columnarizer
		// owns.
		batch.SetSelection(true)
	}
	require.Equal(t, nRows, foundRows)
}

func TestColumnarizerDrainsAndClosesInput(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	rb := distsqlutils.NewRowBuffer([]*types.T{types.Int}, nil /* rows */, distsqlutils.RowBufferArgs{})
	flowCtx := &execinfra.FlowCtx{EvalCtx: &evalCtx}

	const errMsg = "artificial error"
	rb.Push(nil, &execinfrapb.ProducerMetadata{Err: errors.New(errMsg)})
	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0 /* processorID */, rb)
	require.NoError(t, err)

	c.Init()

	// Calling DrainMeta from the vectorized execution engine should propagate to
	// non-vectorized components as calling ConsumerDone and then draining their
	// metadata.
	metaCh := make(chan []execinfrapb.ProducerMetadata)
	go func() {
		metaCh <- c.DrainMeta(ctx)
	}()

	// Make Next race with DrainMeta, this should be supported by the
	// Columnarizer. If the metadata is obtained through this Next call, the
	// Columnarizer still returns it in DrainMeta.
	_ = c.Next(ctx)

	meta := <-metaCh
	require.True(t, len(meta) == 1)
	require.True(t, testutils.IsError(meta[0].Err, errMsg))
	require.True(t, rb.Done)
}

func BenchmarkColumnarize(b *testing.B) {
	types := []*types.T{types.Int, types.Int}
	nRows := 10000
	nCols := 2
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := execinfra.NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))

	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
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
			foundRows += batch.Length()
		}
		if foundRows != nRows {
			b.Fatalf("found %d rows, expected %d", foundRows, nRows)
		}
		input.Reset()
	}
}
