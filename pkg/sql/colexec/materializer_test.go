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
	"fmt"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestColumnarizeMaterialize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, _ := randutil.NewPseudoRand()
	nCols := 1 + rng.Intn(4)
	var typs []*types.T
	for len(typs) < nCols {
		typs = append(typs, sqlbase.RandType(rng))
	}
	nRows := 10000
	rows := sqlbase.RandEncDatumRowsOfTypes(rng, nRows, typs)
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

	m, err := NewMaterializer(
		flowCtx,
		1, /* processorID */
		c,
		typs,
		nil, /* output */
		nil, /* metadataSourcesQueue */
		nil, /* toClose */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(ctx)

	for i := 0; i < nRows; i++ {
		row, meta := m.Next()
		if meta != nil {
			t.Fatalf("unexpected meta %+v", meta)
		}
		if row == nil {
			t.Fatal("unexpected nil row")
		}
		for j := range typs {
			if row[j].Datum.Compare(&evalCtx, rows[i][j].Datum) != 0 {
				t.Fatal("unequal rows", row, rows[i])
			}
		}
	}
	row, meta := m.Next()
	if meta != nil {
		t.Fatalf("unexpected meta %+v", meta)
	}
	if row != nil {
		t.Fatal("unexpected not nil row", row)
	}
}

func BenchmarkMaterializer(b *testing.B) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		Cfg:     &execinfra.ServerConfig{Settings: st},
		EvalCtx: &evalCtx,
	}

	rng, _ := randutil.NewPseudoRand()
	nBatches := 10
	nRows := nBatches * coldata.BatchSize()
	for _, typ := range []*types.T{types.Int, types.Float, types.Bytes} {
		typs := []*types.T{typ}
		nCols := len(typs)
		for _, hasNulls := range []bool{false, true} {
			for _, useSelectionVector := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/hasNulls=%t/useSel=%t", typ, hasNulls, useSelectionVector), func(b *testing.B) {
					nullProb := 0.0
					if hasNulls {
						nullProb = nullProbability
					}
					batch := testAllocator.NewMemBatch(typs)
					for _, colVec := range batch.ColVecs() {
						coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
							Rand:             rng,
							Vec:              colVec,
							N:                coldata.BatchSize(),
							NullProbability:  nullProb,
							BytesFixedLength: 8,
						})
					}
					batch.SetLength(coldata.BatchSize())
					if useSelectionVector {
						batch.SetSelection(true)
						sel := batch.Selection()
						for i := 0; i < coldata.BatchSize(); i++ {
							sel[i] = i
						}
					}
					input := newFiniteBatchSource(batch, typs, nBatches)

					b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))
					for i := 0; i < b.N; i++ {
						m, err := NewMaterializer(
							flowCtx,
							0, /* processorID */
							input,
							typs,
							nil, /* output */
							nil, /* metadataSourcesQueue */
							nil, /* toClose */
							nil, /* outputStatsToTrace */
							nil, /* cancelFlow */
						)
						if err != nil {
							b.Fatal(err)
						}
						m.Start(ctx)

						foundRows := 0
						for {
							row, meta := m.Next()
							if meta != nil {
								b.Fatalf("unexpected metadata %v", meta)
							}
							if row == nil {
								break
							}
							foundRows++
						}
						if foundRows != nRows {
							b.Fatalf("expected %d rows, found %d", nRows, foundRows)
						}
						input.reset(nBatches)
					}
				})
			}
		}
	}
}

func BenchmarkColumnarizeMaterialize(b *testing.B) {
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
	c, err := NewColumnarizer(ctx, testAllocator, flowCtx, 0, input)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))
	for i := 0; i < b.N; i++ {
		m, err := NewMaterializer(
			flowCtx,
			1, /* processorID */
			c,
			types,
			nil, /* output */
			nil, /* metadataSourcesQueue */
			nil, /* toClose */
			nil, /* outputStatsToTrace */
			nil, /* cancelFlow */
		)
		if err != nil {
			b.Fatal(err)
		}
		m.Start(ctx)

		foundRows := 0
		for {
			row, meta := m.Next()
			if meta != nil {
				b.Fatalf("unexpected metadata %v", meta)
			}
			if row == nil {
				break
			}
			foundRows++
		}
		if foundRows != nRows {
			b.Fatalf("expected %d rows, found %d", nRows, foundRows)
		}
		input.Reset()
	}
}
