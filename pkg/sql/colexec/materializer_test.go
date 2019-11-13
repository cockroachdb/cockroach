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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

func TestColumnarizeMaterialize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(jordan,asubiotto): add randomness to this test as more types are supported.
	typs := []types.T{*types.Int, *types.Int}
	nRows := 10000
	nCols := 2
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

	m, err := NewMaterializer(
		flowCtx,
		1, /* processorID */
		c,
		typs,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourcesQueue */
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
		for j := 0; j < nCols; j++ {
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

func TestMaterializeTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(andyk): Make sure to add more types here. Consider iterating over
	// types.OidToTypes list and also using randomly generated EncDatums.
	types := []types.T{
		*types.Bool,
		*types.Int,
		*types.Float,
		*types.Decimal,
		*types.Date,
		*types.String,
		*types.Bytes,
		*types.Name,
		*types.Oid,
	}
	inputRow := sqlbase.EncDatumRow{
		sqlbase.EncDatum{Datum: tree.DBoolTrue},
		sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(31))},
		sqlbase.EncDatum{Datum: tree.NewDFloat(37.41)},
		sqlbase.EncDatum{Datum: &tree.DDecimal{Decimal: *apd.New(43, 47)}},
		sqlbase.EncDatum{Datum: tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(53))},
		sqlbase.EncDatum{Datum: tree.NewDString("hello")},
		sqlbase.EncDatum{Datum: tree.NewDBytes("ciao")},
		sqlbase.EncDatum{Datum: tree.NewDName("aloha")},
		sqlbase.EncDatum{Datum: tree.NewDOid(59)},
	}
	input := execinfra.NewRepeatableRowSource(types, sqlbase.EncDatumRows{inputRow})

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

	outputToInputColIdx := make([]int, len(types))
	for i := range outputToInputColIdx {
		outputToInputColIdx[i] = i
	}
	m, err := NewMaterializer(
		flowCtx,
		1, /* processorID */
		c,
		types,
		&execinfrapb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourcesQueue */
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}
	m.Start(ctx)

	row, meta := m.Next()
	if meta != nil {
		t.Fatalf("unexpected meta %+v", meta)
	}
	if row == nil {
		t.Fatal("unexpected nil row")
	}
	for i := range inputRow {
		inDatum := inputRow[i].Datum
		outDatum := row[i].Datum
		if inDatum.Compare(&evalCtx, outDatum) != 0 {
			t.Fatal("unequal datums", inDatum, outDatum)
		}
	}
}

func BenchmarkColumnarizeMaterialize(b *testing.B) {
	types := []types.T{*types.Int, *types.Int}
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
			&execinfrapb.PostProcessSpec{},
			nil, /* output */
			nil, /* metadataSourcesQueue */
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
