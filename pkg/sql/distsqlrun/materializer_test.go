// Copyright 2018 The Cockroach Authors.
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
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"testing"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestColumnarizeMaterialize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// TODO(jordan,asubiotto): add randomness to this test as more types are supported.
	typs := []types.T{*types.Int, *types.Int}
	nRows := 10000
	nCols := 2
	rows := sqlbase.MakeIntRows(nRows, nCols)
	input := NewRepeatableRowSource(typs, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	c, err := newColumnarizer(flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}

	m, err := newMaterializer(flowCtx, 1, c, typs, []int{0, 1}, &distsqlpb.PostProcessSpec{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nRows; i++ {
		row, meta := m.Next()
		if meta != nil {
			t.Fatalf("unexpected meta %+v", meta)
		}
		if row == nil {
			t.Fatal("unexpected nil row")
		}
		for j := 0; j < nCols; j++ {
			if row[j].Datum.Compare(&evalCtx, input.rows[i][j].Datum) != 0 {
				t.Fatal("unequal rows", row, input.rows[i])
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
		sqlbase.EncDatum{Datum: tree.NewDDate(53)},
		sqlbase.EncDatum{Datum: tree.NewDString("hello")},
		sqlbase.EncDatum{Datum: tree.NewDBytes("ciao")},
		sqlbase.EncDatum{Datum: tree.NewDName("aloha")},
		sqlbase.EncDatum{Datum: tree.NewDOid(59)},
	}
	input := NewRepeatableRowSource(types, sqlbase.EncDatumRows{inputRow})

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	c, err := newColumnarizer(flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}

	outputToInputColIdx := make([]int, len(types))
	for i := range outputToInputColIdx {
		outputToInputColIdx[i] = i
	}
	m, err := newMaterializer(flowCtx, 1, c, types, outputToInputColIdx, &distsqlpb.PostProcessSpec{}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

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
	input := NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	c, err := newColumnarizer(flowCtx, 0, input)
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))
	for i := 0; i < b.N; i++ {
		m, err := newMaterializer(flowCtx, 1, c, types, []int{0, 1}, &distsqlpb.PostProcessSpec{}, nil, nil)
		if err != nil {
			b.Fatal(err)
		}

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
