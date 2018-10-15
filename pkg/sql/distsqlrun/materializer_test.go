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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func TestColumnarizeMaterialize(t *testing.T) {
	types := []sqlbase.ColumnType{intType, intType}
	nRows := 10000
	nCols := 2
	rows := makeIntRows(nRows, nCols)
	input := NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	c := newColumnarizer(flowCtx, 0, input)

	m := newMaterializer(flowCtx, 1, c, types, []int{0, 1}, &PostProcessSpec{}, nil)

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

func BenchmarkColumnarizeMaterialize(b *testing.B) {
	types := []sqlbase.ColumnType{intType, intType}
	nRows := 10000
	nCols := 2
	rows := makeIntRows(nRows, nCols)
	input := NewRepeatableRowSource(types, rows)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	disposer := RowDisposer{}
	c := newColumnarizer(flowCtx, 0, input)

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))
	m := newMaterializer(flowCtx, 1, c, types, []int{0, 1}, &PostProcessSpec{}, &disposer)
	for i := 0; i < b.N; i++ {
		m.State = StateRunning
		m.batch = nil
		m.curIdx = 0
		input.Reset()
		m.Run(ctx, nil)
	}
}
