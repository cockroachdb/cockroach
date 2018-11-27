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

func BenchmarkColumnarize(b *testing.B) {
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

	b.SetBytes(int64(nRows * nCols * int(unsafe.Sizeof(int64(0)))))

	c, err := newColumnarizer(flowCtx, 0, input)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		foundRows := 0
		for {
			batch := c.Next()
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
