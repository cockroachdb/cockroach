// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestVectorizedErrorPropagation verifies that materializers successfully
// handle panics with exec.VectorizedRuntimeErrors. It sets up the following
// chain:
// RowSource -> columnarizer -> vectorized error emitter -> materializer,
// and makes sure that all rows are received at the end of the chain.
func TestVectorizedErrorPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := FlowCtx{
		EvalCtx:  &evalCtx,
		Settings: cluster.MakeTestingClusterSettings(),
	}

	nRows, nCols := 10, 1
	types := sqlbase.OneIntCol

	input := NewRepeatableRowSource(types, sqlbase.MakeIntRows(nRows, nCols))

	col, err := newColumnarizer(&flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}

	vee := exec.NewTestVectorizedErrorEmitter(col)
	matOut := &RowBuffer{types: types}
	mat, err := newMaterializer(&flowCtx, 1, vee, types, []int{0}, &distsqlpb.PostProcessSpec{}, matOut)
	if err != nil {
		t.Fatal(err)
	}

	mat.Run(ctx)

	rowCount, metaCount := 0, 0
	for {
		row, meta := matOut.Next()
		if row == nil && meta == nil {
			break
		}
		if row != nil {
			rowCount++
		} else if meta.Err != nil {
			t.Fatal(meta.Err)
		} else {
			metaCount++
		}
	}
	if rowCount != nRows {
		t.Fatalf("expected %d rows but %d received", nRows, rowCount)
	}
	if metaCount != 0 {
		t.Fatalf("expected no metadata but %d received", metaCount)
	}
}

// TestNonVectorizedErrorPropagation verifies that materializers do not handle
// panics with errors that are not exec.VectorizedRuntimeErrors. It sets up the
// following chain:
// RowSource -> columnarizer -> non vectorized error emitter -> materializer,
// and makes sure that a panic is emitted all the way through the chain.
func TestNonVectorizedErrorPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := FlowCtx{
		EvalCtx:  &evalCtx,
		Settings: cluster.MakeTestingClusterSettings(),
	}

	nRows, nCols := 10, 1
	types := sqlbase.OneIntCol

	input := NewRepeatableRowSource(types, sqlbase.MakeIntRows(nRows, nCols))

	col, err := newColumnarizer(&flowCtx, 0, input)
	if err != nil {
		t.Fatal(err)
	}

	nvee := exec.NewTestNonVectorizedErrorEmitter(col)
	matOut := &RowBuffer{types: types}
	mat, err := newMaterializer(&flowCtx, 1, nvee, types, []int{0}, &distsqlpb.PostProcessSpec{}, matOut)
	if err != nil {
		t.Fatal(err)
	}

	panicEmitted := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				panicEmitted = true
			}
		}()
		mat.Run(ctx)
	}()
	if !panicEmitted {
		t.Fatalf("Not VectorizedRuntimeError was caught by the operators.")
	}
}
