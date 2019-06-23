// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

// TestVectorizedErrorPropagation verifies that materializers successfully
// handle panics coming from exec package. It sets up the following chain:
// RowSource -> columnarizer -> exec error emitter -> materializer,
// and makes sure that a panic doesn't occur yet the error is propagated.
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

	nRows, nCols := 1, 1
	types := sqlbase.OneIntCol
	input := NewRepeatableRowSource(types, sqlbase.MakeIntRows(nRows, nCols))

	col, err := newColumnarizer(&flowCtx, 0 /* processorID */, input)
	if err != nil {
		t.Fatal(err)
	}

	vee := exec.NewTestVectorizedErrorEmitter(col)
	mat, err := newMaterializer(
		&flowCtx,
		1, /* processorID */
		vee,
		types,
		[]int{0},
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourceQueue */
		nil, /* outputStatsToTrace */
	)
	if err != nil {
		t.Fatal(err)
	}

	var meta *distsqlpb.ProducerMetadata
	panicEmitted := false
	func() {
		defer func() {
			if err := recover(); err != nil {
				panicEmitted = true
			}
		}()
		mat.Start(ctx)
		_, meta = mat.Next()
	}()
	if panicEmitted {
		t.Fatalf("VectorizedRuntimeError was not caught by the operators.")
	}
	if meta == nil || meta.Err == nil {
		t.Fatalf("VectorizedRuntimeError was not propagated as metadata.")
	}
}

// TestNonVectorizedErrorPropagation verifies that materializers do not handle
// panics coming not from exec package. It sets up the following chain:
// RowSource -> columnarizer -> distsqlrun error emitter -> materializer,
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

	nRows, nCols := 1, 1
	types := sqlbase.OneIntCol
	input := NewRepeatableRowSource(types, sqlbase.MakeIntRows(nRows, nCols))

	col, err := newColumnarizer(&flowCtx, 0 /* processorID */, input)
	if err != nil {
		t.Fatal(err)
	}

	nvee := newTestVectorizedErrorEmitter(col)
	mat, err := newMaterializer(
		&flowCtx,
		1, /* processorID */
		nvee,
		types,
		[]int{0},
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		nil, /* metadataSourceQueue */
		nil, /* outputStatsToTrace */
	)
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
		mat.Start(ctx)
		_, _ = mat.Next()
	}()
	if !panicEmitted {
		t.Fatalf("Not VectorizedRuntimeError was caught by the operators.")
	}
}

// testNonVectorizedErrorEmitter is an exec.Operator that panics on every
// odd-numbered invocation of Next() and returns the next batch from the input
// on every even-numbered (i.e. it becomes a noop for those iterations). Used
// for tests only.
type testNonVectorizedErrorEmitter struct {
	input     exec.Operator
	emitBatch bool
}

var _ exec.Operator = &testNonVectorizedErrorEmitter{}

// newTestVectorizedErrorEmitter creates a new TestVectorizedErrorEmitter.
func newTestVectorizedErrorEmitter(input exec.Operator) exec.Operator {
	return &testNonVectorizedErrorEmitter{input: input}
}

// Init is part of exec.Operator interface.
func (e *testNonVectorizedErrorEmitter) Init() {
	e.input.Init()
}

// Next is part of exec.Operator interface.
func (e *testNonVectorizedErrorEmitter) Next(ctx context.Context) coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		panic(errors.New("An error from distsqlrun package"))
	}

	e.emitBatch = false
	return e.input.Next(ctx)
}
