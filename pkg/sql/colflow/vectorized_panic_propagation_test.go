// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colflow_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestVectorizedInternalPanic verifies that materializers successfully
// handle panics coming from exec package. It sets up the following chain:
// RowSource -> columnarizer -> test panic emitter -> materializer,
// and makes sure that a panic doesn't occur yet the error is propagated.
func TestVectorizedInternalPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows, nCols := 1, 1
	typs := types.OneIntCol
	input := execinfra.NewRepeatableRowSource(typs, randgen.MakeIntRows(nRows, nCols))

	col := colexec.NewBufferingColumnarizer(testAllocator, &flowCtx, 0 /* processorID */, input)
	vee := newTestVectorizedInternalPanicEmitter(col)
	mat := colexec.NewMaterializer(
		&flowCtx,
		1, /* processorID */
		colexecargs.OpWithMetaInfo{Root: vee},
		typs,
	)
	mat.Start(ctx)

	var meta *execinfrapb.ProducerMetadata
	require.NotPanics(t, func() { _, meta = mat.Next() }, "InternalError was not caught")
	require.NotNil(t, meta.Err, "InternalError was not propagated as metadata")
}

// TestNonVectorizedPanicPropagation verifies that materializers do not handle
// panics coming not from exec package. It sets up the following chain:
// RowSource -> columnarizer -> test panic emitter -> materializer,
// and makes sure that a panic is emitted all the way through the chain.
func TestNonVectorizedPanicPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows, nCols := 1, 1
	typs := types.OneIntCol
	input := execinfra.NewRepeatableRowSource(typs, randgen.MakeIntRows(nRows, nCols))

	col := colexec.NewBufferingColumnarizer(testAllocator, &flowCtx, 0 /* processorID */, input)
	nvee := newTestNonVectorizedPanicEmitter(col)
	mat := colexec.NewMaterializer(
		&flowCtx,
		1, /* processorID */
		colexecargs.OpWithMetaInfo{Root: nvee},
		typs,
	)
	mat.Start(ctx)

	require.Panics(t, func() { mat.Next() }, "NonVectorizedPanic was caught by the operators")
}

// testVectorizedInternalPanicEmitter is a colexecop.Operator that panics with
// colexecerror.InternalError on every odd-numbered invocation of Next()
// and returns the next batch from the input on every even-numbered (i.e. it
// becomes a noop for those iterations). Used for tests only.
type testVectorizedInternalPanicEmitter struct {
	colexecop.OneInputHelper
	emitBatch bool
}

var _ colexecop.Operator = &testVectorizedInternalPanicEmitter{}

func newTestVectorizedInternalPanicEmitter(input colexecop.Operator) colexecop.Operator {
	return &testVectorizedInternalPanicEmitter{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
	}
}

// Next is part of colexecop.Operator interface.
func (e *testVectorizedInternalPanicEmitter) Next() coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		colexecerror.InternalError(errors.AssertionFailedf(""))
	}

	e.emitBatch = false
	return e.Input.Next()
}

// testNonVectorizedPanicEmitter is the same as
// testVectorizedInternalPanicEmitter but it panics with the builtin panic
// function. Used for tests only. It is the only colexecop.Operator panics from
// which are not caught.
type testNonVectorizedPanicEmitter struct {
	colexecop.OneInputHelper
	emitBatch bool
}

var _ colexecop.Operator = &testVectorizedInternalPanicEmitter{}

func newTestNonVectorizedPanicEmitter(input colexecop.Operator) colexecop.Operator {
	return &testNonVectorizedPanicEmitter{
		OneInputHelper: colexecop.MakeOneInputHelper(input),
	}
}

// Next is part of colexecop.Operator interface.
func (e *testNonVectorizedPanicEmitter) Next() coldata.Batch {
	if !e.emitBatch {
		e.emitBatch = true
		colexecerror.NonCatchablePanic("")
	}

	e.emitBatch = false
	return e.Input.Next()
}
