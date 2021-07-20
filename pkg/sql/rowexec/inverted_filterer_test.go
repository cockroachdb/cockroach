// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func intToEncodedInvertedVal(v int64) []byte {
	return encoding.EncodeVarintAscending(nil, v)
}

func intSpanToEncodedSpan(start, end int64) inverted.SpanExpressionProto_Span {
	return inverted.SpanExpressionProto_Span{
		Start: intToEncodedInvertedVal(start),
		End:   intToEncodedInvertedVal(end),
	}
}

func TestInvertedFilterer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Tests do simple intersection and reordering of columns, to exercise the
	// plumbing in invertedFilterer -- all the heavy lifting for filtering is
	// done in helpers called by invertedFilterer that have their own
	// comprehensive tests. The intersection intersects the spans for the
	// inverted column values 1 and 3.
	// TODO(yuzefovich): add some unit tests that prefiltering works.
	testCases := []ProcessorTestCase{
		{
			Name: "simple-intersection-and-onexpr",
			Input: ProcessorTestCaseRows{
				// Inverted column is at index 0. Intersection is {23, 41, 50}.
				Rows: [][]interface{}{
					{1, 12},
					{1, 23},
					{1, 41},
					{1, 50},
					{3, 23},
					{3, 34},
					{3, 36},
					{3, 41},
					{3, 50},
					{3, 51},
				},
				Types: types.MakeIntCols(2),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{nil, 23},
					{nil, 41},
					{nil, 50},
				},
				Types: types.MakeIntCols(2),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				InvertedFilterer: &execinfrapb.InvertedFiltererSpec{},
			},
		},
		{
			Name: "inverted-is-middle-column",
			Input: ProcessorTestCaseRows{
				// Inverted column is at index 1. Intersection is {{12, 41}, {14, 43}}.
				Rows: [][]interface{}{
					{12, 1, 41},
					{13, 1, 42},
					{14, 1, 43},
					{12, 3, 41},
					{14, 3, 43},
				},
				Types: types.MakeIntCols(3),
			},
			Output: ProcessorTestCaseRows{
				Rows: [][]interface{}{
					{12, nil, 41},
					{14, nil, 43},
				},
				Types: types.MakeIntCols(3),
			},
			ProcessorCore: execinfrapb.ProcessorCoreUnion{
				InvertedFilterer: &execinfrapb.InvertedFiltererSpec{
					InvertedColIdx: 1,
				},
			},
		},
	}
	for i := range testCases {
		// Add the intersection InvertedExpr.
		testCases[i].ProcessorCore.InvertedFilterer.InvertedExpr = inverted.SpanExpressionProto{
			Node: inverted.SpanExpressionProto_Node{
				Operator: inverted.SetIntersection,
				Left: &inverted.SpanExpressionProto_Node{
					FactoredUnionSpans: []inverted.SpanExpressionProto_Span{
						intSpanToEncodedSpan(1, 2),
					},
				},
				Right: &inverted.SpanExpressionProto_Node{
					FactoredUnionSpans: []inverted.SpanExpressionProto_Span{
						intSpanToEncodedSpan(3, 4),
					},
				},
			},
		}
	}
	// Setup test environment.
	ctx := context.Background()
	server, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)
	testConfig := DefaultProcessorTestConfig()
	diskMonitor := execinfra.NewTestDiskMonitor(ctx, testConfig.FlowCtx.Cfg.Settings)
	defer diskMonitor.Stop(ctx)
	testConfig.FlowCtx.DiskMonitor = diskMonitor
	testConfig.FlowCtx.Txn = kv.NewTxn(ctx, server.DB(), server.NodeID())
	test := MakeProcessorTest(testConfig)

	// Run test.
	test.RunTestCases(ctx, t, testCases)
	test.Close(ctx)
}
