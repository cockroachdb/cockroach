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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestVectorizedMetaPropagation tests whether metadata is correctly propagated
// alongside columnar operators. It sets up the following "flow":
// RowSource -> metadataTestSender -> columnarizer -> noopOperator ->
// -> materializer -> metadataTestReceiver. Metadata propagation is hooked up
// manually from the columnarizer into the materializer similar to how it is
// done in setupVectorizedFlow.
func TestVectorizedMetaPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := FlowCtx{
		EvalCtx:  &evalCtx,
		Settings: cluster.MakeTestingClusterSettings(),
	}

	nRows := 10
	nCols := 1
	types := sqlbase.OneIntCol

	input := NewRowBuffer(types, sqlbase.MakeIntRows(nRows, nCols), RowBufferArgs{})
	mtsSpec := distsqlpb.ProcessorCoreUnion{
		MetadataTestSender: &distsqlpb.MetadataTestSenderSpec{
			ID: uuid.MakeV4().String(),
		},
	}
	mts, err := newProcessor(ctx, &flowCtx, 0, &mtsSpec, &distsqlpb.PostProcessSpec{}, []RowSource{input}, []RowReceiver{nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mtsAsRowSource, ok := mts.(RowSource)
	if !ok {
		t.Fatal("MetadataTestSender is not a RowSource")
	}

	col, err := newColumnarizer(ctx, &flowCtx, 1, mtsAsRowSource)
	if err != nil {
		t.Fatal(err)
	}

	noop := exec.NewNoop(col)
	mat, err := newMaterializer(
		&flowCtx,
		2, /* processorID */
		noop,
		types,
		[]int{0},
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		[]distsqlpb.MetadataSource{col},
		nil, /* outputStatsToTrace */
	)
	if err != nil {
		t.Fatal(err)
	}

	mtrSpec := distsqlpb.ProcessorCoreUnion{
		MetadataTestReceiver: &distsqlpb.MetadataTestReceiverSpec{
			SenderIDs: []string{mtsSpec.MetadataTestSender.ID},
		},
	}
	mtr, err := newProcessor(ctx, &flowCtx, 3, &mtrSpec, &distsqlpb.PostProcessSpec{}, []RowSource{RowSource(mat)}, []RowReceiver{nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mtrAsRowSource, ok := mtr.(RowSource)
	if !ok {
		t.Fatal("MetadataTestReceiver is not a RowSource")
	}
	mtrAsRowSource.Start(ctx)

	rowCount, metaCount := 0, 0
	for {
		row, meta := mtrAsRowSource.Next()
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
	if metaCount != nRows+1 {
		// metadataTestSender sends a meta after each row plus an additional one to
		// indicate the last meta.
		t.Fatalf("expected %d meta but %d received", nRows+1, metaCount)
	}
}
