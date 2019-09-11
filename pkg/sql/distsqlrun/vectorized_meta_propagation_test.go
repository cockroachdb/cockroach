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
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/distsql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execplan"
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

	flowCtx := distsql.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &distsql.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows := 10
	nCols := 1
	types := sqlbase.OneIntCol

	input := newRowBuffer(types, sqlbase.MakeIntRows(nRows, nCols), rowBufferArgs{})
	mtsSpec := distsqlpb.ProcessorCoreUnion{
		MetadataTestSender: &distsqlpb.MetadataTestSenderSpec{
			ID: uuid.MakeV4().String(),
		},
	}
	mts, err := newProcessor(ctx, &flowCtx, 0, &mtsSpec, &distsqlpb.PostProcessSpec{}, []distsql.RowSource{input}, []distsql.RowReceiver{nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mtsAsRowSource, ok := mts.(distsql.RowSource)
	if !ok {
		t.Fatal("MetadataTestSender is not a RowSource")
	}

	col, err := execplan.NewColumnarizer(ctx, &flowCtx, 1, mtsAsRowSource)
	if err != nil {
		t.Fatal(err)
	}

	noop := colexec.NewNoop(col)
	mat, err := distsql.NewMaterializer(
		&flowCtx,
		2, /* processorID */
		noop,
		types,
		&distsqlpb.PostProcessSpec{},
		nil, /* output */
		[]distsqlpb.MetadataSource{col},
		nil, /* outputStatsToTrace */
		nil, /* cancelFlow */
	)
	if err != nil {
		t.Fatal(err)
	}

	mtrSpec := distsqlpb.ProcessorCoreUnion{
		MetadataTestReceiver: &distsqlpb.MetadataTestReceiverSpec{
			SenderIDs: []string{mtsSpec.MetadataTestSender.ID},
		},
	}
	mtr, err := newProcessor(ctx, &flowCtx, 3, &mtrSpec, &distsqlpb.PostProcessSpec{}, []distsql.RowSource{distsql.RowSource(mat)}, []distsql.RowReceiver{nil}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mtrAsRowSource, ok := mtr.(distsql.RowSource)
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
