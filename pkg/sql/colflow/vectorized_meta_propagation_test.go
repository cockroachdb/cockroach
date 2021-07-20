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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
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

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}

	nRows := 10
	nCols := 1
	typs := types.OneIntCol

	input := distsqlutils.NewRowBuffer(typs, randgen.MakeIntRows(nRows, nCols), distsqlutils.RowBufferArgs{})
	mtsSpec := execinfrapb.ProcessorCoreUnion{
		MetadataTestSender: &execinfrapb.MetadataTestSenderSpec{
			ID: uuid.MakeV4().String(),
		},
	}
	mts, err := execinfra.NewMetadataTestSender(
		&flowCtx,
		0,
		input,
		&execinfrapb.PostProcessSpec{},
		nil,
		uuid.MakeV4().String(),
	)
	if err != nil {
		t.Fatal(err)
	}

	col := colexec.NewBufferingColumnarizer(testAllocator, &flowCtx, 1, mts)
	noop := colexecop.NewNoop(col)
	mat := colexec.NewMaterializer(
		&flowCtx,
		2, /* processorID */
		colexecargs.OpWithMetaInfo{
			Root:            noop,
			MetadataSources: colexecop.MetadataSources{col},
		},
		typs,
	)

	mtr, err := execinfra.NewMetadataTestReceiver(
		&flowCtx,
		3,
		mat,
		&execinfrapb.PostProcessSpec{},
		nil,
		[]string{mtsSpec.MetadataTestSender.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	mtr.Start(ctx)

	rowCount, metaCount := 0, 0
	for {
		row, meta := mtr.Next()
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
