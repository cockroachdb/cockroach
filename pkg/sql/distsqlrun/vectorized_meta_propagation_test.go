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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// TestVectorizedMetaPropagation tests whether metadata is correctly propagated
// alongside columnar operators. It sets up the following "flow":
// RowSource -> metadataTestSender -> columnarizer -> noopOperator ->
// -> materializer -> metadataTestReceiver. Metadata propagation is hooked up
// manually from the columnarizer into the materializer similar to how it is
// done in setupVectorized.
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
	mtsOut := &RowBuffer{types: types}
	mts, err := newProcessor(ctx, &flowCtx, 0, &mtsSpec, &distsqlpb.PostProcessSpec{}, []RowSource{input}, []RowReceiver{mtsOut}, nil)
	if err != nil {
		t.Fatal(err)
	}

	col, err := newColumnarizer(&flowCtx, 1, mtsOut)
	if err != nil {
		t.Fatal(err)
	}

	noop := exec.NewNoop(col)
	matOut := &RowBuffer{types: types}
	mat, err := newMaterializer(&flowCtx, 2, noop, types, []int{0}, &distsqlpb.PostProcessSpec{}, matOut, []MetadataSource{col})
	if err != nil {
		t.Fatal(err)
	}

	mtrSpec := distsqlpb.ProcessorCoreUnion{
		MetadataTestReceiver: &distsqlpb.MetadataTestReceiverSpec{
			SenderIDs: []string{mtsSpec.MetadataTestSender.ID},
		},
	}
	mtrOut := &RowBuffer{types: types}
	mtr, err := newProcessor(ctx, &flowCtx, 3, &mtrSpec, &distsqlpb.PostProcessSpec{}, []RowSource{matOut}, []RowReceiver{mtrOut}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// TODO(yuzefovich): I thought that this commented code is the way to run
	// processors (an attempt to simulate what flows do), but it doesn't seem to
	// work. What am I missing?
	//
	//processors := []Processor{mts, mat, mtr}
	//var waitGroup sync.WaitGroup
	//for i := 0; i < len(processors); i++ {
	//	waitGroup.Add(1)
	//	go func(i int) {
	//		processors[i].Run(ctx)
	//		waitGroup.Done()
	//	}(i)
	//}
	//waitGroup.Wait()

	mts.Run(ctx)
	mat.Run(ctx)
	mtr.Run(ctx)

	rowCount := 0
	metaCount := 0
	for {
		row, meta := mtrOut.Next()
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
