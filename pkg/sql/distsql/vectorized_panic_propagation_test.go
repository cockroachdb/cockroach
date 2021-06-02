// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsql

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colflow"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestNonVectorizedPanicDoesntHangServer verifies that propagating a non
// vectorized panic doesn't result in a hang as described in:
// https://github.com/cockroachdb/cockroach/issues/39779
func TestNonVectorizedPanicDoesntHangServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: cluster.MakeTestingClusterSettings()},
	}
	base := flowinfra.NewFlowBase(
		flowCtx,
		nil, /* flowReg */
		nil, /* rowSyncFlowConsumer */
		nil, /* batchSyncFlowConsumer */
		nil, /* localProcessors */
	)
	flow := colflow.NewVectorizedFlow(base)

	mat := colexec.NewMaterializer(
		&flowCtx,
		0, /* processorID */
		colexecargs.OpWithMetaInfo{Root: &colexecop.CallbackOperator{
			NextCb: func() coldata.Batch {
				panic("")
			},
		}},
		nil, /* typs */
	)

	ctx, _, err := base.Setup(ctx, nil, flowinfra.FuseAggressively)
	require.NoError(t, err)

	base.SetProcessors([]execinfra.Processor{mat})
	// This test specifically verifies that a flow doesn't get stuck in Wait for
	// asynchronous components that haven't been signaled to exit. To simulate
	// this we just create a mock startable.
	flow.AddStartable(
		flowinfra.StartableFn(func(ctx context.Context, wg *sync.WaitGroup, _ context.CancelFunc) {
			wg.Add(1)
			go func() {
				// Ensure context is canceled.
				<-ctx.Done()
				wg.Done()
			}()
		}),
	)

	require.Panics(t, func() { flow.Run(ctx, nil) })
}
