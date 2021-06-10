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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Test that we can send a setup flow request to the distSQLSrv after the
// FlowRegistry is draining.
func TestSetupFlowAfterDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// We'll create a server just so that we can extract its distsql ServerConfig,
	// so we can use it for a manually-built DistSQL Server below. Otherwise, too
	// much work to create that ServerConfig by hand.
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	cfg := s.DistSQLServer().(*ServerImpl).ServerConfig

	flowScheduler := flowinfra.NewFlowScheduler(cfg.AmbientContext, cfg.Stopper, cfg.Settings)
	flowScheduler.Init(cfg.Metrics)
	distSQLSrv := NewServer(
		ctx,
		cfg,
		flowScheduler,
	)
	distSQLSrv.flowRegistry.Drain(
		time.Duration(0) /* flowDrainWait */, time.Duration(0) /* minFlowDrainWait */, nil /* reporter */)

	// We create some flow; it doesn't matter what.
	req := execinfrapb.SetupFlowRequest{Version: execinfra.Version}
	req.Flow = execinfrapb.FlowSpec{
		Processors: []execinfrapb.ProcessorSpec{
			{
				Core: execinfrapb.ProcessorCoreUnion{Values: &execinfrapb.ValuesCoreSpec{}},
				Output: []execinfrapb.OutputRouterSpec{{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{StreamID: 1, Type: execinfrapb.StreamEndpointSpec_REMOTE}},
				}},
			},
			{
				Input: []execinfrapb.InputSyncSpec{{
					Type:    execinfrapb.InputSyncSpec_PARALLEL_UNORDERED,
					Streams: []execinfrapb.StreamEndpointSpec{{StreamID: 1, Type: execinfrapb.StreamEndpointSpec_REMOTE}},
				}},
				Core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
				Output: []execinfrapb.OutputRouterSpec{{
					Type:    execinfrapb.OutputRouterSpec_PASS_THROUGH,
					Streams: []execinfrapb.StreamEndpointSpec{{Type: execinfrapb.StreamEndpointSpec_SYNC_RESPONSE}},
				}},
			},
		},
	}

	// We expect to see an error in the response.
	resp, err := distSQLSrv.SetupFlow(ctx, &req)
	if err != nil {
		t.Fatal(err)
	}
	respErr := resp.Error.ErrorDetail(ctx)
	if !testutils.IsError(respErr, "the registry is draining") {
		t.Fatalf("expected draining err, got: %v", respErr)
	}
}
