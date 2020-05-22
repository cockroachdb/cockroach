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
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test that we can register send a sync flow to the distSQLSrv after the
// FlowRegistry is draining and the we can also clean that flow up (the flow
// will get a draining error). This used to crash.
func TestSyncFlowAfterDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	// We'll create a server just so that we can extract its distsql ServerConfig,
	// so we can use it for a manually-built DistSQL Server below. Otherwise, too
	// much work to create that ServerConfig by hand.
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	cfg := s.DistSQLServer().(*ServerImpl).ServerConfig

	distSQLSrv := NewServer(ctx, cfg)
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
					Type:    execinfrapb.InputSyncSpec_UNORDERED,
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

	types := make([]*types.T, 0)
	rb := distsqlutils.NewRowBuffer(types, nil /* rows */, distsqlutils.RowBufferArgs{})
	ctx, flow, err := distSQLSrv.SetupSyncFlow(ctx, &distSQLSrv.memMonitor, &req, rb)
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.Start(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	flow.Wait()
	_, meta := rb.Next()
	if meta == nil {
		t.Fatal("expected draining err, got no meta")
	}
	if !testutils.IsError(meta.Err, "the registry is draining") {
		t.Fatalf("expected draining err, got: %v", meta.Err)
	}
	flow.Cleanup(ctx)
}
