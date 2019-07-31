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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/colrpc"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type callbackRemoteComponentCreator struct {
	newOutboxFn func(exec.Operator, []types.T, []distsqlpb.MetadataSource) (*colrpc.Outbox, error)
	newInboxFn  func(typs []types.T) (*colrpc.Inbox, error)
}

func (c callbackRemoteComponentCreator) newOutbox(
	input exec.Operator, typs []types.T, metadataSources []distsqlpb.MetadataSource,
) (*colrpc.Outbox, error) {
	return c.newOutboxFn(input, typs, metadataSources)
}

func (c callbackRemoteComponentCreator) newInbox(typs []types.T) (*colrpc.Inbox, error) {
	return c.newInboxFn(typs)
}

// TestDrainOnlyInputDAG is a regression test for #39137 to ensure
// that queries don't hang using the following scenario:
// Consider two nodes n1 and n2, an outbox (o1) and inbox (i1) on n1, and an
// arbitrary flow on n2.
// At the end of the query, o1 will drain its metadata sources when it
// encounters a zero-length batch from its input. If one of these metadata
// sources is i1, there is a possibility that a cycle is unknowingly created
// since i1 (as an example) could be pulling from a remote operator that itself
// is pulling from o1, who is at this moment attempting to drain i1.
// This test verifies that no metadata sources are added to an outbox that are
// not explicitly known to be in its input DAG.
func TestDrainOnlyInputDAG(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numInputTypesToOutbox       = 3
		numInputTypesToMaterializer = 1
	)
	// procs are the ProcessorSpecs that we pass in to create the flow. Note that
	// we order the inbox we don't want the outbox to drain first so that it is
	// created first.
	procs := []distsqlpb.ProcessorSpec{
		{
			// This is i1, the inbox who should be drained by the materializer, not
			// o1.
			Input: []distsqlpb.InputSyncSpec{
				{
					Streams:     []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_REMOTE, StreamID: 1}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			Output: []distsqlpb.OutputRouterSpec{
				{
					Type: distsqlpb.OutputRouterSpec_PASS_THROUGH,
					// We set up a local output so that the inbox is created independently.
					Streams: []distsqlpb.StreamEndpointSpec{
						{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: 2},
					},
				},
			},
		},
		// This is the root of the flow. The noop operator that will read from i1
		// and the materialier.
		{
			Input: []distsqlpb.InputSyncSpec{
				{
					Streams:     []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_LOCAL, StreamID: 2}},
					ColumnTypes: intCols(numInputTypesToMaterializer),
				},
			},
			Core: distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			Output: []distsqlpb.OutputRouterSpec{
				{
					Type:    distsqlpb.OutputRouterSpec_PASS_THROUGH,
					Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_SYNC_RESPONSE}},
				},
			},
		},
		{
			// Because creating a table reader is too complex (you need to create a
			// bunch of other state) we simulate this by creating a noop operator with
			// a remote input, which is treated as having no local edges during
			// topological processing.
			Input: []distsqlpb.InputSyncSpec{
				{
					Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_REMOTE}},
					// Use three Int columns as the type to be able to distinguish between
					// input DAGs when creating the inbox.
					ColumnTypes: intCols(numInputTypesToOutbox),
				},
			},
			Core: distsqlpb.ProcessorCoreUnion{Noop: &distsqlpb.NoopCoreSpec{}},
			// This is o1, the outbox that will drain metadata.
			Output: []distsqlpb.OutputRouterSpec{
				{
					Type:    distsqlpb.OutputRouterSpec_PASS_THROUGH,
					Streams: []distsqlpb.StreamEndpointSpec{{Type: distsqlpb.StreamEndpointSpec_REMOTE}},
				},
			},
		},
	}

	inboxToNumInputTypes := make(map[*colrpc.Inbox][]types.T)
	outboxCreated := false
	componentCreator := callbackRemoteComponentCreator{
		newOutboxFn: func(op exec.Operator, typs []types.T, sources []distsqlpb.MetadataSource) (*colrpc.Outbox, error) {
			require.False(t, outboxCreated)
			outboxCreated = true
			// Verify that there is only one metadata source: the inbox that is the
			// input to the noop operator. This is verified by first checking the
			// number of metadata sources and then that the input types are what we
			// expect from the input DAG.
			require.Len(t, sources, 1)
			require.Len(t, inboxToNumInputTypes[sources[0].(*colrpc.Inbox)], numInputTypesToOutbox)
			return colrpc.NewOutbox(op, typs, sources)
		},
		newInboxFn: func(typs []types.T) (*colrpc.Inbox, error) {
			inbox, err := colrpc.NewInbox(typs)
			inboxToNumInputTypes[inbox] = typs
			return inbox, err
		},
	}

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	ctx := context.Background()
	defer evalCtx.Stop(ctx)
	f := &Flow{FlowCtx: FlowCtx{EvalCtx: &evalCtx, NodeID: roachpb.NodeID(1)}}
	var wg sync.WaitGroup
	vfc := newVectorizedFlowCreator(
		&vectorizedFlowCreatorHelper{f: f},
		componentCreator,
		false, /* recordingStats */
		&wg,
		&RowBuffer{types: intCols(1)},
		nil, /* nodeDialer */
		distsqlpb.FlowID{},
	)

	acc := evalCtx.Mon.MakeBoundAccount()
	defer acc.Close(ctx)
	require.NoError(t, vfc.setupFlow(context.Background(), &f.FlowCtx, procs, &acc))

	// Verify that an outbox was actually created.
	require.True(t, outboxCreated)
}
