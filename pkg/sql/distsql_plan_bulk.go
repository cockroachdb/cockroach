// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

// SetupAllNodesPlanning creates a planCtx and sets up the planCtx.NodeStatuses
// map for all nodes. It returns all nodes that can be used for planning.
func (dsp *DistSQLPlanner) SetupAllNodesPlanning(
	ctx context.Context, evalCtx *extendedEvalContext, execCfg *ExecutorConfig,
) (*PlanningCtx, []base.SQLInstanceID, error) {
	distribute := evalCtx.Codec.ForSystemTenant()
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* planner */, nil /* txn */, distribute)

	ss, err := execCfg.NodesStatusServer.OptionalNodesStatusServer(47900)
	if err != nil {
		return planCtx, []base.SQLInstanceID{dsp.gatewaySQLInstanceID}, nil //nolint:returnerrcheck
	}
	resp, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return nil, nil, err
	}
	// Because we're not going through the normal pathways, we have to set up the
	// planCtx.NodeStatuses map ourselves. CheckInstanceHealthAndVersion() will
	// populate it.
	for _, node := range resp.Nodes {
		_ /* NodeStatus */ = dsp.CheckInstanceHealthAndVersion(planCtx, base.SQLInstanceID(node.Desc.NodeID))
	}
	nodes := make([]base.SQLInstanceID, 0, len(planCtx.NodeStatuses))
	for nodeID, status := range planCtx.NodeStatuses {
		if status == NodeOK {
			nodes = append(nodes, nodeID)
		}
	}
	// Shuffle node order so that multiple IMPORTs done in parallel will not
	// identically schedule CSV reading. For example, if there are 3 nodes and 4
	// files, the first node will get 2 files while the other nodes will each get 1
	// file. Shuffling will make that first node random instead of always the same.
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	return planCtx, nodes, nil
}
