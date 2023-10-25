// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streamingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func checkLaggingNodesLoop(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ingestionJobID jobspb.JobID,
	stopper chan struct{},
) error {
	freq := func() time.Duration { return streamingccl.ReplanFrequency.Get(execCfg.SV()) }
	tick := time.NewTicker(freq())
	defer tick.Stop()
	done := ctx.Done()
	for {
		select {
		case <-stopper:
			return nil
		case <-done:
			return ctx.Err()
		case <-tick.C:
			if err := checkLaggingNodes(ctx, execCfg, ingestionJobID); err != nil {
				return err
			}
			tick.Reset(freq())
		}
	}
}

// checkLaggingNode returns an error if there exist a destination node lagging
// more than `InterNodeLag` time more than other nodes, and if that node isn't
// new to the system.
func checkLaggingNodes(
	ctx context.Context, execCfg *sql.ExecutorConfig, ingestionJobID jobspb.JobID,
) error {
	var executionDetails []frontierExecutionDetails
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		var err error
		executionDetails, err = getExecutionDetails(ctx, txn, ingestionJobID)
		return err
	}); err != nil {
		return err
	}

	laggingNode, minLagDifference := computeMinLagDifference(executionDetails)

	if streamingccl.InterNodeLag.Get(execCfg.SV()) > minLagDifference {
		ss, err := execCfg.NodesStatusServer.OptionalNodesStatusServer()
		if err != nil {
			return err
		}
		res, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return err
		}
		if !laggingNodeIsNew(laggingNode, res.Nodes) {
			return errors.Newf("Node %d is %s minutes behind the next node. Try replanning", laggingNode, minLagDifference.Minutes())
		}
	}
	return nil
}

// laggingNodeIsNew returns true if the lagging node is less than half as old as the average node lifetime
func laggingNodeIsNew(laggingNode base.SQLInstanceID, nodeInfo []statuspb.NodeStatus) bool {
	var combinedLifeSpan int64
	now := timeutil.Now()
	nodeToLifeSpan := make(map[base.SQLInstanceID]time.Duration)
	for _, node := range nodeInfo {
		lifeSpan := now.Sub(timeutil.Unix(0, node.StartedAt))
		nodeToLifeSpan[base.SQLInstanceID(node.Desc.NodeID)] = lifeSpan
		combinedLifeSpan += int64(lifeSpan)
	}
	avgLifeSpan := float64(combinedLifeSpan) / float64(len(nodeInfo))

	return float64(nodeToLifeSpan[laggingNode]) < avgLifeSpan/2
}

func computeMinLagDifference(
	executionDetails []frontierExecutionDetails,
) (base.SQLInstanceID, time.Duration) {
	oldestHWM := hlc.MaxTimestamp.GoTime()
	var laggingNode base.SQLInstanceID
	destNodeFrontier := make(map[base.SQLInstanceID]time.Time)
	if len(executionDetails) < 2 {
		return base.SQLInstanceID(0), 0
	}
	for _, detail := range executionDetails {
		frontier := detail.frontierTS.GoTime()
		if _, ok := destNodeFrontier[detail.destInstanceID]; !ok {
			destNodeFrontier[detail.destInstanceID] = frontier
		} else if destNodeFrontier[detail.destInstanceID].After(frontier) {
			destNodeFrontier[detail.destInstanceID] = frontier
		}
		if oldestHWM.After(frontier) {
			oldestHWM = frontier
			laggingNode = detail.destInstanceID
		}
	}

	minlagDifference := hlc.MaxTimestamp.GoTime().Sub(hlc.MinTimestamp.GoTime())
	for id, frontier := range destNodeFrontier {
		if id == laggingNode {
			continue
		}

		if diff := frontier.Sub(oldestHWM); diff < minlagDifference {
			minlagDifference = diff
		}
	}
	return laggingNode, minlagDifference
}
