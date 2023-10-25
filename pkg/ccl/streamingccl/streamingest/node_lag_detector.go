// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var ErrNodeLagging = errors.New("node frontier too far behind other nodes")

// checkLaggingNode returns an error if there exists a destination node lagging
// more than maxAllowable lag behind any other destination node, and if that
// node isn't new to the system.
func checkLaggingNodes(
	ctx context.Context,
	executionDetails []frontierExecutionDetails,
	maxAllowableLag time.Duration,
	ss serverpb.NodesStatusServer,
) error {
	if maxAllowableLag == 0 {
		return nil
	}
	laggingNode, minLagDifference := computeMinLagDifference(executionDetails)
	if maxAllowableLag < minLagDifference {
		// Only send the ListNodes RPC if a node is actually lagging behind.
		res, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return err
		}
		if !laggingNodeIsNew(laggingNode, maxAllowableLag, res.Nodes) {
			return errors.Wrapf(ErrNodeLagging, "node %d is %.2f minutes behind the next node. Try replanning", laggingNode, minLagDifference.Minutes())
		}
		log.Infof(ctx, "node %d has a lag %.2f minutes behind the next node but is also new", laggingNode, minLagDifference.Minutes())
	}
	return nil
}

// laggingNodeIsNew returns true if the lagging node is less than half as old as
// the average destination node lifetime or younger than the maxAllowableLag.
//
// This function assumes that maxAllowableLag is nonzero.
func laggingNodeIsNew(
	laggingNode base.SQLInstanceID, maxAllowableLag time.Duration, nodeInfo []statuspb.NodeStatus,
) bool {
	var combinedLifeSpan int64
	now := timeutil.Now()
	nodeToLifeSpan := make(map[base.SQLInstanceID]time.Duration)
	for _, node := range nodeInfo {
		lifeSpan := now.Sub(timeutil.Unix(0, node.StartedAt))
		nodeToLifeSpan[base.SQLInstanceID(node.Desc.NodeID)] = lifeSpan
		combinedLifeSpan += int64(lifeSpan)
	}
	avgLifeSpan := float64(combinedLifeSpan) / float64(len(nodeInfo))

	if nodeToLifeSpan[laggingNode] < maxAllowableLag {
		return true
	}

	return float64(nodeToLifeSpan[laggingNode]) < avgLifeSpan/2
}

func computeMinLagDifference(
	executionDetails []frontierExecutionDetails,
) (base.SQLInstanceID, time.Duration) {
	oldestHWM := hlc.MaxTimestamp.GoTime()
	var laggingNode base.SQLInstanceID
	destNodeFrontier := make(map[base.SQLInstanceID]time.Time)
	for _, detail := range executionDetails {
		if detail.frontierTS.IsEmpty() {
			// The lag check does not apply to a span conducting an initial scan.
			continue
		}
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
	if len(destNodeFrontier) < 2 {
		// If there are fewer than 2 nodes in the frontier, we can't compare relative lag.
		return base.SQLInstanceID(0), 0
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
