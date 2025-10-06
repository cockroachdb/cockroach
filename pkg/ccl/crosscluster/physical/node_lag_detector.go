// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var ErrNodeLagging = errors.New("node frontier too far behind other nodes")

// checkLaggingNode returns an error if there exists a destination node lagging
// more than maxAllowable lag behind the mean frontier of all destination nodes. This function
// assumes that all nodes have finished their initial scan (i.e. have a nonzero hwm).
func checkLaggingNodes(
	ctx context.Context, executionDetails []frontierExecutionDetails, maxAllowableLag time.Duration,
) error {
	if maxAllowableLag == 0 {
		return nil
	}
	laggingNode, meanLagDifference := computeMeanLagDifference(ctx, executionDetails)
	log.VEventf(ctx, 2, "computed mean lag diff: %d lagging node, difference %.2f", laggingNode, meanLagDifference.Minutes())
	if maxAllowableLag < meanLagDifference {
		return errors.Wrapf(ErrNodeLagging, "node %d is %.2f minutes behind the average frontier. Try replanning", laggingNode, meanLagDifference.Minutes())
	}
	return nil
}

// computeMeanLagDifference computes the difference between the mean frontier by
// node and the node with the lowest frontier.
func computeMeanLagDifference(
	ctx context.Context, executionDetails []frontierExecutionDetails,
) (base.SQLInstanceID, time.Duration) {
	lowestFrontier := hlc.MaxTimestamp.GoTime()

	// First find the frontier for each node.
	var laggingNode base.SQLInstanceID
	destNodeFrontier := make(map[base.SQLInstanceID]time.Time)
	for _, detail := range executionDetails {
		frontier := detail.frontierTS.GoTime()
		if _, ok := destNodeFrontier[detail.destInstanceID]; !ok {
			destNodeFrontier[detail.destInstanceID] = frontier
		} else if destNodeFrontier[detail.destInstanceID].After(frontier) {
			destNodeFrontier[detail.destInstanceID] = frontier
		}
		if lowestFrontier.After(frontier) {
			lowestFrontier = frontier
			laggingNode = detail.destInstanceID
		}
	}
	if len(destNodeFrontier) < 2 {
		// If there are fewer than 2 nodes in the frontier, we can't compare relative lag.
		return base.SQLInstanceID(0), 0
	}
	meanFrontier := getMeanFrontier(destNodeFrontier)
	log.VEventf(ctx, 2, "mean frontier: %s, lowest frontier %s", meanFrontier, lowestFrontier)

	return laggingNode, meanFrontier.Sub(lowestFrontier)
}

func getMeanFrontier(destNodeFrontier map[base.SQLInstanceID]time.Time) time.Time {
	var sum int64
	for _, frontier := range destNodeFrontier {
		sum += frontier.Unix()
	}
	frontierMean := timeutil.Unix(sum/int64(len(destNodeFrontier)), 0)
	return frontierMean
}
