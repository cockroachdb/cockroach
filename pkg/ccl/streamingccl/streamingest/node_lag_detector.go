// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamingest

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var ErrNodeLagging = errors.New("node frontier too far behind other nodes")

// checkLaggingNode returns an error if there exists a destination node lagging
// more than maxAllowable lag behind any other destination node. This function
// assumes that all nodes have finished their initial scan (i.e. have a nonzero hwm).
func checkLaggingNodes(
	ctx context.Context, executionDetails []frontierExecutionDetails, maxAllowableLag time.Duration,
) error {
	if maxAllowableLag == 0 {
		return nil
	}
	laggingNode, minLagDifference := computeMinLagDifference(executionDetails)
	log.VEventf(ctx, 2, "computed min lag diff: %d lagging node, difference %.2f", laggingNode, minLagDifference.Minutes())
	if maxAllowableLag < minLagDifference {
		return errors.Wrapf(ErrNodeLagging, "node %d is %.2f minutes behind the next node. Try replanning", laggingNode, minLagDifference.Minutes())
	}
	return nil
}

func computeMinLagDifference(
	executionDetails []frontierExecutionDetails,
) (base.SQLInstanceID, time.Duration) {
	oldestHWM := hlc.MaxTimestamp.GoTime()
	var laggingNode base.SQLInstanceID
	destNodeFrontier := make(map[base.SQLInstanceID]time.Time)
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
