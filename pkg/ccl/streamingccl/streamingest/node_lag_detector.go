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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var ErrNodeLagging = errors.New("node frontier too far behind other nodes")

// checkLaggingNode returns an error if there exists a destination node lagging
// more than maxAllowable lag behind any other destination node. This function
// assumes that all nodes have finished their initial scan (i.e. have a nonzero hwm).
func checkLaggingNodes(
	ctx context.Context, nodeStats ingestStatsByNode, maxAllowableLag time.Duration,
) error {
	if maxAllowableLag == 0 {
		return nil
	}
	laggingNode, minLagDifference := computeMinLagDifference(nodeStats)
	log.VEventf(ctx, 2, "computed min lag diff: %d lagging node, difference %.2f", laggingNode, minLagDifference.Minutes())
	if maxAllowableLag < minLagDifference {
		return errors.Wrapf(ErrNodeLagging, "node %d is %.2f minutes behind the next node. Try replanning", laggingNode, minLagDifference.Minutes())
	}
	return nil
}

func computeMinLagDifference(
	ingestProcsStats ingestStatsByNode,
) (base.SQLInstanceID, time.Duration) {
	if len(ingestProcsStats) < 2 {
		// If there are fewer than 2 nodes with updates, we can't compare relative lag.
		return base.SQLInstanceID(0), 0
	}

	// Find the global Low Water mark
	globalLowWaterMark := hlc.MaxTimestamp
	var laggingNode base.SQLInstanceID
	for node, stats := range ingestProcsStats {
		stats.Lock()
		defer stats.Unlock()
		if globalLowWaterMark.After(stats.LowWaterMark) {
			globalLowWaterMark = stats.LowWaterMark
			laggingNode = node
		}
	}

	// Find the minimum difference between the global low water mark and the 2nd most behind node.
	minlagDifference := hlc.MaxTimestamp.GoTime().Sub(hlc.MinTimestamp.GoTime())
	globalLowWaterMarkGoTime := globalLowWaterMark.GoTime()
	for id, stats := range ingestProcsStats {
		if id == laggingNode {
			continue
		}

		if diff := stats.LowWaterMark.GoTime().Sub(globalLowWaterMarkGoTime); diff < minlagDifference {
			minlagDifference = diff
		}
	}
	return laggingNode, minlagDifference
}
