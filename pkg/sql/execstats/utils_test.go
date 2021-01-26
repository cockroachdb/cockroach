// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execstats

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// AddComponentStats modifies TraceAnalyzer internal state to add stats for the
// processor/stream/flow specified in stats.ComponentID and the given node ID.
func (a *TraceAnalyzer) AddComponentStats(
	nodeID roachpb.NodeID, stats *execinfrapb.ComponentStats,
) {
	switch stats.Component.Type {
	case execinfrapb.ComponentID_PROCESSOR:
		processorStat := &processorStats{
			nodeID: nodeID,
			stats:  stats,
		}
		if a.FlowMetadata.processorStats == nil {
			a.FlowMetadata.processorStats = make(map[execinfrapb.ProcessorID]*processorStats)
		}
		a.FlowMetadata.processorStats[execinfrapb.ProcessorID(stats.Component.ID)] = processorStat
	case execinfrapb.ComponentID_STREAM:
		streamStat := &streamStats{
			originNodeID: nodeID,
			stats:        stats,
		}
		if a.FlowMetadata.streamStats == nil {
			a.FlowMetadata.streamStats = make(map[execinfrapb.StreamID]*streamStats)
		}
		a.FlowMetadata.streamStats[execinfrapb.StreamID(stats.Component.ID)] = streamStat
	default:
		flowStat := &flowStats{}
		flowStat.stats = append(flowStat.stats, stats)
		if a.FlowMetadata.flowStats == nil {
			a.FlowMetadata.flowStats = make(map[roachpb.NodeID]*flowStats)
		}
		a.FlowMetadata.flowStats[nodeID] = flowStat
	}
}
