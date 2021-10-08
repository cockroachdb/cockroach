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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
)

// AddComponentStats modifies TraceAnalyzer internal state to add stats for the
// processor/stream/flow specified in stats.ComponentID and the given node ID.
func (a *TraceAnalyzer) AddComponentStats(stats *execinfrapb.ComponentStats) {
	a.FlowsMetadata.AddComponentStats(stats)
}

// AddComponentStats modifies FlowsMetadata to add stats for the
// processor/stream/flow specified in stats.ComponentID and the given node ID.
func (m *FlowsMetadata) AddComponentStats(stats *execinfrapb.ComponentStats) {
	switch stats.Component.Type {
	case execinfrapb.ComponentID_PROCESSOR:
		processorStat := &processorStats{
			nodeID: roachpb.NodeID(stats.Component.SQLInstanceID),
			stats:  stats,
		}
		if m.processorStats == nil {
			m.processorStats = make(map[execinfrapb.ProcessorID]*processorStats)
		}
		m.processorStats[execinfrapb.ProcessorID(stats.Component.ID)] = processorStat
	case execinfrapb.ComponentID_STREAM:
		streamStat := &streamStats{
			originNodeID: roachpb.NodeID(stats.Component.SQLInstanceID),
			stats:        stats,
		}
		if m.streamStats == nil {
			m.streamStats = make(map[execinfrapb.StreamID]*streamStats)
		}
		m.streamStats[execinfrapb.StreamID(stats.Component.ID)] = streamStat
	default:
		flowStat := &flowStats{}
		flowStat.stats = append(flowStat.stats, stats)
		if m.flowStats == nil {
			m.flowStats = make(map[base.SQLInstanceID]*flowStats)
		}
		m.flowStats[stats.Component.SQLInstanceID] = flowStat
	}
}
