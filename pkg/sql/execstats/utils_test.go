// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats

import (
	"github.com/cockroachdb/cockroach/pkg/base"
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
		if m.processorStats == nil {
			m.processorStats = make(map[execinfrapb.ProcessorID]*execinfrapb.ComponentStats)
		}
		m.processorStats[execinfrapb.ProcessorID(stats.Component.ID)] = stats
	case execinfrapb.ComponentID_STREAM:
		streamStat := &streamStats{
			originSQLInstanceID: stats.Component.SQLInstanceID,
			stats:               stats,
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
