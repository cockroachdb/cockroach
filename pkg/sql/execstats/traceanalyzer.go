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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

type processorStats struct {
	// TODO(radu): this field redundant with stats.Component.SQLInstanceID.
	nodeID roachpb.NodeID
	stats  *execinfrapb.ComponentStats
}

type streamStats struct {
	// TODO(radu): this field redundant with stats.Component.SQLInstanceID.
	originNodeID      roachpb.NodeID
	destinationNodeID roachpb.NodeID
	stats             *execinfrapb.ComponentStats
}

type flowStats struct {
	stats []*execinfrapb.ComponentStats
}

// FlowsMetadata contains metadata extracted from flows that comprise a single
// physical plan. This information is stored in sql.flowInfo and is analyzed by
// TraceAnalyzer.
type FlowsMetadata struct {
	// flowID is the FlowID of the flows belonging to the physical plan. Note that
	// the same FlowID is used across multiple flows in the same query.
	flowID execinfrapb.FlowID
	// processorStats maps a processor ID to stats associated with this
	// processor extracted from a trace as well as some metadata. Note that it
	// is possible for the processorStats to have nil stats, which indicates
	// that no stats were found for the given processor in the trace.
	processorStats map[execinfrapb.ProcessorID]*processorStats
	// streamStats maps a stream ID to stats associated with this stream
	// extracted from a trace as well as some metadata. Note that it is possible
	// for the streamStats to have nil stats, which indicates that no stats were
	// found for the given stream in the trace.
	streamStats map[execinfrapb.StreamID]*streamStats
	// flowStats maps a node ID to flow level stats extracted from a trace. Note
	// that the key is not a FlowID because the same FlowID is used across
	// nodes.
	flowStats map[base.SQLInstanceID]*flowStats
}

// NewFlowsMetadata creates a FlowsMetadata for the given physical plan
// information.
func NewFlowsMetadata(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) *FlowsMetadata {
	a := &FlowsMetadata{
		processorStats: make(map[execinfrapb.ProcessorID]*processorStats),
		streamStats:    make(map[execinfrapb.StreamID]*streamStats),
		flowStats:      make(map[base.SQLInstanceID]*flowStats),
	}

	// Annotate the maps with physical plan information.
	for nodeID, flow := range flows {
		if a.flowID.IsUnset() {
			a.flowID = flow.FlowID
		} else if util.CrdbTestBuild && !a.flowID.Equal(flow.FlowID) {
			panic(
				errors.AssertionFailedf(
					"expected the same FlowID to be used for all flows. UUID of first flow: %v, UUID of flow on node %s: %v",
					a.flowID, nodeID, flow.FlowID),
			)
		}
		a.flowStats[base.SQLInstanceID(nodeID)] = &flowStats{}
		for _, proc := range flow.Processors {
			a.processorStats[execinfrapb.ProcessorID(proc.ProcessorID)] = &processorStats{nodeID: nodeID}
			for _, output := range proc.Output {
				for _, stream := range output.Streams {
					a.streamStats[stream.StreamID] = &streamStats{
						originNodeID:      nodeID,
						destinationNodeID: stream.TargetNodeID,
					}
				}
			}
		}
	}

	return a
}

// NodeLevelStats returns all the flow level stats that correspond to the given
// traces and flow metadata.
// TODO(asubiotto): Flatten this struct, we're currently allocating a map per
//  stat.
type NodeLevelStats struct {
	NetworkBytesSentGroupedByNode map[base.SQLInstanceID]int64
	MaxMemoryUsageGroupedByNode   map[base.SQLInstanceID]int64
	MaxDiskUsageGroupedByNode     map[base.SQLInstanceID]int64
	KVBytesReadGroupedByNode      map[base.SQLInstanceID]int64
	KVRowsReadGroupedByNode       map[base.SQLInstanceID]int64
	KVTimeGroupedByNode           map[base.SQLInstanceID]time.Duration
	NetworkMessagesGroupedByNode  map[base.SQLInstanceID]int64
	ContentionTimeGroupedByNode   map[base.SQLInstanceID]time.Duration
}

// QueryLevelStats returns all the query level stats that correspond to the
// given traces and flow metadata.
// NOTE: When adding fields to this struct, be sure to update Accumulate.
type QueryLevelStats struct {
	NetworkBytesSent int64
	MaxMemUsage      int64
	MaxDiskUsage     int64
	KVBytesRead      int64
	KVRowsRead       int64
	KVTime           time.Duration
	NetworkMessages  int64
	ContentionTime   time.Duration
	Regions          []string
}

// Accumulate accumulates other's stats into the receiver.
func (s *QueryLevelStats) Accumulate(other QueryLevelStats) {
	s.NetworkBytesSent += other.NetworkBytesSent
	if other.MaxMemUsage > s.MaxMemUsage {
		s.MaxMemUsage = other.MaxMemUsage
	}
	if other.MaxDiskUsage > s.MaxDiskUsage {
		s.MaxDiskUsage = other.MaxDiskUsage
	}
	s.KVBytesRead += other.KVBytesRead
	s.KVRowsRead += other.KVRowsRead
	s.KVTime += other.KVTime
	s.NetworkMessages += other.NetworkMessages
	s.ContentionTime += other.ContentionTime
	s.Regions = util.CombineUniqueString(s.Regions, other.Regions)
}

// TraceAnalyzer is a struct that helps calculate top-level statistics from a
// flow metadata and an accompanying trace of the flows' execution.
type TraceAnalyzer struct {
	*FlowsMetadata
	nodeLevelStats  NodeLevelStats
	queryLevelStats QueryLevelStats
}

// NewTraceAnalyzer creates a TraceAnalyzer with the corresponding physical
// plan. Call AddTrace to calculate meaningful stats.
func NewTraceAnalyzer(flowsMetadata *FlowsMetadata) *TraceAnalyzer {
	return &TraceAnalyzer{FlowsMetadata: flowsMetadata}
}

// AddTrace adds the stats from the given trace to the TraceAnalyzer.
//
// If makeDeterministic is set, statistics that can vary from run to run are set
// to fixed values; see ComponentStats.MakeDeterministic.
func (a *TraceAnalyzer) AddTrace(trace []tracingpb.RecordedSpan, makeDeterministic bool) error {
	m := execinfrapb.ExtractStatsFromSpans(trace, makeDeterministic)
	// Annotate the maps with stats extracted from the trace.
	for component, componentStats := range m {
		if !component.FlowID.Equal(a.flowID) {
			// This component belongs to a flow we do not care about. Note that we use
			// a bytes comparison because the UUID Equals method only returns true iff
			// the UUIDs are the same object.
			continue
		}
		switch component.Type {
		case execinfrapb.ComponentID_PROCESSOR:
			id := component.ID
			processorStats := a.processorStats[execinfrapb.ProcessorID(id)]
			if processorStats == nil {
				return errors.Errorf("trace has span for processor %d but the processor does not exist in the physical plan", id)
			}
			processorStats.stats = componentStats

		case execinfrapb.ComponentID_STREAM:
			id := component.ID
			streamStats := a.streamStats[execinfrapb.StreamID(id)]
			if streamStats == nil {
				return errors.Errorf("trace has span for stream %d but the stream does not exist in the physical plan", id)
			}
			streamStats.stats = componentStats

		case execinfrapb.ComponentID_FLOW:
			flowStats := a.flowStats[component.SQLInstanceID]
			if flowStats == nil {
				return errors.Errorf(
					"trace has span for flow %s on node %s but the flow does not exist in the physical plan",
					component.FlowID,
					component.SQLInstanceID,
				)
			}
			flowStats.stats = append(flowStats.stats, componentStats)
		}
	}

	return nil
}

// ProcessStats calculates node level and query level stats for the trace and
// stores them in TraceAnalyzer. If errors occur while calculating stats,
// ProcessStats returns the combined errors to the caller but continues
// calculating other stats.
func (a *TraceAnalyzer) ProcessStats() error {
	// Process node level stats.
	a.nodeLevelStats = NodeLevelStats{
		NetworkBytesSentGroupedByNode: make(map[base.SQLInstanceID]int64),
		MaxMemoryUsageGroupedByNode:   make(map[base.SQLInstanceID]int64),
		MaxDiskUsageGroupedByNode:     make(map[base.SQLInstanceID]int64),
		KVBytesReadGroupedByNode:      make(map[base.SQLInstanceID]int64),
		KVRowsReadGroupedByNode:       make(map[base.SQLInstanceID]int64),
		KVTimeGroupedByNode:           make(map[base.SQLInstanceID]time.Duration),
		NetworkMessagesGroupedByNode:  make(map[base.SQLInstanceID]int64),
		ContentionTimeGroupedByNode:   make(map[base.SQLInstanceID]time.Duration),
	}
	var errs error

	// Process processorStats.
	for _, stats := range a.processorStats {
		if stats.stats == nil {
			continue
		}
		instanceID := base.SQLInstanceID(stats.nodeID)
		a.nodeLevelStats.KVBytesReadGroupedByNode[instanceID] += int64(stats.stats.KV.BytesRead.Value())
		a.nodeLevelStats.KVRowsReadGroupedByNode[instanceID] += int64(stats.stats.KV.TuplesRead.Value())
		a.nodeLevelStats.KVTimeGroupedByNode[instanceID] += stats.stats.KV.KVTime.Value()
		a.nodeLevelStats.ContentionTimeGroupedByNode[instanceID] += stats.stats.KV.ContentionTime.Value()
	}

	// Process streamStats.
	for _, stats := range a.streamStats {
		if stats.stats == nil {
			continue
		}
		originInstanceID := base.SQLInstanceID(stats.originNodeID)

		// Set networkBytesSentGroupedByNode.
		bytes, err := getNetworkBytesFromComponentStats(stats.stats)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating network bytes sent"))
		} else {
			a.nodeLevelStats.NetworkBytesSentGroupedByNode[originInstanceID] += bytes
		}

		// The row execution flow attaches flow stats to a stream stat with the
		// last outbox, so we need to check stream stats for max memory and disk
		// usage.
		// TODO(cathymw): maxMemUsage shouldn't be attached to span stats that
		// are associated with streams, since it's a flow level stat. However,
		// due to the row exec engine infrastructure, it is too complicated to
		// attach this to a flow level span. If the row exec engine gets
		// removed, getting maxMemUsage from streamStats should be removed as
		// well.
		if stats.stats.FlowStats.MaxMemUsage.HasValue() {
			memUsage := int64(stats.stats.FlowStats.MaxMemUsage.Value())
			if memUsage > a.nodeLevelStats.MaxMemoryUsageGroupedByNode[originInstanceID] {
				a.nodeLevelStats.MaxMemoryUsageGroupedByNode[originInstanceID] = memUsage
			}
		}
		if stats.stats.FlowStats.MaxDiskUsage.HasValue() {
			if diskUsage := int64(stats.stats.FlowStats.MaxDiskUsage.Value()); diskUsage > a.nodeLevelStats.MaxDiskUsageGroupedByNode[originInstanceID] {
				a.nodeLevelStats.MaxDiskUsageGroupedByNode[originInstanceID] = diskUsage
			}
		}

		numMessages, err := getNumNetworkMessagesFromComponentsStats(stats.stats)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating number of network messages"))
		} else {
			a.nodeLevelStats.NetworkMessagesGroupedByNode[originInstanceID] += numMessages
		}
	}

	// Process flowStats.
	for instanceID, stats := range a.flowStats {
		if stats.stats == nil {
			continue
		}

		for _, v := range stats.stats {
			if v.FlowStats.MaxMemUsage.HasValue() {
				if memUsage := int64(v.FlowStats.MaxMemUsage.Value()); memUsage > a.nodeLevelStats.MaxMemoryUsageGroupedByNode[instanceID] {
					a.nodeLevelStats.MaxMemoryUsageGroupedByNode[instanceID] = memUsage
				}
			}
			if v.FlowStats.MaxDiskUsage.HasValue() {
				if diskUsage := int64(v.FlowStats.MaxDiskUsage.Value()); diskUsage > a.nodeLevelStats.MaxDiskUsageGroupedByNode[instanceID] {
					a.nodeLevelStats.MaxDiskUsageGroupedByNode[instanceID] = diskUsage
				}

			}
		}
	}

	// Process query level stats.
	a.queryLevelStats = QueryLevelStats{}

	for _, bytesSentByNode := range a.nodeLevelStats.NetworkBytesSentGroupedByNode {
		a.queryLevelStats.NetworkBytesSent += bytesSentByNode
	}

	for _, maxMemUsage := range a.nodeLevelStats.MaxMemoryUsageGroupedByNode {
		if maxMemUsage > a.queryLevelStats.MaxMemUsage {
			a.queryLevelStats.MaxMemUsage = maxMemUsage
		}
	}

	for _, maxDiskUsage := range a.nodeLevelStats.MaxDiskUsageGroupedByNode {
		if maxDiskUsage > a.queryLevelStats.MaxDiskUsage {
			a.queryLevelStats.MaxDiskUsage = maxDiskUsage
		}
	}

	for _, kvBytesRead := range a.nodeLevelStats.KVBytesReadGroupedByNode {
		a.queryLevelStats.KVBytesRead += kvBytesRead
	}

	for _, kvRowsRead := range a.nodeLevelStats.KVRowsReadGroupedByNode {
		a.queryLevelStats.KVRowsRead += kvRowsRead
	}

	for _, kvTime := range a.nodeLevelStats.KVTimeGroupedByNode {
		a.queryLevelStats.KVTime += kvTime
	}

	for _, networkMessages := range a.nodeLevelStats.NetworkMessagesGroupedByNode {
		a.queryLevelStats.NetworkMessages += networkMessages
	}

	for _, contentionTime := range a.nodeLevelStats.ContentionTimeGroupedByNode {
		a.queryLevelStats.ContentionTime += contentionTime
	}
	return errs
}

func getNetworkBytesFromComponentStats(v *execinfrapb.ComponentStats) (int64, error) {
	// We expect exactly one of BytesReceived and BytesSent to be set. It may
	// seem like we are double-counting everything (from both the send and the
	// receive side) but in practice only one side of each stream presents
	// statistics (specifically the sending side in the row engine, and the
	// receiving side in the vectorized engine).
	if v.NetRx.BytesReceived.HasValue() {
		if v.NetTx.BytesSent.HasValue() {
			return 0, errors.Errorf("could not get network bytes; both BytesReceived and BytesSent are set")
		}
		return int64(v.NetRx.BytesReceived.Value()), nil
	}
	if v.NetTx.BytesSent.HasValue() {
		return int64(v.NetTx.BytesSent.Value()), nil
	}
	// If neither BytesReceived or BytesSent is set, this ComponentStat belongs to
	// a local component, e.g. a local hashrouter output.
	return 0, nil
}

func getNumNetworkMessagesFromComponentsStats(v *execinfrapb.ComponentStats) (int64, error) {
	// We expect exactly one of MessagesReceived and MessagesSent to be set. It
	// may seem like we are double-counting everything (from both the send and
	// the receive side) but in practice only one side of each stream presents
	// statistics (specifically the sending side in the row engine, and the
	// receiving side in the vectorized engine).
	if v.NetRx.MessagesReceived.HasValue() {
		if v.NetTx.MessagesSent.HasValue() {
			return 0, errors.Errorf("could not get network messages; both MessagesReceived and MessagesSent are set")
		}
		return int64(v.NetRx.MessagesReceived.Value()), nil
	}
	if v.NetTx.MessagesSent.HasValue() {
		return int64(v.NetTx.MessagesSent.Value()), nil
	}
	// If neither BytesReceived or BytesSent is set, this ComponentStat belongs to
	// a local component, e.g. a local hashrouter output.
	return 0, nil
}

// GetNodeLevelStats returns the node level stats calculated and stored in the
// TraceAnalyzer.
func (a *TraceAnalyzer) GetNodeLevelStats() NodeLevelStats {
	return a.nodeLevelStats
}

// GetQueryLevelStats returns the query level stats calculated and stored in
// TraceAnalyzer.
func (a *TraceAnalyzer) GetQueryLevelStats() QueryLevelStats {
	return a.queryLevelStats
}

// GetQueryLevelStats returns all the top-level stats in a QueryLevelStats
// struct. GetQueryLevelStats tries to process as many stats as possible. If
// errors occur while processing stats, GetQueryLevelStats returns the combined
// errors to the caller but continues calculating other stats.
func GetQueryLevelStats(
	trace []tracingpb.RecordedSpan, deterministicExplainAnalyze bool, flowsMetadata []*FlowsMetadata,
) (QueryLevelStats, error) {
	var queryLevelStats QueryLevelStats
	var errs error
	for _, metadata := range flowsMetadata {
		analyzer := NewTraceAnalyzer(metadata)
		if err := analyzer.AddTrace(trace, deterministicExplainAnalyze); err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error analyzing trace statistics"))
			continue
		}

		if err := analyzer.ProcessStats(); err != nil {
			errs = errors.CombineErrors(errs, err)
			continue
		}
		queryLevelStats.Accumulate(analyzer.GetQueryLevelStats())
	}
	return queryLevelStats, errs
}
