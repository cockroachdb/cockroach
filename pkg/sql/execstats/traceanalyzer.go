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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

type processorStats struct {
	nodeID roachpb.NodeID
	stats  *execinfrapb.ComponentStats
}

type streamStats struct {
	originNodeID      roachpb.NodeID
	destinationNodeID roachpb.NodeID
	stats             *execinfrapb.ComponentStats
}

type flowStats struct {
	stats []*execinfrapb.ComponentStats
}

// FlowMetadata contains metadata extracted from flows. This information is stored
// in sql.flowInfo and is analyzed by TraceAnalyzer.
type FlowMetadata struct {
	// processorStats maps a processor ID to stats associated with this processor
	// extracted from a trace as well as some metadata. Note that it is possible
	// for the processorStats to have nil stats, which indicates that no stats
	// were found for the given processor in the trace.
	processorStats map[execinfrapb.ProcessorID]*processorStats
	// streamStats maps a stream ID to stats associated with this stream extracted
	// from a trace as well as some metadata. Note that is is possible for the
	// streamStats to have nil stats, which indicates that no stats were found
	// for the given stream in the trace.
	streamStats map[execinfrapb.StreamID]*streamStats
	// flowStats maps a flow ID to flow level stats extracted from a trace.
	flowStats map[execinfrapb.FlowID]*flowStats
}

// NewFlowMetadata creates a FlowMetadata with the given physical plan information.
func NewFlowMetadata(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) *FlowMetadata {
	a := &FlowMetadata{
		processorStats: make(map[execinfrapb.ProcessorID]*processorStats),
		streamStats:    make(map[execinfrapb.StreamID]*streamStats),
		flowStats:      make(map[execinfrapb.FlowID]*flowStats),
	}

	// Annotate the maps with physical plan information.
	for nodeID, flow := range flows {
		a.flowStats[flow.FlowID] = &flowStats{}
		for _, proc := range flow.Processors {
			a.processorStats[execinfrapb.ProcessorID(proc.ProcessorID)] = &processorStats{nodeID: nodeID}
			for _, output := range proc.Output {
				for _, stream := range output.Streams {
					if stream.Type == execinfrapb.StreamEndpointSpec_REMOTE {
						a.streamStats[stream.StreamID] = &streamStats{
							originNodeID:      nodeID,
							destinationNodeID: stream.TargetNodeID,
						}
					}
				}
			}
		}
	}

	return a
}

// TraceAnalyzer is a struct that helps calculate top-level statistics from a
// flow metadata and an accompanying trace of the flows' execution.
// Example usage:
//     analyzer := MakeTraceAnalyzer(flowMetadata)
//     analyzer.AddTrace(trace)
//     bytesGroupedByNode, err := analyzer.GetNetworkBytesSent()
type TraceAnalyzer struct {
	*FlowMetadata
}

// MakeTraceAnalyzer creates a TraceAnalyzer with the corresponding physical
// plan. Call AddTrace to calculate meaningful stats.
func MakeTraceAnalyzer(flowMetadata *FlowMetadata) TraceAnalyzer {
	a := TraceAnalyzer{
		FlowMetadata: flowMetadata,
	}

	return a
}

// AddTrace adds the stats from the given trace to the TraceAnalyzer.
//
// If makeDeterministic is set, statistics that can vary from run to run are set
// to fixed values; see ComponentStats.MakeDeterministic.
func (a TraceAnalyzer) AddTrace(trace []tracingpb.RecordedSpan, makeDeterministic bool) error {
	// Annotate the maps with stats extracted from the trace.
	for _, span := range trace {
		if span.Stats == nil {
			// No stats to unmarshal (e.g. noop processors at time of writing).
			continue
		}

		var stats execinfrapb.ComponentStats
		if err := types.UnmarshalAny(span.Stats, &stats); err != nil {
			return errors.Wrap(err, "unable to unmarshal in TraceAnalyzer")
		}
		if makeDeterministic {
			stats.MakeDeterministic()
		}

		// Get the processor or stream id for this span. If neither exists, this
		// span doesn't belong to a processor or stream.
		if pid, ok := span.Tags[execinfrapb.ProcessorIDTagKey]; ok {
			stringID := pid
			id, err := strconv.Atoi(stringID)
			if err != nil {
				return errors.Wrap(err, "unable to convert span processor ID tag in TraceAnalyzer")
			}
			processorStats := a.processorStats[execinfrapb.ProcessorID(id)]
			if processorStats == nil {
				return errors.Errorf("trace has span for processor %d but the processor does not exist in the physical plan", id)
			}
			processorStats.stats = &stats
		} else if sid, ok := span.Tags[execinfrapb.StreamIDTagKey]; ok {
			stringID := sid
			id, err := strconv.Atoi(stringID)
			if err != nil {
				return errors.Wrap(err, "unable to convert span processor ID tag in TraceAnalyzer")
			}
			streamStats := a.streamStats[execinfrapb.StreamID(id)]
			if streamStats == nil {
				return errors.Errorf("trace has span for stream %d but the stream does not exist in the physical plan", id)
			}
			streamStats.stats = &stats
		} else if fid, ok := span.Tags[execinfrapb.FlowIDTagKey]; ok {
			uuid, err := uuid.FromString(fid)
			if err != nil {
				return errors.Wrap(err, "unable to convert span flow ID tag in TraceAnalyzer")
			}
			flowStats := a.flowStats[execinfrapb.FlowID{UUID: uuid}]
			if flowStats == nil {
				return errors.Errorf("trace has span for flow %s but the flow does not exist in the physical plan", fid)
			}
			flowStats.stats = append(flowStats.stats, &stats)
		}
	}

	return nil
}

func getNetworkBytesFromComponentStats(v *execinfrapb.ComponentStats) (int64, error) {
	// We expect exactly one of BytesReceived and BytesSent to be set.
	// It may seem like we are double-counting everything (from both the send and
	// the receive side) but in practice only one side of each stream presents
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
	return 0, errors.Errorf("could not get network bytes; neither BytesReceived and BytesSent is set")
}

// GetNetworkBytesSent returns the number of bytes sent over the network the
// trace reports, grouped by NodeID.
func (a TraceAnalyzer) GetNetworkBytesSent() (map[roachpb.NodeID]int64, error) {
	result := make(map[roachpb.NodeID]int64)
	for _, stats := range a.streamStats {
		if stats.stats == nil {
			continue
		}
		bytes, err := getNetworkBytesFromComponentStats(stats.stats)
		if err != nil {
			return nil, err
		}
		result[stats.originNodeID] += bytes
	}
	return result, nil
}

func getMaxMemoryUsageFromFlowStats(flowStats map[execinfrapb.FlowID]*flowStats) int64 {
	var maxMemUsage int64
	for _, stats := range flowStats {
		if stats.stats == nil {
			continue
		}
		for _, v := range stats.stats {
			if memUsage := int64(v.FlowStats.MaxMemUsage.Value()); memUsage > maxMemUsage {
				maxMemUsage = memUsage
			}
		}
	}
	return maxMemUsage
}

func getMaxMemoryUsageFromStreamStats(streamStats map[execinfrapb.StreamID]*streamStats) int64 {
	var maxMemUsage int64
	for _, stats := range streamStats {
		if stats.stats == nil {
			continue
		}
		if memUsage := int64(stats.stats.FlowStats.MaxMemUsage.Value()); memUsage > maxMemUsage {
			maxMemUsage = memUsage
		}
	}
	return maxMemUsage
}

// GetMaxMemoryUsage returns the maximum memory used by the trace.
func (a TraceAnalyzer) GetMaxMemoryUsage() int64 {
	// The vectorized flow attaches the MaxMemUsage stat to a flow level span, so we check flow stats
	// for MaxMemUsage.
	maxMemUsage := getMaxMemoryUsageFromFlowStats(a.flowStats)

	// If maxMemUsage is greater than 0, the vectorized flow was used and we can return this value without
	// checking stream stats.
	if maxMemUsage > 0 {
		return maxMemUsage
	}

	// TODO(cathymw): maxMemUsage shouldn't be attached to span stats that are associated with streams,
	// since it's a flow level stat. However, due to the row exec engine infrastructure, it is too
	// complicated to attach this to a flow level span. If the row exec engine gets removed, getting
	// maxMemUsage from streamStats should be removed as well.

	// The row execution flow attaches this stat to a stream stat with the last outbox, so we need to check
	// stream stats for MaxMemUsage.
	return getMaxMemoryUsageFromStreamStats(a.streamStats)
}

// QueryLevelStats returns all the top level stats that correspond to the given traces and flow metadata.
type QueryLevelStats struct {
	NetworkBytesSent int64
	MaxMemUsage      int64
}

// GetQueryLevelStats returns all the top-level stats in a QueryLevelStats struct.
// GetQueryLevelStats tries to calculate as many stats as possible. If errors occur
// while calculating stats, GetQueryLevelStats adds the error to a slice to be returned
// to the caller, but continues calculating other stats.
func GetQueryLevelStats(
	trace []tracingpb.RecordedSpan, deterministicExplainAnalyze bool, flowMetadata []*FlowMetadata,
) (QueryLevelStats, []error) {
	var queryLevelStats QueryLevelStats
	var errs []error
	networkBytesSent := int64(0)
	queryMaxMemUsage := int64(0)
	for _, metadata := range flowMetadata {
		analyzer := MakeTraceAnalyzer(metadata)
		if err := analyzer.AddTrace(trace, deterministicExplainAnalyze); err != nil {
			errs = append(errs, errors.Wrap(err, "error analyzing trace statistics"))
			continue
		}

		networkBytesSentGroupedByNode, err := analyzer.GetNetworkBytesSent()
		if err != nil {
			errs = append(errs, errors.Wrap(err, "error calculating network bytes sent"))
			continue
		}
		for _, bytesSentByNode := range networkBytesSentGroupedByNode {
			networkBytesSent += bytesSentByNode
		}
		if flowMaxMemUsage := analyzer.GetMaxMemoryUsage(); flowMaxMemUsage > queryMaxMemUsage {
			queryMaxMemUsage = flowMaxMemUsage
		}
	}
	queryLevelStats = QueryLevelStats{
		NetworkBytesSent: networkBytesSent,
		MaxMemUsage:      queryMaxMemUsage,
	}
	return queryLevelStats, errs
}
