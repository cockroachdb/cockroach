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
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
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
	stats             []*execinfrapb.ComponentStats
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
	// flowStats maps a flow ID, represented as a string, to flow level stats
	// extracted from a trace.
	flowStats map[string]*flowStats
}

// NewFlowMetadata creates a FlowMetadata with the given physical plan information.
func NewFlowMetadata(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) *FlowMetadata {
	a := &FlowMetadata{
		processorStats: make(map[execinfrapb.ProcessorID]*processorStats),
		streamStats:    make(map[execinfrapb.StreamID]*streamStats),
		flowStats:      make(map[string]*flowStats),
	}

	// Annotate the maps with physical plan information.
	for nodeID, flow := range flows {
		a.flowStats[flow.FlowID.String()] = &flowStats{}
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
//     analyzer := NewTraceAnalyzer(flowMetadata)
//     analyzer.AddTrace(trace)
//     bytesGroupedByNode, err := analyzer.GetNetworkBytesSent()
type TraceAnalyzer struct {
	FlowMetadata
}

// NewTraceAnalyzer creates a TraceAnalyzer with the corresponding physical
// plan. Call AddTrace to calculate meaningful stats.
func NewTraceAnalyzer(flowMetadata FlowMetadata) *TraceAnalyzer {
	a := &TraceAnalyzer{
		FlowMetadata: flowMetadata,
	}

	return a
}

// AddTrace adds the stats from the given trace to the TraceAnalyzer.
//
// If makeDeterministic is set, statistics that can vary from run to run are set
// to fixed values; see ComponentStats.MakeDeterministic.
func (a *TraceAnalyzer) AddTrace(trace []tracingpb.RecordedSpan, makeDeterministic bool) error {
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
			streamStats.stats = append(streamStats.stats, &stats)
		} else if fid, ok := span.Tags[execinfrapb.FlowIDTagKey]; ok {
			flowStats := a.flowStats[fid]
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
	if v.FlowStats.MaxMemUsage.HasValue() {
		// This stream might have multiple span stats, some of which may not have network bytes information.
		return 0, nil
	}
	return 0, errors.Errorf("could not get network bytes; neither BytesReceived and BytesSent is set")
}

// GetNetworkBytesSent returns the number of bytes sent over the network the
// trace reports, grouped by NodeID.
func (a *TraceAnalyzer) GetNetworkBytesSent() (map[roachpb.NodeID]int64, error) {
	result := make(map[roachpb.NodeID]int64)
	for _, stats := range a.streamStats {
		if stats.stats == nil {
			continue
		}
		for _, streamStats := range stats.stats {
			bytes, err := getNetworkBytesFromComponentStats(streamStats)
			if err != nil {
				return nil, err
			}
			result[stats.originNodeID] += bytes
		}
	}
	return result, nil
}

func getMaxMemoryUsageFromComponentStats(stats []*execinfrapb.ComponentStats) int64 {
	var maxMemUsage int64
	for _, v := range stats {
		if memUsage := int64(v.FlowStats.MaxMemUsage.Value()); memUsage > maxMemUsage {
			maxMemUsage = memUsage
		}
	}
	return maxMemUsage
}

func getMaxMemoryUsageFromFlowStats(flowStats map[string]*flowStats) int64 {
	var maxMemUsage int64
	for _, stats := range flowStats {
		if stats.stats == nil {
			continue
		}
		if memUsage := getMaxMemoryUsageFromComponentStats(stats.stats); memUsage > maxMemUsage {
			maxMemUsage = memUsage
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
		if memUsage := getMaxMemoryUsageFromComponentStats(stats.stats); memUsage > maxMemUsage {
			maxMemUsage = memUsage
		}
	}
	return maxMemUsage
}

// GetMaxMemoryUsage returns the maximum memory used by the trace.
func (a *TraceAnalyzer) GetMaxMemoryUsage() int64 {
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

// GetBytesReadFromKV returns the total number of bytes read from KV.
func (a *TraceAnalyzer) GetBytesReadFromKV() int64 {
	var bytesRead int64
	for _, stats := range a.processorStats {
		if stats.stats == nil {
			continue
		}
		bytesRead += int64(stats.stats.KV.BytesRead.Value())
	}
	return bytesRead
}

// GetRowsReadFromKV returns the total number of rows read from KV.
func (a *TraceAnalyzer) GetRowsReadFromKV() int64 {
	var rowsRead int64
	for _, stats := range a.processorStats {
		if stats.stats == nil {
			continue
		}
		rowsRead += int64(stats.stats.KV.TuplesRead.Value())
	}
	return rowsRead
}

func (a *TraceAnalyzer) GetTotalKVTime() (time.Duration, int64) {
	var kvTime time.Duration
	var numKVOps int64
	for _, stats := range a.processorStats {
		if stats.stats == nil {
			continue
		}
		if stats.stats.KV.KVTime.HasValue() {
			kvTime += stats.stats.KV.KVTime.Value()
			numKVOps += 1
		}
	}
	return kvTime, numKVOps
}

// QueryLevelStats returns all the top level stats that correspond to the given traces and flow metadata.
type QueryLevelStats struct {
	NetworkBytesSent int64
	MaxMemUsage      int64
	BytesReadFromKV  int64
	RowsReadFromKV   int64
	KVTime           time.Duration
}

// GetQueryLevelStats returns all the top-level stats in a QueryLevelStats struct.
func GetQueryLevelStats(
	ctx context.Context,
	trace []tracingpb.RecordedSpan,
	deterministicExplainAnalyze bool,
	ast tree.Statement,
	flowMetadata []*FlowMetadata,
) *QueryLevelStats {
	networkBytesSent := int64(0)
	queryMaxMemUsage := int64(0)
	bytesReadFromKV := int64(0)
	rowsReadFromKV := int64(0)
	var totalKVTime time.Duration
	for _, metadata := range flowMetadata {
		analyzer := NewTraceAnalyzer(*metadata)
		if err := analyzer.AddTrace(trace, deterministicExplainAnalyze); err != nil {
			log.VInfof(ctx, 1, "error analyzing trace statistics for stmt %s: %v", ast, err)
			continue
		}

		networkBytesSentGroupedByNode, err := analyzer.GetNetworkBytesSent()
		if err != nil {
			log.VInfof(ctx, 1, "error calculating network bytes sent for stmt %s: %v", ast, err)
			continue
		}
		for _, bytesSentByNode := range networkBytesSentGroupedByNode {
			networkBytesSent += bytesSentByNode
		}
		if flowMaxMemUsage := analyzer.GetMaxMemoryUsage(); flowMaxMemUsage > queryMaxMemUsage {
			queryMaxMemUsage = flowMaxMemUsage
		}

		bytesReadFromKV += analyzer.GetBytesReadFromKV()
		rowsReadFromKV += analyzer.GetRowsReadFromKV()
		kvTime, _ := analyzer.GetTotalKVTime()
		totalKVTime += kvTime
	}
	return &QueryLevelStats{
		NetworkBytesSent: networkBytesSent,
		MaxMemUsage:      queryMaxMemUsage,
		BytesReadFromKV:  bytesReadFromKV,
		RowsReadFromKV:   rowsReadFromKV,
		KVTime:           totalKVTime,
	}
}
