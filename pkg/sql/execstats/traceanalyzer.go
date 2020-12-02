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

// TraceAnalyzer is a struct that helps calculate top-level statistics from a
// collection of flows and an accompanying trace of the flows' execution.
// Example usage:
//     analyzer := NewTraceAnalyzer(flows)
//     analyzer.AddTrace(trace, false /* makeDeterministic */)
//     bytesGroupedByNode, err := analyzer.GetNetworkBytesSent()
type TraceAnalyzer struct {
	// processorIDMap maps a processor ID to stats associated with this processor
	// extracted from a trace as well as some metadata. Note that it is possible
	// for the processorStats to have nil stats, which indicates that no stats
	// were found for the given processor in the trace.
	processorStats map[execinfrapb.ProcessorID]*processorStats
	// streamIDMap maps a stream ID to stats associated with this stream extracted
	// from a trace as well as some metadata. Note that is is possible for the
	// streamStats to have nil stats, which indicates that no stats were found
	// for the given stream in the trace.
	streamStats map[execinfrapb.StreamID]*streamStats
}

// NewTraceAnalyzer creates a TraceAnalyzer with the corresponding physical
// plan. Call AddTrace to calculate meaningful stats.
func NewTraceAnalyzer(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) *TraceAnalyzer {
	a := &TraceAnalyzer{
		processorStats: make(map[execinfrapb.ProcessorID]*processorStats),
		streamStats:    make(map[execinfrapb.StreamID]*streamStats),
	}

	// Annotate the maps with physical plan information.
	for nodeID, flow := range flows {
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
			streamStats.stats = &stats
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
func (a *TraceAnalyzer) GetNetworkBytesSent() (map[roachpb.NodeID]int64, error) {
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
