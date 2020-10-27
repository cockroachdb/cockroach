// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/types"
)

const nodeTagKey = "node"

// TraceAnalyzer is a struct that helps calculate top-level statistics from a
// collection of flows and an accompanying trace of the flows' execution.
// Example usage:
//     analyzer := &sql.TraceAnalyzer{}
//     analyzer.Reset(flows, trace)
//     bytesGroupedByNode, err := analyzer.GetNetworkBytesSent()
type TraceAnalyzer struct {
	// processorIDMap maps a processor ID to a node it was planned on.
	processorIDMap map[int]roachpb.NodeID
	// streamIDMap maps a stream ID to a given pair of nodes. The first node in
	// the array is the origin node, the second is the destination node.
	streamIDMap map[int][2]roachpb.NodeID

	// processorStats maps a processor ID to the stats associated with this
	// processor extracted from a trace.
	processorStats map[int]execinfrapb.DistSQLSpanStats
	// streamStats maps a stream ID to the stats associated with this stream
	// extracted from a trace.
	streamStats map[int]execinfrapb.DistSQLSpanStats
}

func (a *TraceAnalyzer) populateIDMaps(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) {
	for k := range a.processorIDMap {
		delete(a.processorIDMap, k)
	}
	for k := range a.streamIDMap {
		delete(a.streamIDMap, k)
	}
	for nodeID, flow := range flows {
		for _, proc := range flow.Processors {
			a.processorIDMap[int(proc.ProcessorID)] = nodeID
			for _, output := range proc.Output {
				for _, stream := range output.Streams {
					if stream.Type == execinfrapb.StreamEndpointSpec_REMOTE {
						a.streamIDMap[int(stream.StreamID)] = [2]roachpb.NodeID{nodeID, stream.TargetNodeID}
					}
				}
			}
		}
	}
}

func (a *TraceAnalyzer) extractStatsByID(trace []tracingpb.RecordedSpan) error {
	for _, m := range []map[int]execinfrapb.DistSQLSpanStats{a.processorStats, a.streamStats} {
		for k := range m {
			delete(m, k)
		}
	}

	for _, span := range trace {
		// Get the processor or stream id for this span. If neither exists, this
		// span doesn't belong to a processor or stream.
		var (
			stringID  string
			idToStats map[int]execinfrapb.DistSQLSpanStats
		)
		if pid, ok := span.Tags[execinfrapb.ProcessorIDTagKey]; ok {
			stringID = pid
			idToStats = a.processorStats
		} else if sid, ok := span.Tags[execinfrapb.StreamIDTagKey]; ok {
			stringID = sid
			idToStats = a.streamStats
		} else {
			// No stream or processor stats.
			continue
		}

		if span.Stats == nil {
			// No stats to unmarshal (e.g. noop processors at time of writing).
			continue
		}

		var da types.DynamicAny
		if err := types.UnmarshalAny(span.Stats, &da); err != nil {
			return errors.Wrap(err, "unable to unmarshal in TraceAnalyzer")
		}
		if dss, ok := da.Message.(execinfrapb.DistSQLSpanStats); ok {
			id, err := strconv.Atoi(stringID)
			if err != nil {
				return errors.Wrap(err, "unable to convert span stream/processor ID tag in TraceAnalyzer")
			}
			if _, ok := idToStats[id]; ok {
				return errors.Errorf("already found stats for id %d", id)
			}
			idToStats[id] = dss
		}
	}
	return nil
}

// Reset resets or initializes the receiver for the calculation of top-level
// statistics from the flows and an accompanying trace.
func (a *TraceAnalyzer) Reset(
	flows map[roachpb.NodeID]*execinfrapb.FlowSpec, trace []tracingpb.RecordedSpan,
) error {
	if a.processorIDMap == nil {
		a.processorIDMap = make(map[int]roachpb.NodeID)
	}
	if a.streamIDMap == nil {
		a.streamIDMap = make(map[int][2]roachpb.NodeID)
	}
	if a.processorStats == nil {
		a.processorStats = make(map[int]execinfrapb.DistSQLSpanStats)
	}
	if a.streamStats == nil {
		a.streamStats = make(map[int]execinfrapb.DistSQLSpanStats)
	}
	a.populateIDMaps(flows)
	return a.extractStatsByID(trace)
}

func getNetworkBytesFromDistSQLSpanStats(dss execinfrapb.DistSQLSpanStats) (int64, error) {
	switch v := dss.(type) {
	case *flowinfra.OutboxStats:
		return v.BytesSent, nil
	case *execpb.VectorizedStats:
		// VectorizedStats are output by the Inbox, hence the read/sent difference
		// with OutboxStats.
		return v.BytesRead, nil
	}
	return 0, errors.Errorf("could not get network bytes from %T", dss)
}

// GetNetworkBytesSent returns the number of bytes sent over the network the
// trace reports, grouped by NodeID.
func (a *TraceAnalyzer) GetNetworkBytesSent() (map[roachpb.NodeID]int64, error) {
	result := make(map[roachpb.NodeID]int64)
	for streamID, nodePair := range a.streamIDMap {
		bytes, err := getNetworkBytesFromDistSQLSpanStats(a.streamStats[streamID])
		if err != nil {
			return nil, err
		}
		// The first node in the nodePair array is the origin node.
		result[nodePair[0]] += bytes
	}
	return result, nil
}
