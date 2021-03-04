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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

// FlowsMetadata contains metadata extracted from flows that comprise a single
// physical plan. This information is stored in sql.flowInfo and is analyzed by
// TraceAnalyzer.
type FlowsMetadata struct {
	// flowID is the FlowID of the flows belonging to the physical plan. Note that
	// the same FlowID is used across multiple flows in the same query.
	flowID execinfrapb.FlowID
}

// NewFlowsMetadata creates a FlowsMetadata for the given physical plan
// information.
func NewFlowsMetadata(flows map[roachpb.NodeID]*execinfrapb.FlowSpec) *FlowsMetadata {
	a := &FlowsMetadata{}

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
	}

	return a
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
}

// TraceAnalyzer is a struct that helps calculate top-level statistics from a
// flow metadata and an accompanying trace of the flows' execution.
type TraceAnalyzer struct {
	*FlowsMetadata
	queryLevelStats QueryLevelStats
}

// NewTraceAnalyzer creates a TraceAnalyzer with the corresponding physical
// plan. Call AddTrace to calculate meaningful stats.
func NewTraceAnalyzer(flowsMetadata *FlowsMetadata) *TraceAnalyzer {
	return &TraceAnalyzer{FlowsMetadata: flowsMetadata}
}

// ProcessStats calculates node level and query level stats for the trace and
// stores them in TraceAnalyzer. If errors occur while calculating stats,
// ProcessStats returns the combined errors to the caller but continues
// calculating other stats.
func (a *TraceAnalyzer) ProcessStats(trace []tracingpb.RecordedSpan, makeDeterministic bool) error {
	var errs error

	m, cleanup := execinfrapb.ExtractStatsFromSpans(trace, makeDeterministic)
	defer cleanup()
	for component, componentStats := range m {
		if !component.FlowID.Equal(a.flowID) {
			// This component belongs to a flow we do not care about. Note that we use
			// a bytes comparison because the UUID Equals method only returns true iff
			// the UUIDs are the same object.
			continue
		}
		switch component.Type {
		case execinfrapb.ComponentID_PROCESSOR:
			a.queryLevelStats.KVBytesRead += int64(componentStats.KV.BytesRead.Value())
			a.queryLevelStats.KVRowsRead += int64(componentStats.KV.TuplesRead.Value())
			a.queryLevelStats.KVTime += componentStats.KV.KVTime.Value()
			a.queryLevelStats.ContentionTime += componentStats.KV.ContentionTime.Value()

		case execinfrapb.ComponentID_STREAM:
			// Set networkBytesSentGroupedByNode.
			bytes, err := getNetworkBytesFromComponentStats(componentStats)
			if err != nil {
				errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating network bytes sent"))
			} else {
				a.queryLevelStats.NetworkBytesSent += bytes
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
			if componentStats.FlowStats.MaxMemUsage.HasValue() {
				memUsage := int64(componentStats.FlowStats.MaxMemUsage.Value())
				if memUsage > a.queryLevelStats.MaxMemUsage {
					a.queryLevelStats.MaxMemUsage = memUsage
				}
			}
			if componentStats.FlowStats.MaxDiskUsage.HasValue() {
				if diskUsage := int64(componentStats.FlowStats.MaxDiskUsage.Value()); diskUsage > a.queryLevelStats.MaxDiskUsage {
					a.queryLevelStats.MaxDiskUsage = diskUsage
				}
			}

			numMessages, err := getNumNetworkMessagesFromComponentsStats(componentStats)
			if err != nil {
				errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating number of network messages"))
			} else {
				a.queryLevelStats.NetworkMessages += numMessages
			}

		case execinfrapb.ComponentID_FLOW:
			if componentStats.FlowStats.MaxMemUsage.HasValue() {
				if memUsage := int64(componentStats.FlowStats.MaxMemUsage.Value()); memUsage > a.queryLevelStats.MaxMemUsage {
					a.queryLevelStats.MaxMemUsage = memUsage
				}
			}
			if componentStats.FlowStats.MaxDiskUsage.HasValue() {
				if diskUsage := int64(componentStats.FlowStats.MaxDiskUsage.Value()); diskUsage > a.queryLevelStats.MaxDiskUsage {
					a.queryLevelStats.MaxDiskUsage = diskUsage
				}

			}
		}
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
		if err := analyzer.ProcessStats(trace, deterministicExplainAnalyze); err != nil {
			errs = errors.CombineErrors(errs, err)
			continue
		}
		queryLevelStats.Accumulate(analyzer.GetQueryLevelStats())
	}
	return queryLevelStats, errs
}
