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
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// QueryLevelStats returns all the query level stats that correspond to the
// given traces.
type QueryLevelStats struct {
	NetworkBytesSent int64
	MaxMemUsage      int64
	KVBytesRead      int64
	KVRowsRead       int64
	KVTime           time.Duration
	NetworkMessages  int64
	ContentionTime   time.Duration
}

// TraceAnalyzer is a struct that helps calculate query-level statistics from
// the trace of the execution.
type TraceAnalyzer struct {
	queryLevelStats QueryLevelStats
}

// If makeDeterministic is set, statistics that can vary from run to run are set
// to fixed values; see ComponentStats.MakeDeterministic.
func (a *TraceAnalyzer) Analyze(trace []tracingpb.RecordedSpan, makeDeterministic bool) error {
	m := execinfrapb.ExtractStatsFromSpans(trace, makeDeterministic)
	var errs error
	for _, componentStats := range m {
		errs = errors.CombineErrors(errs, a.ProcessComponentStats(componentStats))
	}
	a.queryLevelStats.ContentionTime = calculateContentionTime(trace, makeDeterministic)
	return errs
}

func calculateContentionTime(trace []tracingpb.RecordedSpan, makeDeterministic bool) time.Duration {
	if makeDeterministic {
		// Use 1ns, similar to execstats.ExtractStatsFromSpans.
		return time.Nanosecond
	}
	var contentionTime time.Duration
	var ev roachpb.ContentionEvent
	for i := range trace {
		trace[i].Structured(func(any *pbtypes.Any) {
			if !pbtypes.Is(any, roachpb.EmptyContentionEvent) {
				return
			}
			if err := pbtypes.UnmarshalAny(any, &ev); err != nil {
				return
			}
			contentionTime += ev.Duration
		})
	}
	return contentionTime
}

func (a *TraceAnalyzer) ProcessComponentStats(componentStats *execinfrapb.ComponentStats) error {
	var errs error
	switch componentStats.Component.Type {
	case execinfrapb.ComponentID_PROCESSOR:
		a.queryLevelStats.KVBytesRead += int64(componentStats.KV.BytesRead.Value())
		a.queryLevelStats.KVRowsRead += int64(componentStats.KV.TuplesRead.Value())
		a.queryLevelStats.KVTime += componentStats.KV.KVTime.Value()

	case execinfrapb.ComponentID_STREAM:
		bytes, err := getNetworkBytesFromComponentStats(componentStats)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating network bytes sent"))
		} else {
			a.queryLevelStats.NetworkBytesSent += bytes
		}

		// The row execution flow attaches this stat to a stream stat with the
		// last outbox, so we need to check stream stats for max memory usage.
		// TODO(cathymw): maxMemUsage shouldn't be attached to span stats that
		// are associated with streams, since it's a flow level stat. However,
		// due to the row exec engine infrastructure, it is too complicated to
		// attach this to a flow level span. If the row exec engine gets
		// removed, getting maxMemUsage from streamStats should be removed as
		// well.
		if componentStats.FlowStats.MaxMemUsage.HasValue() {
			if memUsage := int64(componentStats.FlowStats.MaxMemUsage.Value()); memUsage > a.queryLevelStats.MaxMemUsage {
				a.queryLevelStats.MaxMemUsage = memUsage
			}
		}

		numMessages, err := getNumNetworkMessagesFromComponentsStats(componentStats)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating number of network messages"))
		} else {
			a.queryLevelStats.NetworkMessages += numMessages
		}

	case execinfrapb.ComponentID_FLOW:
		// The vectorized flow attaches the MaxMemUsage stat to a flow level
		// span, so we need to check flow stats for max memory usage.
		if componentStats.FlowStats.MaxMemUsage.HasValue() {
			if memUsage := int64(componentStats.FlowStats.MaxMemUsage.Value()); memUsage > a.queryLevelStats.MaxMemUsage {
				a.queryLevelStats.MaxMemUsage = memUsage
			}
		}
	}
	return errs
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

func getNumNetworkMessagesFromComponentsStats(v *execinfrapb.ComponentStats) (int64, error) {
	// We expect exactly one of MessagesReceived and MessagesSent to be set.
	// It may seem like we are double-counting everything (from both the send and
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
	return 0, errors.Errorf("could not get network messages; neither MessagesReceived and MessagesSent is set")
}

// GetQueryLevelStats returns the query level stats calculated and stored in TraceAnalyzer.
func (a *TraceAnalyzer) GetQueryLevelStats() QueryLevelStats {
	return a.queryLevelStats
}

// GetQueryLevelStats returns all the top-level stats in a QueryLevelStats struct.
// GetQueryLevelStats tries to process as many stats as possible. If errors occur
// while processing stats, GetQueryLevelStats returns the combined errors to the caller
// but continues calculating other stats.
func GetQueryLevelStats(
	trace []tracingpb.RecordedSpan, deterministicExplainAnalyze bool,
) (QueryLevelStats, error) {
	var analyzer TraceAnalyzer
	errs := analyzer.Analyze(trace, deterministicExplainAnalyze)
	return analyzer.queryLevelStats, errs
}
