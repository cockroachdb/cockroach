// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

type streamStats struct {
	originSQLInstanceID      base.SQLInstanceID
	destinationSQLInstanceID base.SQLInstanceID
	stats                    *execinfrapb.ComponentStats
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
	// processorStats maps a processor ID to stats associated with this component
	// extracted from a trace as well as some metadata. Note that it is possible
	// for the processorStats to have nil stats, which indicates that no stats
	// were found for the given processor in the trace.
	processorStats map[execinfrapb.ProcessorID]*execinfrapb.ComponentStats
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
func NewFlowsMetadata(flows map[base.SQLInstanceID]*execinfrapb.FlowSpec) *FlowsMetadata {
	a := &FlowsMetadata{
		processorStats: make(map[execinfrapb.ProcessorID]*execinfrapb.ComponentStats),
		streamStats:    make(map[execinfrapb.StreamID]*streamStats),
		flowStats:      make(map[base.SQLInstanceID]*flowStats),
	}

	// Annotate the maps with physical plan information.
	for sqlInstanceID, flow := range flows {
		if a.flowID.IsUnset() {
			a.flowID = flow.FlowID
		} else if buildutil.CrdbTestBuild && !a.flowID.Equal(flow.FlowID) {
			panic(
				errors.AssertionFailedf(
					"expected the same FlowID to be used for all flows. UUID of first flow: %v, UUID of flow on node %s: %v",
					a.flowID, sqlInstanceID, flow.FlowID),
			)
		}
		a.flowStats[sqlInstanceID] = &flowStats{}
		for _, proc := range flow.Processors {
			procID := execinfrapb.ProcessorID(proc.ProcessorID)
			a.processorStats[procID] = &execinfrapb.ComponentStats{}
			a.processorStats[procID].Component.SQLInstanceID = sqlInstanceID

			for _, output := range proc.Output {
				for _, stream := range output.Streams {
					a.streamStats[stream.StreamID] = &streamStats{
						originSQLInstanceID:      sqlInstanceID,
						destinationSQLInstanceID: stream.TargetNodeID,
					}
				}
			}
		}
	}

	return a
}

// QueryLevelStats returns all the query level stats that correspond to the
// given traces and flow metadata.
// NOTE: When adding fields to this struct, be sure to update Accumulate.
type QueryLevelStats struct {
	NetworkBytesSent                   int64
	MaxMemUsage                        int64
	MaxDiskUsage                       int64
	KVBytesRead                        int64
	KVPairsRead                        int64
	KVRowsRead                         int64
	KVBatchRequestsIssued              int64
	KVTime                             time.Duration
	MvccSteps                          int64
	MvccStepsInternal                  int64
	MvccSeeks                          int64
	MvccSeeksInternal                  int64
	MvccBlockBytes                     int64
	MvccBlockBytesInCache              int64
	MvccKeyBytes                       int64
	MvccValueBytes                     int64
	MvccPointCount                     int64
	MvccPointsCoveredByRangeTombstones int64
	MvccRangeKeyCount                  int64
	MvccRangeKeyContainedPoints        int64
	MvccRangeKeySkippedPoints          int64
	NetworkMessages                    int64
	ContentionTime                     time.Duration
	ContentionEvents                   []kvpb.ContentionEvent
	RUEstimate                         float64
	CPUTime                            time.Duration
	// SQLInstanceIDs is an ordered list of SQL instances that were involved in
	// query processing.
	SQLInstanceIDs []int32
	// KVNodeIDs is an ordered list of KV Node IDs that were used for KV reads
	// while processing the query.
	KVNodeIDs []int32
	// Regions is an ordered list of regions in which both SQL and KV nodes
	// involved in query processing reside.
	Regions []string
	// UsedFollowerRead indicates whether at least some reads were served by the
	// follower replicas.
	UsedFollowerRead bool
	ClientTime       time.Duration
}

// QueryLevelStatsWithErr is the same as QueryLevelStats, but also tracks
// if an error occurred while getting query-level stats.
type QueryLevelStatsWithErr struct {
	Stats QueryLevelStats
	Err   error
}

// MakeQueryLevelStatsWithErr creates a QueryLevelStatsWithErr from a
// QueryLevelStats and error.
func MakeQueryLevelStatsWithErr(stats QueryLevelStats, err error) QueryLevelStatsWithErr {
	return QueryLevelStatsWithErr{
		stats,
		err,
	}
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
	s.KVPairsRead += other.KVPairsRead
	s.KVRowsRead += other.KVRowsRead
	s.KVBatchRequestsIssued += other.KVBatchRequestsIssued
	s.KVTime += other.KVTime
	s.MvccSteps += other.MvccSteps
	s.MvccStepsInternal += other.MvccStepsInternal
	s.MvccSeeks += other.MvccSeeks
	s.MvccSeeksInternal += other.MvccSeeksInternal
	s.MvccBlockBytes += other.MvccBlockBytes
	s.MvccBlockBytesInCache += other.MvccBlockBytesInCache
	s.MvccKeyBytes += other.MvccKeyBytes
	s.MvccValueBytes += other.MvccValueBytes
	s.MvccPointCount += other.MvccPointCount
	s.MvccPointsCoveredByRangeTombstones += other.MvccPointsCoveredByRangeTombstones
	s.MvccRangeKeyCount += other.MvccRangeKeyCount
	s.MvccRangeKeyContainedPoints += other.MvccRangeKeyContainedPoints
	s.MvccRangeKeySkippedPoints += other.MvccRangeKeySkippedPoints
	s.NetworkMessages += other.NetworkMessages
	s.ContentionTime += other.ContentionTime
	s.ContentionEvents = append(s.ContentionEvents, other.ContentionEvents...)
	s.RUEstimate += other.RUEstimate
	s.CPUTime += other.CPUTime
	s.SQLInstanceIDs = util.CombineUnique(s.SQLInstanceIDs, other.SQLInstanceIDs)
	s.KVNodeIDs = util.CombineUnique(s.KVNodeIDs, other.KVNodeIDs)
	s.Regions = util.CombineUnique(s.Regions, other.Regions)
	s.UsedFollowerRead = s.UsedFollowerRead || other.UsedFollowerRead
	s.ClientTime += other.ClientTime
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
			a.processorStats[execinfrapb.ProcessorID(id)] = componentStats

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

// ProcessStats calculates query level stats for the trace and stores them in
// TraceAnalyzer.
func (a *TraceAnalyzer) ProcessStats() {
	s := &a.queryLevelStats

	// Process processorStats.
	for _, stats := range a.processorStats {
		if stats == nil {
			continue
		}
		s.KVNodeIDs = util.CombineUnique(s.KVNodeIDs, stats.KV.NodeIDs)
		// Aggregate both KV and SQL regions into the same field.
		s.Regions = util.CombineUnique(s.Regions, stats.KV.Regions)
		s.UsedFollowerRead = s.UsedFollowerRead || stats.KV.UsedFollowerRead
		s.KVBytesRead += int64(stats.KV.BytesRead.Value())
		s.KVPairsRead += int64(stats.KV.KVPairsRead.Value())
		s.KVRowsRead += int64(stats.KV.TuplesRead.Value())
		s.KVBatchRequestsIssued += int64(stats.KV.BatchRequestsIssued.Value())
		s.KVTime += stats.KV.KVTime.Value()
		s.MvccSteps += int64(stats.KV.NumInterfaceSteps.Value())
		s.MvccStepsInternal += int64(stats.KV.NumInternalSteps.Value())
		s.MvccSeeks += int64(stats.KV.NumInterfaceSeeks.Value())
		s.MvccSeeksInternal += int64(stats.KV.NumInternalSeeks.Value())
		s.MvccBlockBytes += int64(stats.KV.BlockBytes.Value())
		s.MvccBlockBytesInCache += int64(stats.KV.BlockBytesInCache.Value())
		s.MvccKeyBytes += int64(stats.KV.BlockBytesInCache.Value())
		s.MvccValueBytes += int64(stats.KV.ValueBytes.Value())
		s.MvccPointCount += int64(stats.KV.PointCount.Value())
		s.MvccPointsCoveredByRangeTombstones += int64(stats.KV.PointsCoveredByRangeTombstones.Value())
		s.MvccRangeKeyCount += int64(stats.KV.RangeKeyCount.Value())
		s.MvccRangeKeyContainedPoints += int64(stats.KV.RangeKeyContainedPoints.Value())
		s.MvccRangeKeySkippedPoints += int64(stats.KV.RangeKeySkippedPoints.Value())
		s.ContentionTime += stats.KV.ContentionTime.Value()
		s.RUEstimate += float64(stats.Exec.ConsumedRU.Value())
		s.CPUTime += stats.Exec.CPUTime.Value()
	}

	// Process streamStats.
	for _, stats := range a.streamStats {
		if stats.stats == nil {
			continue
		}
		s.NetworkBytesSent += getNetworkBytesFromComponentStats(stats.stats)
		s.NetworkMessages += getNumNetworkMessagesFromComponentsStats(stats.stats)
	}

	// Process flowStats.
	for instanceID, stats := range a.flowStats {
		if stats.stats == nil {
			continue
		}
		s.SQLInstanceIDs = util.InsertUnique(s.SQLInstanceIDs, int32(instanceID))
		for _, v := range stats.stats {
			// Avoid duplicates and empty string.
			if v.Component.Region != "" {
				s.Regions = util.InsertUnique(s.Regions, v.Component.Region)
			}

			if v.FlowStats.MaxMemUsage.HasValue() {
				if memUsage := int64(v.FlowStats.MaxMemUsage.Value()); memUsage > a.queryLevelStats.MaxMemUsage {
					a.queryLevelStats.MaxMemUsage = memUsage
				}
			}
			if v.FlowStats.MaxDiskUsage.HasValue() {
				if diskUsage := int64(v.FlowStats.MaxDiskUsage.Value()); diskUsage > a.queryLevelStats.MaxDiskUsage {
					a.queryLevelStats.MaxDiskUsage = diskUsage
				}
			}
			if v.FlowStats.ConsumedRU.HasValue() {
				s.RUEstimate += float64(v.FlowStats.ConsumedRU.Value())
			}
		}
	}
}

func getNetworkBytesFromComponentStats(v *execinfrapb.ComponentStats) int64 {
	// We expect exactly one of BytesReceived and BytesSent to be set. It may
	// seem like we are double-counting everything (from both the send and the
	// receive side) but in practice only one side of each stream presents
	// statistics (specifically the sending side in the row engine, and the
	// receiving side in the vectorized engine).
	if v.NetRx.BytesReceived.HasValue() {
		if buildutil.CrdbTestBuild {
			if v.NetTx.BytesSent.HasValue() {
				panic(errors.AssertionFailedf("could not get network bytes; both BytesReceived and BytesSent are set"))
			}
		}
		return int64(v.NetRx.BytesReceived.Value())
	}
	if v.NetTx.BytesSent.HasValue() {
		return int64(v.NetTx.BytesSent.Value())
	}
	// If neither BytesReceived or BytesSent is set, this ComponentStat belongs to
	// a local component, e.g. a local hashrouter output.
	return 0
}

func getNumNetworkMessagesFromComponentsStats(v *execinfrapb.ComponentStats) int64 {
	// We expect exactly one of MessagesReceived and MessagesSent to be set. It
	// may seem like we are double-counting everything (from both the send and
	// the receive side) but in practice only one side of each stream presents
	// statistics (specifically the sending side in the row engine, and the
	// receiving side in the vectorized engine).
	if v.NetRx.MessagesReceived.HasValue() {
		if buildutil.CrdbTestBuild {
			if v.NetTx.MessagesSent.HasValue() {
				panic(errors.AssertionFailedf("could not get network messages; both MessagesReceived and MessagesSent are set"))
			}
		}
		return int64(v.NetRx.MessagesReceived.Value())
	}
	if v.NetTx.MessagesSent.HasValue() {
		return int64(v.NetTx.MessagesSent.Value())
	}
	// If neither BytesReceived or BytesSent is set, this ComponentStat belongs to
	// a local component, e.g. a local hashrouter output.
	return 0
}

// GetQueryLevelStats returns the query level stats calculated and stored in
// TraceAnalyzer.
func (a *TraceAnalyzer) GetQueryLevelStats() QueryLevelStats {
	return a.queryLevelStats
}

// getAllContentionEvents returns all contention events that are found in the
// given trace.
func getAllContentionEvents(trace []tracingpb.RecordedSpan) []kvpb.ContentionEvent {
	var contentionEvents []kvpb.ContentionEvent
	var ev kvpb.ContentionEvent
	for i := range trace {
		trace[i].Structured(func(any *pbtypes.Any, _ time.Time) {
			if !pbtypes.Is(any, &ev) {
				return
			}
			if err := pbtypes.UnmarshalAny(any, &ev); err != nil {
				return
			}
			contentionEvents = append(contentionEvents, ev)
		})
	}
	return contentionEvents
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

		analyzer.ProcessStats()
		queryLevelStats.Accumulate(analyzer.GetQueryLevelStats())
	}
	queryLevelStats.ContentionEvents = getAllContentionEvents(trace)
	return queryLevelStats, errs
}
