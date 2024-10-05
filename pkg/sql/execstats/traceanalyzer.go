// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execstats

import (
	"sort"
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

// NodeLevelStats returns all the flow level stats that correspond to the given
// traces and flow metadata.
// TODO(asubiotto): Flatten this struct, we're currently allocating a map per
//
//	stat.
type NodeLevelStats struct {
	NetworkBytesSentGroupedByNode                   map[base.SQLInstanceID]int64
	MaxMemoryUsageGroupedByNode                     map[base.SQLInstanceID]int64
	MaxDiskUsageGroupedByNode                       map[base.SQLInstanceID]int64
	KVBytesReadGroupedByNode                        map[base.SQLInstanceID]int64
	KVPairsReadGroupedByNode                        map[base.SQLInstanceID]int64
	KVRowsReadGroupedByNode                         map[base.SQLInstanceID]int64
	KVBatchRequestsIssuedGroupedByNode              map[base.SQLInstanceID]int64
	KVTimeGroupedByNode                             map[base.SQLInstanceID]time.Duration
	MvccStepsGroupedByNode                          map[base.SQLInstanceID]int64
	MvccStepsInternalGroupedByNode                  map[base.SQLInstanceID]int64
	MvccSeeksGroupedByNode                          map[base.SQLInstanceID]int64
	MvccSeeksInternalGroupedByNode                  map[base.SQLInstanceID]int64
	MvccBlockBytesGroupedByNode                     map[base.SQLInstanceID]int64
	MvccBlockBytesInCacheGroupedByNode              map[base.SQLInstanceID]int64
	MvccKeyBytesGroupedByNode                       map[base.SQLInstanceID]int64
	MvccValueBytesGroupedByNode                     map[base.SQLInstanceID]int64
	MvccPointCountGroupedByNode                     map[base.SQLInstanceID]int64
	MvccPointsCoveredByRangeTombstonesGroupedByNode map[base.SQLInstanceID]int64
	MvccRangeKeyCountGroupedByNode                  map[base.SQLInstanceID]int64
	MvccRangeKeyContainedPointsGroupedByNode        map[base.SQLInstanceID]int64
	MvccRangeKeySkippedPointsGroupedByNode          map[base.SQLInstanceID]int64
	NetworkMessagesGroupedByNode                    map[base.SQLInstanceID]int64
	ContentionTimeGroupedByNode                     map[base.SQLInstanceID]time.Duration
	RUEstimateGroupedByNode                         map[base.SQLInstanceID]float64
	CPUTimeGroupedByNode                            map[base.SQLInstanceID]time.Duration
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
	SqlInstanceIds                     map[base.SQLInstanceID]struct{}
	Regions                            []string
	ClientTime                         time.Duration
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
	if len(s.SqlInstanceIds) == 0 && len(other.SqlInstanceIds) > 0 {
		s.SqlInstanceIds = other.SqlInstanceIds
	} else if len(other.SqlInstanceIds) > 0 && len(s.SqlInstanceIds) > 0 {
		for id := range other.SqlInstanceIds {
			s.SqlInstanceIds[id] = struct{}{}
		}
	}
	s.Regions = util.CombineUnique(s.Regions, other.Regions)
	s.ClientTime += other.ClientTime
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

// ProcessStats calculates node level and query level stats for the trace and
// stores them in TraceAnalyzer. If errors occur while calculating stats,
// ProcessStats returns the combined errors to the caller but continues
// calculating other stats.
func (a *TraceAnalyzer) ProcessStats() error {
	// Process node level stats.
	a.nodeLevelStats = NodeLevelStats{
		NetworkBytesSentGroupedByNode:                   make(map[base.SQLInstanceID]int64),
		MaxMemoryUsageGroupedByNode:                     make(map[base.SQLInstanceID]int64),
		MaxDiskUsageGroupedByNode:                       make(map[base.SQLInstanceID]int64),
		KVBytesReadGroupedByNode:                        make(map[base.SQLInstanceID]int64),
		KVPairsReadGroupedByNode:                        make(map[base.SQLInstanceID]int64),
		KVRowsReadGroupedByNode:                         make(map[base.SQLInstanceID]int64),
		KVBatchRequestsIssuedGroupedByNode:              make(map[base.SQLInstanceID]int64),
		KVTimeGroupedByNode:                             make(map[base.SQLInstanceID]time.Duration),
		MvccStepsGroupedByNode:                          make(map[base.SQLInstanceID]int64),
		MvccStepsInternalGroupedByNode:                  make(map[base.SQLInstanceID]int64),
		MvccSeeksGroupedByNode:                          make(map[base.SQLInstanceID]int64),
		MvccSeeksInternalGroupedByNode:                  make(map[base.SQLInstanceID]int64),
		MvccBlockBytesGroupedByNode:                     make(map[base.SQLInstanceID]int64),
		MvccBlockBytesInCacheGroupedByNode:              make(map[base.SQLInstanceID]int64),
		MvccKeyBytesGroupedByNode:                       make(map[base.SQLInstanceID]int64),
		MvccValueBytesGroupedByNode:                     make(map[base.SQLInstanceID]int64),
		MvccPointCountGroupedByNode:                     make(map[base.SQLInstanceID]int64),
		MvccPointsCoveredByRangeTombstonesGroupedByNode: make(map[base.SQLInstanceID]int64),
		MvccRangeKeyCountGroupedByNode:                  make(map[base.SQLInstanceID]int64),
		MvccRangeKeyContainedPointsGroupedByNode:        make(map[base.SQLInstanceID]int64),
		MvccRangeKeySkippedPointsGroupedByNode:          make(map[base.SQLInstanceID]int64),
		NetworkMessagesGroupedByNode:                    make(map[base.SQLInstanceID]int64),
		ContentionTimeGroupedByNode:                     make(map[base.SQLInstanceID]time.Duration),
		RUEstimateGroupedByNode:                         make(map[base.SQLInstanceID]float64),
		CPUTimeGroupedByNode:                            make(map[base.SQLInstanceID]time.Duration),
	}
	var errs error

	// Process processorStats.
	for _, stats := range a.processorStats {
		if stats == nil {
			continue
		}
		instanceID := stats.Component.SQLInstanceID
		a.nodeLevelStats.KVBytesReadGroupedByNode[instanceID] += int64(stats.KV.BytesRead.Value())
		a.nodeLevelStats.KVPairsReadGroupedByNode[instanceID] += int64(stats.KV.KVPairsRead.Value())
		a.nodeLevelStats.KVRowsReadGroupedByNode[instanceID] += int64(stats.KV.TuplesRead.Value())
		a.nodeLevelStats.KVBatchRequestsIssuedGroupedByNode[instanceID] += int64(stats.KV.BatchRequestsIssued.Value())
		a.nodeLevelStats.KVTimeGroupedByNode[instanceID] += stats.KV.KVTime.Value()
		a.nodeLevelStats.MvccStepsGroupedByNode[instanceID] += int64(stats.KV.NumInterfaceSteps.Value())
		a.nodeLevelStats.MvccStepsInternalGroupedByNode[instanceID] += int64(stats.KV.NumInternalSteps.Value())
		a.nodeLevelStats.MvccSeeksGroupedByNode[instanceID] += int64(stats.KV.NumInterfaceSeeks.Value())
		a.nodeLevelStats.MvccSeeksInternalGroupedByNode[instanceID] += int64(stats.KV.NumInternalSeeks.Value())
		a.nodeLevelStats.MvccBlockBytesGroupedByNode[instanceID] += int64(stats.KV.BlockBytes.Value())
		a.nodeLevelStats.MvccBlockBytesInCacheGroupedByNode[instanceID] += int64(stats.KV.BlockBytesInCache.Value())
		a.nodeLevelStats.MvccKeyBytesGroupedByNode[instanceID] += int64(stats.KV.BlockBytesInCache.Value())
		a.nodeLevelStats.MvccValueBytesGroupedByNode[instanceID] += int64(stats.KV.ValueBytes.Value())
		a.nodeLevelStats.MvccPointCountGroupedByNode[instanceID] += int64(stats.KV.PointCount.Value())
		a.nodeLevelStats.MvccPointsCoveredByRangeTombstonesGroupedByNode[instanceID] += int64(stats.KV.PointsCoveredByRangeTombstones.Value())
		a.nodeLevelStats.MvccRangeKeyCountGroupedByNode[instanceID] += int64(stats.KV.RangeKeyCount.Value())
		a.nodeLevelStats.MvccRangeKeyContainedPointsGroupedByNode[instanceID] += int64(stats.KV.RangeKeyContainedPoints.Value())
		a.nodeLevelStats.MvccRangeKeySkippedPointsGroupedByNode[instanceID] += int64(stats.KV.RangeKeySkippedPoints.Value())
		a.nodeLevelStats.ContentionTimeGroupedByNode[instanceID] += stats.KV.ContentionTime.Value()
		a.nodeLevelStats.RUEstimateGroupedByNode[instanceID] += float64(stats.Exec.ConsumedRU.Value())
		a.nodeLevelStats.CPUTimeGroupedByNode[instanceID] += stats.Exec.CPUTime.Value()
	}

	// Process streamStats.
	for _, stats := range a.streamStats {
		if stats.stats == nil {
			continue
		}
		originInstanceID := stats.originSQLInstanceID

		// Set networkBytesSentGroupedByNode.
		bytes, err := getNetworkBytesFromComponentStats(stats.stats)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating network bytes sent"))
		} else {
			a.nodeLevelStats.NetworkBytesSentGroupedByNode[originInstanceID] += bytes
		}

		numMessages, err := getNumNetworkMessagesFromComponentsStats(stats.stats)
		if err != nil {
			errs = errors.CombineErrors(errs, errors.Wrap(err, "error calculating number of network messages"))
		} else {
			a.nodeLevelStats.NetworkMessagesGroupedByNode[originInstanceID] += numMessages
		}
	}

	instanceIds := make(map[base.SQLInstanceID]struct{}, len(a.flowStats))
	// Default to 1 since most queries only use a single region.
	regions := make([]string, 0, 1)

	// Process flowStats.
	for instanceID, stats := range a.flowStats {
		if stats.stats == nil {
			continue
		}

		instanceIds[instanceID] = struct{}{}
		for _, v := range stats.stats {
			// Avoid duplicates and empty string.
			if v.Component.Region != "" {
				regions = util.CombineUnique(regions, []string{v.Component.Region})
			}

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
			if v.FlowStats.ConsumedRU.HasValue() {
				a.nodeLevelStats.RUEstimateGroupedByNode[instanceID] += float64(v.FlowStats.ConsumedRU.Value())
			}
		}
	}

	// Process query level stats.
	a.queryLevelStats = QueryLevelStats{}
	a.queryLevelStats.SqlInstanceIds = instanceIds
	sort.Strings(regions)

	a.queryLevelStats.Regions = regions
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

	for _, kvPairsRead := range a.nodeLevelStats.KVPairsReadGroupedByNode {
		a.queryLevelStats.KVPairsRead += kvPairsRead
	}

	for _, kvRowsRead := range a.nodeLevelStats.KVRowsReadGroupedByNode {
		a.queryLevelStats.KVRowsRead += kvRowsRead
	}

	for _, kvBatchRequestsIssued := range a.nodeLevelStats.KVBatchRequestsIssuedGroupedByNode {
		a.queryLevelStats.KVBatchRequestsIssued += kvBatchRequestsIssued
	}

	for _, kvTime := range a.nodeLevelStats.KVTimeGroupedByNode {
		a.queryLevelStats.KVTime += kvTime
	}

	for _, MvccSteps := range a.nodeLevelStats.MvccStepsGroupedByNode {
		a.queryLevelStats.MvccSteps += MvccSteps
	}

	for _, MvccStepsInternal := range a.nodeLevelStats.MvccStepsInternalGroupedByNode {
		a.queryLevelStats.MvccStepsInternal += MvccStepsInternal
	}

	for _, MvccSeeks := range a.nodeLevelStats.MvccSeeksGroupedByNode {
		a.queryLevelStats.MvccSeeks += MvccSeeks
	}

	for _, MvccSeeksInternal := range a.nodeLevelStats.MvccSeeksInternalGroupedByNode {
		a.queryLevelStats.MvccSeeksInternal += MvccSeeksInternal
	}

	for _, MvccBlockBytes := range a.nodeLevelStats.MvccBlockBytesGroupedByNode {
		a.queryLevelStats.MvccBlockBytes += MvccBlockBytes
	}

	for _, MvccBlockBytesInCache := range a.nodeLevelStats.MvccBlockBytesInCacheGroupedByNode {
		a.queryLevelStats.MvccBlockBytesInCache += MvccBlockBytesInCache
	}

	for _, MvccKeyBytes := range a.nodeLevelStats.MvccKeyBytesGroupedByNode {
		a.queryLevelStats.MvccKeyBytes += MvccKeyBytes
	}

	for _, MvccValueBytes := range a.nodeLevelStats.MvccValueBytesGroupedByNode {
		a.queryLevelStats.MvccValueBytes += MvccValueBytes
	}

	for _, MvccPointCount := range a.nodeLevelStats.MvccPointCountGroupedByNode {
		a.queryLevelStats.MvccPointCount += MvccPointCount
	}

	for _, MvccPointsCoveredByRangeTombstones := range a.nodeLevelStats.MvccPointsCoveredByRangeTombstonesGroupedByNode {
		a.queryLevelStats.MvccPointsCoveredByRangeTombstones += MvccPointsCoveredByRangeTombstones
	}

	for _, MvccRangeKeyCount := range a.nodeLevelStats.MvccRangeKeyCountGroupedByNode {
		a.queryLevelStats.MvccRangeKeyCount += MvccRangeKeyCount
	}

	for _, MvccRangeKeyContainedPoints := range a.nodeLevelStats.MvccRangeKeyContainedPointsGroupedByNode {
		a.queryLevelStats.MvccRangeKeyContainedPoints += MvccRangeKeyContainedPoints
	}

	for _, MvccRangeKeySkippedPoints := range a.nodeLevelStats.MvccRangeKeySkippedPointsGroupedByNode {
		a.queryLevelStats.MvccRangeKeySkippedPoints += MvccRangeKeySkippedPoints
	}

	for _, networkMessages := range a.nodeLevelStats.NetworkMessagesGroupedByNode {
		a.queryLevelStats.NetworkMessages += networkMessages
	}

	for _, contentionTime := range a.nodeLevelStats.ContentionTimeGroupedByNode {
		a.queryLevelStats.ContentionTime += contentionTime
	}

	for _, estimatedRU := range a.nodeLevelStats.RUEstimateGroupedByNode {
		a.queryLevelStats.RUEstimate += estimatedRU
	}

	for _, cpuTime := range a.nodeLevelStats.CPUTimeGroupedByNode {
		a.queryLevelStats.CPUTime += cpuTime
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

		if err := analyzer.ProcessStats(); err != nil {
			errs = errors.CombineErrors(errs, err)
			continue
		}
		queryLevelStats.Accumulate(analyzer.GetQueryLevelStats())
	}
	queryLevelStats.ContentionEvents = getAllContentionEvents(trace)
	return queryLevelStats, errs
}
