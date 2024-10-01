// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"github.com/gogo/protobuf/types"
)

// ProcessorComponentID returns a ComponentID for the given processor in a flow.
func ProcessorComponentID(
	instanceID base.SQLInstanceID, flowID FlowID, processorID int32,
) ComponentID {
	return ComponentID{
		FlowID:        flowID,
		Type:          ComponentID_PROCESSOR,
		ID:            processorID,
		SQLInstanceID: instanceID,
	}
}

// StreamComponentID returns a ComponentID for the given stream in a flow.
func StreamComponentID(
	originInstanceID base.SQLInstanceID, flowID FlowID, streamID StreamID,
) ComponentID {
	return ComponentID{
		FlowID:        flowID,
		Type:          ComponentID_STREAM,
		ID:            int32(streamID),
		SQLInstanceID: originInstanceID,
	}
}

// FlowComponentID returns a ComponentID for the given flow.
func FlowComponentID(instanceID base.SQLInstanceID, flowID FlowID, region string) ComponentID {
	return ComponentID{
		FlowID:        flowID,
		Type:          ComponentID_FLOW,
		SQLInstanceID: instanceID,
		Region:        region,
	}
}

const (
	// FlowIDTagKey is the key used for flow id tags in tracing spans.
	FlowIDTagKey = "cockroach.flowid"

	// StreamIDTagKey is the key used for stream id tags in tracing spans.
	StreamIDTagKey = "cockroach.streamid"

	// ProcessorIDTagKey is the key used for processor id tags in tracing spans.
	ProcessorIDTagKey = "cockroach.processorid"
)

// StatsForQueryPlan returns the statistics as a list of strings that can be
// displayed in query plans and diagrams.
func (s *ComponentStats) StatsForQueryPlan() []string {
	result := make([]string, 0, 4)
	s.formatStats(func(key string, value interface{}) {
		if value != nil {
			result = append(result, fmt.Sprintf("%s: %v", key, value))
		} else {
			result = append(result, key)
		}
	})
	return result
}

// String implements fmt.Stringer and protoutil.Message.
func (s *ComponentStats) String() string {
	return redact.StringWithoutMarkers(s)
}

var _ redact.SafeFormatter = (*ComponentStats)(nil)

// SafeValue implements redact.SafeValue.
func (ComponentID_Type) SafeValue() {}

// SafeFormat implements redact.SafeFormatter.
func (s *ComponentStats) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("ComponentStats{ID: %v", s.Component)
	s.formatStats(func(key string, value interface{}) {
		if value != nil {
			w.Printf(", %s: %v", redact.SafeString(key), value)
		} else {
			w.Printf(", %s", redact.SafeString(key))
		}
	})
	w.SafeRune('}')
}

func printNodeIDs(nodeIDs []int32) redact.SafeString {
	var sb strings.Builder
	for i, id := range nodeIDs {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("n%d", id))
	}
	return redact.SafeString(sb.String())
}

// formatStats calls fn for each statistic that is set. value can be nil.
func (s *ComponentStats) formatStats(fn func(suffix string, value interface{})) {
	// Network Rx stats.
	if s.NetRx.Latency.HasValue() {
		fn("network latency", humanizeutil.Duration(s.NetRx.Latency.Value()))
	}
	if s.NetRx.WaitTime.HasValue() {
		fn("network wait time", humanizeutil.Duration(s.NetRx.WaitTime.Value()))
	}
	if s.NetRx.DeserializationTime.HasValue() {
		fn("deserialization time", humanizeutil.Duration(s.NetRx.DeserializationTime.Value()))
	}
	if s.NetRx.TuplesReceived.HasValue() {
		fn("network rows received", humanizeutil.Count(s.NetRx.TuplesReceived.Value()))
	}
	if s.NetRx.BytesReceived.HasValue() {
		fn("network bytes received", humanize.IBytes(s.NetRx.BytesReceived.Value()))
	}
	if s.NetRx.MessagesReceived.HasValue() {
		fn("network messages received", humanizeutil.Count(s.NetRx.MessagesReceived.Value()))
	}

	// Network Tx stats.
	if s.NetTx.TuplesSent.HasValue() {
		fn("network rows sent", humanizeutil.Count(s.NetTx.TuplesSent.Value()))
	}
	if s.NetTx.BytesSent.HasValue() {
		fn("network bytes sent", humanize.IBytes(s.NetTx.BytesSent.Value()))
	}
	if s.NetTx.MessagesSent.HasValue() {
		fn("network messages sent", humanizeutil.Count(s.NetTx.MessagesSent.Value()))
	}

	// Input stats.
	switch len(s.Inputs) {
	case 1:
		if s.Inputs[0].NumTuples.HasValue() {
			fn("input rows", humanizeutil.Count(s.Inputs[0].NumTuples.Value()))
		}
		if s.Inputs[0].WaitTime.HasValue() {
			fn("input stall time", humanizeutil.Duration(s.Inputs[0].WaitTime.Value()))
		}

	case 2:
		if s.Inputs[0].NumTuples.HasValue() {
			fn("left rows", humanizeutil.Count(s.Inputs[0].NumTuples.Value()))
		}
		if s.Inputs[0].WaitTime.HasValue() {
			fn("left stall time", humanizeutil.Duration(s.Inputs[0].WaitTime.Value()))
		}
		if s.Inputs[1].NumTuples.HasValue() {
			fn("right rows", humanizeutil.Count(s.Inputs[1].NumTuples.Value()))
		}
		if s.Inputs[1].WaitTime.HasValue() {
			fn("right stall time", humanizeutil.Duration(s.Inputs[1].WaitTime.Value()))
		}
	}

	// KV stats.
	if s.KV.UsedFollowerRead {
		fn("used follower read", nil)
	}
	if len(s.KV.NodeIDs) > 0 {
		fn("KV nodes", printNodeIDs(s.KV.NodeIDs))
	}
	if len(s.KV.Regions) > 0 {
		fn("KV regions", strings.Join(s.KV.Regions, ", "))
	}
	if s.KV.KVTime.HasValue() {
		fn("KV time", humanizeutil.Duration(s.KV.KVTime.Value()))
	}
	if s.KV.ContentionTime.HasValue() {
		fn("KV contention time", humanizeutil.Duration(s.KV.ContentionTime.Value()))
	}
	if s.KV.TuplesRead.HasValue() {
		fn("KV rows decoded", humanizeutil.Count(s.KV.TuplesRead.Value()))
	}
	if s.KV.BytesRead.HasValue() {
		fn("KV bytes read", humanize.IBytes(s.KV.BytesRead.Value()))
	}
	if s.KV.BatchRequestsIssued.HasValue() {
		fn("KV gRPC calls", humanizeutil.Count(s.KV.BatchRequestsIssued.Value()))
	}
	if s.KV.KVPairsRead.HasValue() {
		fn("KV pairs read", humanizeutil.Count(s.KV.KVPairsRead.Value()))
	}
	if s.KV.NumInterfaceSteps.HasValue() {
		fn("MVCC step count (ext/int)",
			fmt.Sprintf("%s/%s",
				humanizeutil.Count(s.KV.NumInterfaceSteps.Value()),
				humanizeutil.Count(s.KV.NumInternalSteps.Value())),
		)
	}
	if s.KV.NumInterfaceSeeks.HasValue() {
		fn("MVCC seek count (ext/int)",
			fmt.Sprintf("%s/%s",
				humanizeutil.Count(s.KV.NumInterfaceSeeks.Value()),
				humanizeutil.Count(s.KV.NumInternalSeeks.Value())),
		)
	}
	if s.KV.UsedStreamer {
		fn("used streamer", nil)
	}

	// Exec stats.
	if s.Exec.ExecTime.HasValue() {
		fn("execution time", humanizeutil.Duration(s.Exec.ExecTime.Value()))
	}
	if s.Exec.MaxAllocatedMem.HasValue() {
		fn("max memory allocated", humanize.IBytes(s.Exec.MaxAllocatedMem.Value()))
	}
	if s.Exec.MaxAllocatedDisk.HasValue() {
		fn("max sql temp disk usage", humanize.IBytes(s.Exec.MaxAllocatedDisk.Value()))
	}
	if s.Exec.CPUTime.HasValue() {
		fn("sql cpu time", humanizeutil.Duration(s.Exec.CPUTime.Value()))
	}

	// Output stats.
	if s.Output.NumBatches.HasValue() {
		fn("batches output", humanizeutil.Count(s.Output.NumBatches.Value()))
	}
	if s.Output.NumTuples.HasValue() {
		fn("rows output", humanizeutil.Count(s.Output.NumTuples.Value()))
	}
}

// Union creates a new ComponentStats that contains all statistics in either the
// receiver (s) or the argument (other).
// If a statistic is set in both, the one in the receiver (s) is preferred.
func (s *ComponentStats) Union(other *ComponentStats) *ComponentStats {
	result := *s

	// Network Rx stats.
	if !result.NetRx.Latency.HasValue() {
		result.NetRx.Latency = other.NetRx.Latency
	}
	if !result.NetRx.WaitTime.HasValue() {
		result.NetRx.WaitTime = other.NetRx.WaitTime
	}
	if !result.NetRx.DeserializationTime.HasValue() {
		result.NetRx.DeserializationTime = other.NetRx.DeserializationTime
	}
	if !result.NetRx.TuplesReceived.HasValue() {
		result.NetRx.TuplesReceived = other.NetRx.TuplesReceived
	}
	if !result.NetRx.BytesReceived.HasValue() {
		result.NetRx.BytesReceived = other.NetRx.BytesReceived
	}
	if !result.NetRx.MessagesReceived.HasValue() {
		result.NetRx.MessagesReceived = other.NetRx.MessagesReceived
	}

	// Network Tx stats.
	if !result.NetTx.TuplesSent.HasValue() {
		result.NetTx.TuplesSent = other.NetTx.TuplesSent
	}
	if !result.NetTx.BytesSent.HasValue() {
		result.NetTx.BytesSent = other.NetTx.BytesSent
	}

	// Input stats. Make sure we don't reuse slices.
	result.Inputs = append([]InputStats(nil), s.Inputs...)
	result.Inputs = append(result.Inputs, other.Inputs...)

	// KV stats.
	if len(other.KV.NodeIDs) != 0 {
		result.KV.NodeIDs = util.CombineUnique(result.KV.NodeIDs, other.KV.NodeIDs)
	}
	if len(other.KV.Regions) != 0 {
		result.KV.Regions = util.CombineUnique(result.KV.Regions, other.KV.Regions)
	}
	result.KV.UsedFollowerRead = result.KV.UsedFollowerRead || other.KV.UsedFollowerRead
	if !result.KV.KVTime.HasValue() {
		result.KV.KVTime = other.KV.KVTime
	}
	if !result.KV.ContentionTime.HasValue() {
		result.KV.ContentionTime = other.KV.ContentionTime
	}
	if !result.KV.NumInterfaceSteps.HasValue() {
		result.KV.NumInterfaceSteps = other.KV.NumInterfaceSteps
	}
	if !result.KV.NumInternalSteps.HasValue() {
		result.KV.NumInternalSteps = other.KV.NumInternalSteps
	}
	if !result.KV.NumInterfaceSeeks.HasValue() {
		result.KV.NumInterfaceSeeks = other.KV.NumInterfaceSeeks
	}
	if !result.KV.NumInternalSeeks.HasValue() {
		result.KV.NumInternalSeeks = other.KV.NumInternalSeeks
	}
	if !result.KV.BlockBytes.HasValue() {
		result.KV.BlockBytes = other.KV.BlockBytes
	}
	if !result.KV.BlockBytesInCache.HasValue() {
		result.KV.BlockBytesInCache = other.KV.BlockBytesInCache
	}
	if !result.KV.KeyBytes.HasValue() {
		result.KV.KeyBytes = other.KV.KeyBytes
	}
	if !result.KV.ValueBytes.HasValue() {
		result.KV.ValueBytes = other.KV.ValueBytes
	}
	if !result.KV.PointCount.HasValue() {
		result.KV.PointCount = other.KV.PointCount
	}
	if !result.KV.PointsCoveredByRangeTombstones.HasValue() {
		result.KV.PointsCoveredByRangeTombstones = other.KV.PointsCoveredByRangeTombstones
	}
	if !result.KV.RangeKeyCount.HasValue() {
		result.KV.RangeKeyCount = other.KV.RangeKeyCount
	}
	if !result.KV.RangeKeyContainedPoints.HasValue() {
		result.KV.RangeKeyContainedPoints = other.KV.RangeKeyContainedPoints
	}
	if !result.KV.RangeKeySkippedPoints.HasValue() {
		result.KV.RangeKeySkippedPoints = other.KV.RangeKeySkippedPoints
	}
	if !result.KV.TuplesRead.HasValue() {
		result.KV.TuplesRead = other.KV.TuplesRead
	}
	if !result.KV.BytesRead.HasValue() {
		result.KV.BytesRead = other.KV.BytesRead
	}
	if !result.KV.BatchRequestsIssued.HasValue() {
		result.KV.BatchRequestsIssued = other.KV.BatchRequestsIssued
	}
	if !result.KV.KVPairsRead.HasValue() {
		result.KV.KVPairsRead = other.KV.KVPairsRead
	}

	// Exec stats.
	if !result.Exec.ExecTime.HasValue() {
		result.Exec.ExecTime = other.Exec.ExecTime
	}
	if !result.Exec.MaxAllocatedMem.HasValue() {
		result.Exec.MaxAllocatedMem = other.Exec.MaxAllocatedMem
	}
	if !result.Exec.MaxAllocatedDisk.HasValue() {
		result.Exec.MaxAllocatedDisk = other.Exec.MaxAllocatedDisk
	}
	if !result.Exec.ConsumedRU.HasValue() {
		result.Exec.ConsumedRU = other.Exec.ConsumedRU
	}
	if !result.Exec.CPUTime.HasValue() {
		result.Exec.CPUTime = other.Exec.CPUTime
	}

	// Output stats.
	if !result.Output.NumBatches.HasValue() {
		result.Output.NumBatches = other.Output.NumBatches
	}
	if !result.Output.NumTuples.HasValue() {
		result.Output.NumTuples = other.Output.NumTuples
	}

	// Flow stats.
	if !result.FlowStats.MaxMemUsage.HasValue() {
		result.FlowStats.MaxMemUsage = other.FlowStats.MaxMemUsage
	}
	if !result.FlowStats.MaxDiskUsage.HasValue() {
		result.FlowStats.MaxDiskUsage = other.FlowStats.MaxDiskUsage
	}
	if !result.FlowStats.ConsumedRU.HasValue() {
		result.FlowStats.ConsumedRU = other.FlowStats.ConsumedRU
	}

	return &result
}

// MakeDeterministic is used only for testing; it modifies any non-deterministic
// statistics like elapsed time or exact number of bytes to fixed or
// manufactured values.
//
// Note that it does not modify which fields that are set. In other words, a
// field will have a non-zero protobuf value iff it had a non-zero protobuf
// value before. This allows tests to verify the set of stats that were
// collected.
func (s *ComponentStats) MakeDeterministic() {
	// resetUint resets an optional.Uint to 0, if it was set.
	resetUint := func(v *optional.Uint) {
		if v.HasValue() {
			v.Set(0)
		}
	}
	// timeVal resets a duration to 1ns, if it was set.
	timeVal := func(v *optional.Duration) {
		if v.HasValue() {
			v.Set(0)
		}
	}

	// NetRx.
	timeVal(&s.NetRx.Latency)
	timeVal(&s.NetRx.WaitTime)
	timeVal(&s.NetRx.DeserializationTime)
	if s.NetRx.BytesReceived.HasValue() {
		// BytesReceived can be non-deterministic because some message fields have
		// varying sizes across different runs (e.g. metadata). Override to a useful
		// value for tests.
		s.NetRx.BytesReceived.Set(8 * s.NetRx.TuplesReceived.Value())
	}
	if s.NetRx.MessagesReceived.HasValue() {
		// Override to a useful value for tests.
		s.NetRx.MessagesReceived.Set(s.NetRx.TuplesReceived.Value() / 2)
	}

	// NetTx.
	if s.NetTx.BytesSent.HasValue() {
		// BytesSent can be non-deterministic because some message fields have
		// varying sizes across different runs (e.g. metadata). Override to a useful
		// value for tests.
		s.NetTx.BytesSent.Set(8 * s.NetTx.TuplesSent.Value())
	}
	if s.NetTx.MessagesSent.HasValue() {
		// Override to a useful value for tests.
		s.NetTx.MessagesSent.Set(s.NetTx.TuplesSent.Value() / 2)
	}

	// KV.
	timeVal(&s.KV.KVTime)
	timeVal(&s.KV.ContentionTime)
	resetUint(&s.KV.NumInterfaceSteps)
	resetUint(&s.KV.NumInternalSteps)
	resetUint(&s.KV.NumInterfaceSeeks)
	resetUint(&s.KV.NumInternalSeeks)
	resetUint(&s.KV.BlockBytes)
	resetUint(&s.KV.BlockBytesInCache)
	resetUint(&s.KV.KeyBytes)
	resetUint(&s.KV.ValueBytes)
	resetUint(&s.KV.PointCount)
	resetUint(&s.KV.PointsCoveredByRangeTombstones)
	resetUint(&s.KV.RangeKeyCount)
	resetUint(&s.KV.RangeKeyContainedPoints)
	resetUint(&s.KV.RangeKeySkippedPoints)
	if s.KV.BytesRead.HasValue() {
		// BytesRead is overridden to a useful value for tests.
		s.KV.BytesRead.Set(8 * s.KV.TuplesRead.Value())
	}
	if s.KV.KVPairsRead.HasValue() {
		// KVPairsRead is overridden to a useful value for tests. Note that it
		// is a double of the "tuples read" so that it wouldn't be hidden in
		// the EXPLAIN output.
		s.KV.KVPairsRead.Set(2 * s.KV.TuplesRead.Value())
	}
	if s.KV.BatchRequestsIssued.HasValue() {
		// BatchRequestsIssued is overridden to a useful value for tests.
		s.KV.BatchRequestsIssued.Set(s.KV.TuplesRead.Value())
	}
	if len(s.KV.NodeIDs) > 0 {
		// The nodes can be non-deterministic because they depend on the actual
		// cluster configuration. Override to a useful value for tests.
		s.KV.NodeIDs = []int32{1}
	}
	if len(s.KV.Regions) > 0 {
		s.KV.Regions = []string{"test"}
	}

	// Exec.
	timeVal(&s.Exec.ExecTime)
	resetUint(&s.Exec.MaxAllocatedMem)
	resetUint(&s.Exec.MaxAllocatedDisk)
	resetUint(&s.Exec.ConsumedRU)
	if s.Exec.CPUTime.HasValue() {
		// The CPU time won't be set on all platforms, so we can't output it when
		// determinism is required.
		s.Exec.CPUTime.Clear()
	}

	// Output.
	resetUint(&s.Output.NumBatches)

	// Inputs.
	for i := range s.Inputs {
		timeVal(&s.Inputs[i].WaitTime)
	}
}

// ExtractStatsFromSpans extracts all ComponentStats from a set of tracing
// spans.
func ExtractStatsFromSpans(
	spans []tracingpb.RecordedSpan, makeDeterministic bool,
) map[ComponentID]*ComponentStats {
	statsMap := make(map[ComponentID]*ComponentStats)
	// componentStats is only used to check whether a structured payload item is
	// of ComponentStats type.
	var componentStats ComponentStats
	for i := range spans {
		span := &spans[i]
		span.Structured(func(item *types.Any, _ time.Time) {
			if !types.Is(item, &componentStats) {
				return
			}
			var stats ComponentStats
			if err := protoutil.Unmarshal(item.Value, &stats); err != nil {
				return
			}
			if stats.Component == (ComponentID{}) {
				return
			}
			if makeDeterministic {
				stats.MakeDeterministic()
			}
			existing := statsMap[stats.Component]
			if existing == nil {
				statsMap[stats.Component] = &stats
			} else {
				// In the vectorized flow we can have multiple statistics
				// entries for one componentID because a single processor is
				// represented by multiple components (e.g. when hash/merge
				// joins have an ON expression that is not supported natively -
				// we will plan the row-execution filterer processor then).
				//
				// Merge the stats together.
				// TODO(yuzefovich): remove this once such edge cases are no
				// longer present.
				statsMap[stats.Component] = existing.Union(&stats)
			}
		})
	}
	return statsMap
}
