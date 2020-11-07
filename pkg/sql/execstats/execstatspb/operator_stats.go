// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execstatspb

import (
	fmt "fmt"
	time "time"

	"github.com/dustin/go-humanize"
)

// IntValue stores an optional unsigned integer value, used in the OperatorStats
// message.
//
// The underlying value is the logical value plus 1, so that zero remains the
// special case of having no value.
type IntValue uint64

// MakeIntValue returns an IntValue with a set value.
func MakeIntValue(value uint64) IntValue {
	return IntValue(value + 1)
}

// HasValue returns true if a value was set.
func (i IntValue) HasValue() bool {
	return i != 0
}

// Value returns the current value, or 0 if HasValue() is false.
func (i IntValue) Value() uint64 {
	if i == 0 {
		return 0
	}
	return uint64(i - 1)
}

// Clear the value.
func (i *IntValue) Clear() {
	*i = 0
}

// Set the value.
func (i *IntValue) Set(value uint64) {
	*i = MakeIntValue(value)
}

// Add modifies the value by adding a delta.
func (i *IntValue) Add(delta int64) {
	*i = MakeIntValue(uint64(int64(i.Value()) + delta))
}

const (
	batchesOutputQueryPlanSuffix     = "batches output"
	tuplesOutputQueryPlanSuffix      = "tuples output"
	ioTimeQueryPlanSuffix            = "IO time"
	executionTimeQueryPlanSuffix     = "execution time"
	maxVecMemoryBytesQueryPlanSuffix = "max vectorized memory allocated"
	maxVecDiskBytesQueryPlanSuffix   = "max vectorized disk allocated"
	bytesReadQueryPlanSuffix         = "bytes read"
	rowsReadQueryPlanSuffix          = "rows read"
	networkLatencyQueryPlanSuffix    = "network latency"
)

// Stats is part of SpanStats interface.
func (s *OperatorStats) Stats() map[string]string {
	result := make(map[string]string, 4)
	s.formatStats(func(suffix string, value interface{}) {
		result[suffix] = fmt.Sprint(value)
	})
	return result
}

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (s *OperatorStats) StatsForQueryPlan() []string {
	result := make([]string, 0, 4)
	s.formatStats(func(suffix string, value interface{}) {
		result = append(result, fmt.Sprintf("%s: %v", suffix, value))
	})
	return result
}

// formatStats calls fn for each statistic that is set.
func (s *OperatorStats) formatStats(fn func(suffix string, value interface{})) {
	if s.NumBatches.HasValue() {
		fn(batchesOutputQueryPlanSuffix, s.NumBatches.Value())
	}
	if s.NumTuples.HasValue() {
		fn(tuplesOutputQueryPlanSuffix, s.NumTuples.Value())
	}
	if s.IOTime != 0 {
		fn(ioTimeQueryPlanSuffix, s.IOTime.Round(time.Microsecond))
	}
	if s.ExecTime != 0 {
		fn(executionTimeQueryPlanSuffix, s.ExecTime.Round(time.Microsecond))
	}

	if s.MaxAllocatedMem.HasValue() {
		fn(maxVecMemoryBytesQueryPlanSuffix, humanize.IBytes(s.MaxAllocatedMem.Value()))
	}
	if s.MaxAllocatedDisk.HasValue() {
		fn(maxVecDiskBytesQueryPlanSuffix, humanize.IBytes(s.MaxAllocatedDisk.Value()))
	}
	if s.BytesRead.HasValue() {
		fn(bytesReadQueryPlanSuffix, humanize.IBytes(s.BytesRead.Value()))
	}
	if s.RowsRead.HasValue() {
		fn(rowsReadQueryPlanSuffix, s.RowsRead.Value())
	}
	if s.NetworkLatency != 0 {
		fn(networkLatencyQueryPlanSuffix, s.NetworkLatency.Round(time.Microsecond))
	}
}
