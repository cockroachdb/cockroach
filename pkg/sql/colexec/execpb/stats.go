// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execpb

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var _ tracing.SpanStats = &VectorizedStats{}
var _ execinfrapb.DistSQLSpanStats = &VectorizedStats{}
var _ tracing.SpanStats = &VectorizedInboxStats{}
var _ execinfrapb.DistSQLSpanStats = &VectorizedInboxStats{}

const (
	batchesOutputTagSuffix     = "output.batches"
	tuplesOutputTagSuffix      = "output.tuples"
	ioTimeTagSuffix            = "time.io"
	executionTimeTagSuffix     = "time.execution"
	maxVecMemoryBytesTagSuffix = "mem.vectorized.max"
	maxVecDiskBytesTagSuffix   = "disk.vectorized.max"
	bytesReadTagSuffix         = "bytes.read"
	rowsReadTagSuffix          = "rows.read"
	networkLatencyTagSuffix    = "network.latency"
)

// Stats is part of SpanStats interface.
func (vs *VectorizedStats) Stats() map[string]string {
	var timeSuffix string
	if vs.IO {
		timeSuffix = ioTimeTagSuffix
	} else {
		timeSuffix = executionTimeTagSuffix
	}
	stats := map[string]string{
		batchesOutputTagSuffix:     fmt.Sprintf("%d", vs.NumBatches),
		tuplesOutputTagSuffix:      fmt.Sprintf("%d", vs.NumTuples),
		timeSuffix:                 fmt.Sprintf("%v", vs.Time.Round(time.Microsecond)),
		maxVecMemoryBytesTagSuffix: fmt.Sprintf("%d", vs.MaxAllocatedMem),
		maxVecDiskBytesTagSuffix:   fmt.Sprintf("%d", vs.MaxAllocatedDisk),
	}
	if vs.BytesRead != 0 {
		stats[bytesReadTagSuffix] = humanizeutil.IBytes(vs.BytesRead)
	}
	if vs.RowsRead != 0 {
		stats[rowsReadTagSuffix] = fmt.Sprintf("%d", vs.RowsRead)
	}
	return stats
}

// Stats is part of SpanStats interface.
func (vs *VectorizedInboxStats) Stats() map[string]string {
	stats := map[string]string{
		batchesOutputTagSuffix: fmt.Sprintf("%d", vs.BaseVectorizedStats.NumBatches),
		// TODO(cathymw): Have inbox collect its own deserialization time with a
		// timer and display deserialization time instead of ioTime.
		ioTimeTagSuffix: fmt.Sprintf("%v", vs.BaseVectorizedStats.Time.Round(time.Microsecond)),
	}
	if vs.BaseVectorizedStats.BytesRead != 0 {
		stats[bytesReadTagSuffix] = humanizeutil.IBytes(vs.BaseVectorizedStats.BytesRead)
	}
	if vs.BaseVectorizedStats.RowsRead != 0 {
		stats[rowsReadTagSuffix] = fmt.Sprintf("%d", vs.BaseVectorizedStats.RowsRead)
	}
	if vs.NetworkLatency != 0 {
		stats[networkLatencyTagSuffix] = fmt.Sprintf("%v", time.Duration(vs.NetworkLatency).Round(time.Microsecond))
	}
	return stats
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

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedStats) StatsForQueryPlan() []string {
	var timeSuffix string
	if vs.IO {
		timeSuffix = ioTimeQueryPlanSuffix
	} else {
		timeSuffix = executionTimeQueryPlanSuffix
	}
	stats := []string{
		fmt.Sprintf("%s: %d", batchesOutputQueryPlanSuffix, vs.NumBatches),
		fmt.Sprintf("%s: %d", tuplesOutputQueryPlanSuffix, vs.NumTuples),
		fmt.Sprintf("%s: %v", timeSuffix, vs.Time.Round(time.Microsecond)),
	}
	if vs.MaxAllocatedMem != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", maxVecMemoryBytesQueryPlanSuffix, humanizeutil.IBytes(vs.MaxAllocatedMem)))
	}
	if vs.MaxAllocatedDisk != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", maxVecDiskBytesQueryPlanSuffix, humanizeutil.IBytes(vs.MaxAllocatedDisk)))
	}
	if vs.BytesRead != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", bytesReadQueryPlanSuffix, humanizeutil.IBytes(vs.BytesRead)))
	}
	if vs.RowsRead != 0 {
		stats = append(stats, fmt.Sprintf("%s: %d", rowsReadQueryPlanSuffix, vs.RowsRead))
	}
	return stats
}

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedInboxStats) StatsForQueryPlan() []string {
	stats := []string{
		fmt.Sprintf("%s: %d", batchesOutputQueryPlanSuffix, vs.BaseVectorizedStats.NumBatches),
		fmt.Sprintf("%s: %v", ioTimeQueryPlanSuffix, vs.BaseVectorizedStats.Time.Round(time.Microsecond)),
	}
	if vs.BaseVectorizedStats.BytesRead != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %s", bytesReadQueryPlanSuffix, humanizeutil.IBytes(vs.BaseVectorizedStats.BytesRead)))
	}
	if vs.BaseVectorizedStats.RowsRead != 0 {
		stats = append(stats, fmt.Sprintf("%s: %d", rowsReadQueryPlanSuffix, vs.BaseVectorizedStats.RowsRead))
	}
	if vs.NetworkLatency != 0 {
		stats = append(stats,
			fmt.Sprintf("%s: %v", networkLatencyQueryPlanSuffix, time.Duration(vs.NetworkLatency).Round(time.Microsecond)))
	}
	return stats
}
