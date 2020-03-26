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

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

var _ tracing.SpanStats = &VectorizedStats{}
var _ execinfrapb.DistSQLSpanStats = &VectorizedStats{}

const (
	batchesOutputTagSuffix     = "output.batches"
	tuplesOutputTagSuffix      = "output.tuples"
	selectivityTagSuffix       = "selectivity"
	stallTimeTagSuffix         = "time.stall"
	executionTimeTagSuffix     = "time.execution"
	maxVecMemoryBytesTagSuffix = "mem.vectorized.max"
	maxVecDiskBytesTagSuffix   = "disk.vectorized.max"
)

// Stats is part of SpanStats interface.
func (vs *VectorizedStats) Stats() map[string]string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeTagSuffix
	} else {
		timeSuffix = executionTimeTagSuffix
	}
	selectivity := float64(0)
	if vs.NumBatches > 0 {
		selectivity = float64(vs.NumTuples) / float64(int64(coldata.BatchSize())*vs.NumBatches)
	}
	return map[string]string{
		batchesOutputTagSuffix:     fmt.Sprintf("%d", vs.NumBatches),
		tuplesOutputTagSuffix:      fmt.Sprintf("%d", vs.NumTuples),
		selectivityTagSuffix:       fmt.Sprintf("%.2f", selectivity),
		timeSuffix:                 fmt.Sprintf("%v", vs.Time.Round(time.Microsecond)),
		maxVecMemoryBytesTagSuffix: fmt.Sprintf("%d", vs.MaxAllocatedMem),
		maxVecDiskBytesTagSuffix:   fmt.Sprintf("%d", vs.MaxAllocatedDisk),
	}
}

const (
	batchesOutputQueryPlanSuffix     = "batches output"
	tuplesOutputQueryPlanSuffix      = "tuples output"
	selectivityQueryPlanSuffix       = "selectivity"
	stallTimeQueryPlanSuffix         = "stall time"
	executionTimeQueryPlanSuffix     = "execution time"
	maxVecMemoryBytesQueryPlanSuffix = "max vectorized memory allocated"
	maxVecDiskBytesQueryPlanSuffix   = "max vectorized disk allocated"
)

// StatsForQueryPlan is part of DistSQLSpanStats interface.
func (vs *VectorizedStats) StatsForQueryPlan() []string {
	var timeSuffix string
	if vs.Stall {
		timeSuffix = stallTimeQueryPlanSuffix
	} else {
		timeSuffix = executionTimeQueryPlanSuffix
	}
	selectivity := float64(0)
	if vs.NumBatches > 0 {
		selectivity = float64(vs.NumTuples) / float64(int64(coldata.BatchSize())*vs.NumBatches)
	}
	stats := []string{
		fmt.Sprintf("%s: %d", batchesOutputQueryPlanSuffix, vs.NumBatches),
		fmt.Sprintf("%s: %d", tuplesOutputQueryPlanSuffix, vs.NumTuples),
		fmt.Sprintf("%s: %.2f", selectivityQueryPlanSuffix, selectivity),
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
	return stats
}
