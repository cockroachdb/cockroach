// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	sqlstats "github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/pkg/errors"
)

type requestedStat struct {
	columns             []sqlbase.ColumnID
	histogram           bool
	histogramMaxBuckets int
	name                string
}

const histogramSamples = 10000
const histogramBuckets = 200

func (dsp *DistSQLPlanner) createStatsPlan(
	planCtx *PlanningCtx,
	desc *sqlbase.ImmutableTableDescriptor,
	stats []requestedStat,
	job *jobs.Job,
) (PhysicalPlan, error) {
	if len(stats) == 0 {
		return PhysicalPlan{}, errors.New("no stats requested")
	}

	details := job.Details().(jobspb.CreateStatsDetails)

	// Calculate the set of columns we need to scan.
	var colCfg scanColumnsConfig
	var tableColSet util.FastIntSet
	for _, s := range stats {
		for _, c := range s.columns {
			if !tableColSet.Contains(int(c)) {
				tableColSet.Add(int(c))
				colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(c))
			}
		}
	}

	// Create the table readers; for this we initialize a dummy scanNode.
	scan := scanNode{desc: desc}
	err := scan.initDescDefaults(nil /* planDependencies */, colCfg)
	if err != nil {
		return PhysicalPlan{}, err
	}
	scan.spans, err = unconstrainedSpans(desc, scan.index, scan.isDeleteSource)
	if err != nil {
		return PhysicalPlan{}, err
	}

	p, err := dsp.createTableReaders(planCtx, &scan, nil /* overrideResultColumns */)
	if err != nil {
		return PhysicalPlan{}, err
	}

	sketchSpecs := make([]distsqlpb.SketchSpec, len(stats))
	sampledColumnIDs := make([]sqlbase.ColumnID, scan.valNeededForCol.Len())
	for i, s := range stats {
		spec := distsqlpb.SketchSpec{
			SketchType:          distsqlpb.SketchType_HLL_PLUS_PLUS_V1,
			GenerateHistogram:   s.histogram,
			HistogramMaxBuckets: uint32(s.histogramMaxBuckets),
			Columns:             make([]uint32, len(s.columns)),
			StatName:            s.name,
		}
		for i, colID := range s.columns {
			colIdx, ok := scan.colIdxMap[colID]
			if !ok {
				panic("necessary column not scanned")
			}
			streamColIdx := p.PlanToStreamColMap[colIdx]
			spec.Columns[i] = uint32(streamColIdx)
			sampledColumnIDs[streamColIdx] = colID
		}

		sketchSpecs[i] = spec
	}

	// Set up the samplers.
	sampler := &distsqlpb.SamplerSpec{Sketches: sketchSpecs}
	for _, s := range stats {
		sampler.MaxFractionIdle = details.MaxFractionIdle
		if s.histogram {
			sampler.SampleSize = histogramSamples
		}
	}

	// The sampler outputs the original columns plus a rank column and four sketch columns.
	outTypes := make([]types.T, 0, len(p.ResultTypes)+5)
	outTypes = append(outTypes, p.ResultTypes...)
	// An INT column for the rank of each row.
	outTypes = append(outTypes, *types.Int)
	// An INT column indicating the sketch index.
	outTypes = append(outTypes, *types.Int)
	// An INT column indicating the number of rows processed.
	outTypes = append(outTypes, *types.Int)
	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	outTypes = append(outTypes, *types.Int)
	// A BYTES column with the sketch data.
	outTypes = append(outTypes, *types.Bytes)

	p.AddNoGroupingStage(
		distsqlpb.ProcessorCoreUnion{Sampler: sampler},
		distsqlpb.PostProcessSpec{},
		outTypes,
		distsqlpb.Ordering{},
	)

	// Estimate the expected number of rows based on existing stats in the cache.
	tableStats, err := planCtx.planner.execCfg.TableStatsCache.GetTableStats(planCtx.ctx, desc.ID)
	if err != nil {
		return PhysicalPlan{}, err
	}

	var rowsExpected uint64
	if len(tableStats) > 0 {
		overhead := sqlstats.AutomaticStatisticsFractionStaleRows.Get(&dsp.st.SV)
		// Convert to a signed integer first to make the linter happy.
		rowsExpected = uint64(int64(
			// The total expected number of rows is the same number that was measured
			// most recently, plus some overhead for possible insertions.
			float64(tableStats[0].RowCount) * (1 + overhead),
		))
	}

	// Set up the final SampleAggregator stage.
	agg := &distsqlpb.SampleAggregatorSpec{
		Sketches:         sketchSpecs,
		SampleSize:       sampler.SampleSize,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.ID,
		JobID:            *job.ID(),
		RowsExpected:     rowsExpected,
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.nodeDesc.NodeID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].Node
	}
	p.AddSingleGroupStage(
		node,
		distsqlpb.ProcessorCoreUnion{SampleAggregator: agg},
		distsqlpb.PostProcessSpec{},
		[]types.T{},
	)

	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForCreateStats(
	planCtx *PlanningCtx, job *jobs.Job,
) (PhysicalPlan, error) {
	details := job.Details().(jobspb.CreateStatsDetails)
	stats := make([]requestedStat, len(details.ColumnLists))
	for i := 0; i < len(stats); i++ {
		// Currently we do not use histograms, so don't bother creating one.
		// When this changes, we can only use it for single-column stats.
		histogram := false
		stats[i] = requestedStat{
			columns:             details.ColumnLists[i].IDs,
			histogram:           histogram,
			histogramMaxBuckets: histogramBuckets,
			name:                details.Name,
		}
	}

	tableDesc := sqlbase.NewImmutableTableDescriptor(details.Table)
	return dsp.createStatsPlan(planCtx, tableDesc, stats, job)
}

func (dsp *DistSQLPlanner) planAndRunCreateStats(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *client.Txn,
	job *jobs.Job,
	resultRows *RowResultWriter,
) error {
	ctx = logtags.AddTag(ctx, "create-stats-distsql", nil)

	physPlan, err := dsp.createPlanForCreateStats(planCtx, job)
	if err != nil {
		return err
	}

	dsp.FinalizePlan(planCtx, &physPlan)

	recv := MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.DDL,
		evalCtx.ExecCfg.RangeDescriptorCache,
		evalCtx.ExecCfg.LeaseHolderCache,
		txn,
		func(ts hlc.Timestamp) {
			_ = evalCtx.ExecCfg.Clock.Update(ts)
		},
		evalCtx.Tracing,
	)
	defer recv.Release()

	dsp.Run(planCtx, txn, &physPlan, recv, evalCtx, nil /* finishedSetupFn */)
	return resultRows.Err()
}
