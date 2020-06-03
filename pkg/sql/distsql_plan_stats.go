// Copyright 2017 The Cockroach Authors.
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
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type requestedStat struct {
	columns             []sqlbase.ColumnID
	histogram           bool
	histogramMaxBuckets int
	name                string
	inverted            bool
}

const histogramSamples = 10000
const histogramBuckets = 200

// maxTimestampAge is the maximum allowed age of a scan timestamp during table
// stats collection, used when creating statistics AS OF SYSTEM TIME. The
// timestamp is advanced during long operations as needed. See TableReaderSpec.
//
// The lowest TTL we recommend is 10 minutes. This value must be be lower than
// that.
var maxTimestampAge = settings.RegisterDurationSetting(
	"sql.stats.max_timestamp_age",
	"maximum age of timestamp during table statistics collection",
	5*time.Minute,
)

func (dsp *DistSQLPlanner) createStatsPlan(
	planCtx *PlanningCtx,
	desc *sqlbase.ImmutableTableDescriptor,
	reqStats []requestedStat,
	job *jobs.Job,
) (*PhysicalPlan, error) {
	if len(reqStats) == 0 {
		return nil, errors.New("no stats requested")
	}

	details := job.Details().(jobspb.CreateStatsDetails)

	// Calculate the set of columns we need to scan.
	var colCfg scanColumnsConfig
	var tableColSet util.FastIntSet
	for _, s := range reqStats {
		for _, c := range s.columns {
			if !tableColSet.Contains(int(c)) {
				tableColSet.Add(int(c))
				colCfg.wantedColumns = append(colCfg.wantedColumns, tree.ColumnID(c))
			}
		}
	}

	// Create the table readers; for this we initialize a dummy scanNode.
	scan := scanNode{desc: desc}
	err := scan.initDescDefaults(colCfg)
	if err != nil {
		return nil, err
	}
	sb := span.MakeBuilder(planCtx.planner.ExecCfg().Codec, desc.TableDesc(), scan.index)
	scan.spans, err = sb.UnconstrainedSpans()
	if err != nil {
		return nil, err
	}
	scan.isFull = true

	p, err := dsp.createTableReaders(planCtx, &scan)
	if err != nil {
		return nil, err
	}

	if details.AsOf != nil {
		// If the read is historical, set the max timestamp age.
		val := maxTimestampAge.Get(&dsp.st.SV)
		for i := range p.Processors {
			spec := p.Processors[i].Spec.Core.TableReader
			spec.MaxTimestampAgeNanos = uint64(val)
		}
	}

	var sketchSpecs, invSketchSpecs []execinfrapb.SketchSpec
	sampledColumnIDs := make([]sqlbase.ColumnID, len(scan.cols))
	for _, s := range reqStats {
		spec := execinfrapb.SketchSpec{
			SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
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
			streamColIdx := uint32(p.PlanToStreamColMap[colIdx])
			spec.Columns[i] = streamColIdx
			sampledColumnIDs[streamColIdx] = colID
		}
		if s.inverted {
			invSketchSpecs = append(invSketchSpecs, spec)
		} else {
			sketchSpecs = append(sketchSpecs, spec)
		}
	}

	// Set up the samplers.
	sampler := &execinfrapb.SamplerSpec{
		Sketches:         sketchSpecs,
		InvertedSketches: invSketchSpecs,
	}
	for _, s := range reqStats {
		sampler.MaxFractionIdle = details.MaxFractionIdle
		if s.histogram {
			sampler.SampleSize = histogramSamples
		}
	}

	// The sampler outputs the original columns plus a rank column, four
	// sketch columns, and two inverted histogram columns.
	outTypes := make([]*types.T, 0, len(p.ResultTypes)+5)
	outTypes = append(outTypes, p.ResultTypes...)
	// An INT column for the rank of each row.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the sketch index.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the number of rows processed.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	outTypes = append(outTypes, types.Int)
	// A BYTES column with the sketch data.
	outTypes = append(outTypes, types.Bytes)
	// An INT column indicating the inverted sketch index.
	outTypes = append(outTypes, types.Int)
	// A BYTES column with the inverted index key datum.
	outTypes = append(outTypes, types.Bytes)

	p.AddNoGroupingStage(
		execinfrapb.ProcessorCoreUnion{Sampler: sampler},
		execinfrapb.PostProcessSpec{},
		outTypes,
		execinfrapb.Ordering{},
	)

	// Estimate the expected number of rows based on existing stats in the cache.
	tableStats, err := planCtx.planner.execCfg.TableStatsCache.GetTableStats(planCtx.ctx, desc.ID)
	if err != nil {
		return nil, err
	}

	var rowsExpected uint64
	if len(tableStats) > 0 {
		overhead := stats.AutomaticStatisticsFractionStaleRows.Get(&dsp.st.SV)
		// Convert to a signed integer first to make the linter happy.
		rowsExpected = uint64(int64(
			// The total expected number of rows is the same number that was measured
			// most recently, plus some overhead for possible insertions.
			float64(tableStats[0].RowCount) * (1 + overhead),
		))
	}

	var jobID int64
	if job.ID() != nil {
		jobID = *job.ID()
	}

	// Set up the final SampleAggregator stage.
	agg := &execinfrapb.SampleAggregatorSpec{
		Sketches:         sketchSpecs,
		InvertedSketches: invSketchSpecs,
		SampleSize:       sampler.SampleSize,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.ID,
		JobID:            jobID,
		RowsExpected:     rowsExpected,
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.gatewayNodeID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].Node
	}
	p.AddSingleGroupStage(
		node,
		execinfrapb.ProcessorCoreUnion{SampleAggregator: agg},
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
	)

	return p, nil
}

func (dsp *DistSQLPlanner) createPlanForCreateStats(
	planCtx *PlanningCtx, job *jobs.Job,
) (*PhysicalPlan, error) {
	details := job.Details().(jobspb.CreateStatsDetails)
	reqStats := make([]requestedStat, len(details.ColumnStats))
	histogramCollectionEnabled := stats.HistogramClusterMode.Get(&dsp.st.SV)
	for i := 0; i < len(reqStats); i++ {
		histogram := details.ColumnStats[i].HasHistogram && histogramCollectionEnabled
		reqStats[i] = requestedStat{
			columns:             details.ColumnStats[i].ColumnIDs,
			histogram:           histogram,
			histogramMaxBuckets: histogramBuckets,
			name:                details.Name,
			inverted:            details.ColumnStats[i].Inverted,
		}
	}

	tableDesc := sqlbase.NewImmutableTableDescriptor(details.Table)
	return dsp.createStatsPlan(planCtx, tableDesc, reqStats, job)
}

func (dsp *DistSQLPlanner) planAndRunCreateStats(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	txn *kv.Txn,
	job *jobs.Job,
	resultRows *RowResultWriter,
) error {
	ctx = logtags.AddTag(ctx, "create-stats-distsql", nil)

	physPlan, err := dsp.createPlanForCreateStats(planCtx, job)
	if err != nil {
		return err
	}

	dsp.FinalizePlan(planCtx, physPlan)

	recv := MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.DDL,
		evalCtx.ExecCfg.RangeDescriptorCache,
		txn,
		func(ts hlc.Timestamp) {
			evalCtx.ExecCfg.Clock.Update(ts)
		},
		evalCtx.Tracing,
	)
	defer recv.Release()

	dsp.Run(planCtx, txn, physPlan, recv, evalCtx, nil /* finishedSetupFn */)()
	return resultRows.Err()
}
