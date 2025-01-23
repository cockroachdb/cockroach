// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/stats/bounds"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

type requestedStat struct {
	columns             []descpb.ColumnID
	histogram           bool
	histogramMaxBuckets uint32
	name                string
	inverted            bool
}

// histogramSamples is the number of sample rows to be collected for histogram
// construction. For larger tables, it may be beneficial to increase this number
// to get a more accurate distribution. The default value is 0, which means that
// we will automatically pick a reasonable default based on the table size.
var histogramSamples = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_samples.count",
	"number of rows sampled for histogram construction during table statistics collection. "+
		"Not setting this or setting a value of 0 means that a reasonable sample size will be "+
		"automatically picked based on the table size.",
	0,
	settings.NonNegativeIntWithMaximum(math.MaxUint32),
	settings.WithPublic)

// maxTimestampAge is the maximum allowed age of a scan timestamp during table
// stats collection, used when creating statistics AS OF SYSTEM TIME. The
// timestamp is advanced during long operations as needed. See TableReaderSpec.
//
// The lowest TTL we recommend is 10 minutes. This value must be lower than
// that.
var maxTimestampAge = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.stats.max_timestamp_age",
	"maximum age of timestamp during table statistics collection",
	5*time.Minute,
)

// minAutoHistogramSamples and maxAutoHistogramSamples are the bounds used by
// computeNumberSamples to determine the number of samples to collect for
// histogram construction.
var minAutoHistogramSamples = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_samples.min",
	"minimum sample size to be selected when sample size is automatically determined",
	10000,
	settings.NonNegativeIntWithMaximum(math.MaxUint32),
	settings.WithVisibility(settings.Reserved))

var maxAutoHistogramSamples = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.stats.histogram_samples.max",
	"maximum sample size to be selected when sample size is automatically determined",
	300000,
	settings.NonNegativeIntWithMaximum(math.MaxUint32),
	settings.WithVisibility(settings.Reserved))

// computeNumberSamples dynamically determines the number of samples to collect
// based on the estimated number of rows in the table. The formula 582n^0.29 is
// based on empirical data collected by running the sampler with different
// sample sizes on a variety of table sizes and observing the proportion of
// heavy hitters (most frequent elements) represented in the sample. It was
// derived by fitting a best-fit curve to the table below. The number of samples
// returned is bounded between minAutoHistogramSamples and
// maxAutoHistogramSamples (10,000 and 300,000 by default).
// +---------------+-------------+
// | Table Size    | Sample Size |
// +---------------+-------------+
// | 10,000        | 10,000      |
// | 100,000       | 15,000      |
// | 1,000,000     | 30,000      |
// | 10,000,000    | 60,000      |
// | 100,000,000   | 100,000     |
// | 1,000,000,000 | 300,000     |
// +---------------+-------------+
//
// The sample sizes above empirically achieved the following coverage:
//   - 100k rows/15k samples: ~100% coverage of multiplicities down to 100x,
//     ~80% down to 10x
//   - 1m rows/30k samples: ~100% coverage of multiplicities down to 1000x, ~95%
//     down to 100x
//   - 10m rows/60k samples: ~100% coverage of multiplicities down to 10000x,
//     ~95% down to 1000x, ~50% down to 100x
//   - 100m rows/100k samples: ~100% coverage of multiplicities down to 10000x,
//     ~65% down to 1000x, ~10% down to 100x
//   - 1b rows/300k samples: ~100% coverage of multiplicities down to 100000x,
//     ~95% down to 10000x, ~25% down to 1000x
func computeNumberSamples(ctx context.Context, numRows uint64, st *cluster.Settings) uint32 {
	maxSampleSize := maxAutoHistogramSamples.Get(&st.SV)
	minSampleSize := minAutoHistogramSamples.Get(&st.SV)

	if maxSampleSize < minSampleSize {
		log.Infof(
			ctx,
			"using default sample size bounds since max sample size %d is less than min sample size %d",
			maxSampleSize,
			minSampleSize,
		)
		maxSampleSize = maxAutoHistogramSamples.Default()
		minSampleSize = minAutoHistogramSamples.Default()
	}

	numSamples := math.Max(
		math.Min(
			582.0*math.Pow(float64(numRows), 0.29),
			float64(maxSampleSize),
		),
		float64(minSampleSize),
	)
	return uint32(numSamples)
}

func (dsp *DistSQLPlanner) createAndAttachSamplers(
	ctx context.Context,
	p *PhysicalPlan,
	desc catalog.TableDescriptor,
	tableStats []*stats.TableStatistic,
	details jobspb.CreateStatsDetails,
	sampledColumnIDs []descpb.ColumnID,
	jobID jobspb.JobID,
	reqStats []requestedStat,
	sketchSpec, invSketchSpec []execinfrapb.SketchSpec,
	numIndexes int,
	curIndex int,
) *PhysicalPlan {
	// Estimate the expected number of rows based on existing stats in the cache.
	var rowsExpected uint64
	if len(tableStats) > 0 {
		overhead := stats.AutomaticStatisticsFractionStaleRows.Get(&dsp.st.SV)
		if autoStatsFractionStaleRowsForTable, ok := desc.AutoStatsFractionStaleRows(); ok {
			overhead = autoStatsFractionStaleRowsForTable
		}
		// Convert to a signed integer first to make the linter happy.
		if details.UsingExtremes {
			rowsExpected = uint64(int64(
				// The total expected number of rows is the estimated number of stale
				// rows since we're only collecting stats on rows outside the bounds of
				// the most recent statistic.
				float64(tableStats[0].RowCount) * overhead,
			))
		} else {
			rowsExpected = uint64(int64(
				// The total expected number of rows is the same number that was measured
				// most recently, plus some overhead for possible insertions.
				float64(tableStats[0].RowCount) * (1 + overhead),
			))
		}
	}

	// Set up the samplers.
	sampler := &execinfrapb.SamplerSpec{
		Sketches:         sketchSpec,
		InvertedSketches: invSketchSpec,
	}
	sampler.MaxFractionIdle = details.MaxFractionIdle
	// For partial statistics this loop should only iterate once
	// since we only support one reqStat at a time.
	for _, s := range reqStats {
		if s.histogram {
			var histogramSamplesCount uint32
			if tableSampleCount, ok := desc.HistogramSamplesCount(); ok {
				histogramSamplesCount = tableSampleCount
			} else if clusterSampleCount := histogramSamples.Get(&dsp.st.SV); clusterSampleCount != histogramSamples.Default() {
				histogramSamplesCount = uint32(clusterSampleCount)
			} else {
				histogramSamplesCount = computeNumberSamples(
					ctx,
					rowsExpected,
					dsp.st,
				)
				log.Infof(ctx, "using computed sample size of %d for histogram construction", histogramSamplesCount)
			}
			sampler.SampleSize = histogramSamplesCount
			// This could be anything >= 2 to produce a histogram, but the max number
			// of buckets is probably also a reasonable minimum number of samples. (If
			// there are fewer rows than this in the table, there will be fewer
			// samples of course, which is fine.)
			sampler.MinSampleSize = s.histogramMaxBuckets
			break
		}
	}

	// The sampler outputs the original columns plus a rank column, five
	// sketch columns, and two inverted histogram columns.
	outTypes := make([]*types.T, 0, len(p.GetResultTypes())+5)
	outTypes = append(outTypes, p.GetResultTypes()...)
	// An INT column for the rank of each row.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the sketch index.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the number of rows processed.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	outTypes = append(outTypes, types.Int)
	// An INT column indicating the size of the columns in this sketch.
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

	// Set up the final SampleAggregator stage.
	agg := &execinfrapb.SampleAggregatorSpec{
		Sketches:         sketchSpec,
		InvertedSketches: invSketchSpec,
		SampleSize:       sampler.SampleSize,
		MinSampleSize:    sampler.MinSampleSize,
		SampledColumnIDs: sampledColumnIDs,
		TableID:          desc.GetID(),
		JobID:            jobID,
		RowsExpected:     rowsExpected,
		DeleteOtherStats: details.DeleteOtherStats,
		NumIndexes:       uint64(numIndexes),
		CurIndex:         uint64(curIndex),
	}
	// Plan the SampleAggregator on the gateway, unless we have a single Sampler.
	node := dsp.gatewaySQLInstanceID
	if len(p.ResultRouters) == 1 {
		node = p.Processors[p.ResultRouters[0]].SQLInstanceID
	}
	p.AddSingleGroupStage(
		ctx,
		node,
		execinfrapb.ProcessorCoreUnion{SampleAggregator: agg},
		execinfrapb.PostProcessSpec{},
		[]*types.T{},
	)
	p.PlanToStreamColMap = []int{}
	return p
}

func (dsp *DistSQLPlanner) createPartialStatsPlan(
	ctx context.Context,
	planCtx *PlanningCtx,
	desc catalog.TableDescriptor,
	reqStats []requestedStat,
	jobID jobspb.JobID,
	details jobspb.CreateStatsDetails,
	numIndexes int,
	curIndex int,
) (*PhysicalPlan, error) {
	// Partial stats collections on multiple columns create different plans,
	// so we only support one requested stat at a time here.
	if len(reqStats) > 1 {
		return nil, unimplemented.NewWithIssue(
			128904,
			"cannot process multiple partial statistics requests at once",
		)
	}

	reqStat := reqStats[0]

	if len(reqStat.columns) > 1 {
		// TODO (faizaanmadhani): Add support for creating multi-column stats
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "multi-column partial statistics are not currently supported")
	}

	var typeResolver *descs.DistSQLTypeResolver
	if p := planCtx.planner; p != nil {
		r := descs.NewDistSQLTypeResolver(p.Descriptors(), p.Txn())
		typeResolver = &r
	}
	// Fetch all stats for the table that matches the given table descriptor.
	tableStats, err := planCtx.ExtendedEvalCtx.ExecCfg.TableStatsCache.GetTableStats(ctx, desc, typeResolver)
	if err != nil {
		return nil, err
	}

	column, err := catalog.MustFindColumnByID(desc, reqStat.columns[0])
	if err != nil {
		return nil, err
	}

	// Calculate the column we need to scan
	// TODO (faizaanmadhani): Iterate through all columns in a requested stat when
	// when we add support for multi-column statistics.
	var colCfg scanColumnsConfig
	colCfg.wantedColumns = append(colCfg.wantedColumns, column.GetID())

	// Initialize a dummy scanNode for the requested statistic.
	scan := scanNode{desc: desc}
	err = scan.initDescSpecificCol(colCfg, column)
	if err != nil {
		return nil, err
	}
	// Map the ColumnIDs to their ordinals in scan.cols
	// This loop should only iterate once, since we only
	// handle single column partial statistics.
	// TODO(faizaanmadhani): Add support for multi-column partial stats next
	var colIdxMap catalog.TableColMap
	for i, c := range scan.cols {
		colIdxMap.Set(c.GetID(), i)
	}

	var sb span.Builder
	sb.InitAllowingExternalRowData(
		planCtx.EvalContext(), planCtx.ExtendedEvalCtx.Codec, desc, scan.index,
	)

	var stat *stats.TableStatistic
	var histogram []cat.HistogramBucket
	// Find the statistic and histogram from the newest table statistic for our
	// column that is not partial and not forecasted. The first one we find will
	// be the latest due to the newest to oldest ordering property of the cache.
	for _, t := range tableStats {
		if len(t.ColumnIDs) == 1 && column.GetID() == t.ColumnIDs[0] &&
			!t.IsPartial() && !t.IsMerged() && !t.IsForecast() {
			if t.HistogramData == nil || t.HistogramData.ColumnType == nil || len(t.Histogram) == 0 {
				return nil, pgerror.Newf(
					pgcode.ObjectNotInPrerequisiteState,
					"the latest full statistic for column %s has no histogram",
					column.GetName(),
				)
			}
			if colinfo.ColumnTypeIsInvertedIndexable(column.GetType()) &&
				t.HistogramData.ColumnType.Family() == types.BytesFamily {
				return nil, pgerror.Newf(
					pgcode.ObjectNotInPrerequisiteState,
					"the latest full statistic histogram for column %s is an inverted index histogram",
					column.GetName(),
				)
			}
			stat = t
			histogram = t.Histogram
			break
		}
	}
	if stat == nil {
		return nil, pgerror.Newf(
			pgcode.ObjectNotInPrerequisiteState,
			"column %s does not have a prior statistic",
			column.GetName())
	}
	lowerBound, upperBound, err := bounds.GetUsingExtremesBounds(ctx, planCtx.EvalContext(), histogram)
	if err != nil {
		return nil, err
	}
	extremesSpans, err := bounds.ConstructUsingExtremesSpans(lowerBound, upperBound, scan.index)
	if err != nil {
		return nil, err
	}
	extremesPredicate := bounds.ConstructUsingExtremesPredicate(lowerBound, upperBound, column.GetName())
	// Get roachpb.Spans from constraint.Spans
	scan.spans, err = sb.SpansFromConstraintSpan(&extremesSpans, span.NoopSplitter())
	if err != nil {
		return nil, err
	}
	p, err := dsp.createTableReaders(ctx, planCtx, &scan)
	if err != nil {
		return nil, err
	}
	if details.AsOf != nil {
		val := maxTimestampAge.Get(&dsp.st.SV)
		for i := range p.Processors {
			spec := p.Processors[i].Spec.Core.TableReader
			spec.MaxTimestampAgeNanos = uint64(val)
		}
	}

	sampledColumnIDs := make([]descpb.ColumnID, len(scan.cols))
	spec := execinfrapb.SketchSpec{
		SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
		GenerateHistogram:   reqStat.histogram,
		HistogramMaxBuckets: reqStat.histogramMaxBuckets,
		Columns:             make([]uint32, len(reqStat.columns)),
		StatName:            reqStat.name,
		PartialPredicate:    extremesPredicate,
		FullStatisticID:     stat.StatisticID,
		PrevLowerBound:      tree.Serialize(lowerBound),
	}
	// For now, this loop should iterate only once, as we only
	// handle single-column partial statistics.
	// TODO(faizaanmadhani): Add support for multi-column partial stats next
	for i, colID := range reqStat.columns {
		colIdx, ok := colIdxMap.Get(colID)
		if !ok {
			panic("necessary column not scanned")
		}
		streamColIdx := uint32(p.PlanToStreamColMap[colIdx])
		spec.Columns[i] = streamColIdx
		sampledColumnIDs[streamColIdx] = colID
	}
	var sketchSpec, invSketchSpec []execinfrapb.SketchSpec
	if reqStat.inverted {
		// Find the first inverted index on the first column for collecting
		// histograms. Although there may be more than one index, we don't
		// currently have a way of using more than one or deciding which one
		// is better.
		//
		// We do not generate multi-column stats with histograms, so there
		// is no need to find an index for multi-column stats here.
		//
		// TODO(mjibson): allow multiple inverted indexes on the same column
		// (i.e., with different configurations). See #50655.
		if len(reqStat.columns) == 1 {
			for _, index := range desc.PublicNonPrimaryIndexes() {
				if index.GetType() == idxtype.INVERTED && index.InvertedColumnID() == column.GetID() {
					spec.Index = index.IndexDesc()
					break
				}
			}
		}
		// Even if spec.Index is nil because there isn't an inverted index
		// on the requested stats column, we can still proceed. We aren't
		// generating histograms in that case so we don't need an index
		// descriptor to generate the inverted index entries.
		invSketchSpec = append(invSketchSpec, spec)
	} else {
		sketchSpec = append(sketchSpec, spec)
	}
	return dsp.createAndAttachSamplers(
		ctx,
		p,
		desc,
		tableStats,
		details,
		sampledColumnIDs,
		jobID,
		reqStats,
		sketchSpec, invSketchSpec,
		numIndexes, curIndex), nil
}

func (dsp *DistSQLPlanner) createStatsPlan(
	ctx context.Context,
	planCtx *PlanningCtx,
	semaCtx *tree.SemaContext,
	desc catalog.TableDescriptor,
	reqStats []requestedStat,
	jobID jobspb.JobID,
	details jobspb.CreateStatsDetails,
	numIndexes int,
	curIndex int,
) (*PhysicalPlan, error) {
	if len(reqStats) == 0 {
		return nil, errors.New("no stats requested")
	}

	// Calculate the set of columns we need to scan and any virtual computed cols.
	var colCfg scanColumnsConfig
	var tableColSet catalog.TableColSet
	var requestedCols []catalog.Column
	var virtComputedCols []catalog.Column
	for _, s := range reqStats {
		for _, c := range s.columns {
			if !tableColSet.Contains(c) {
				tableColSet.Add(c)
				col, err := catalog.MustFindColumnByID(desc, c)
				if err != nil {
					return nil, err
				}
				requestedCols = append(requestedCols, col)
				if col.IsVirtual() {
					virtComputedCols = append(virtComputedCols, col)
				} else {
					colCfg.wantedColumns = append(colCfg.wantedColumns, c)
				}
			}
		}
	}

	// Add columns to the scan that are referenced by virtual computed column
	// expressions but were not in the requested statistics.
	if len(virtComputedCols) != 0 {
		exprStrings := make([]string, 0, len(virtComputedCols))
		for _, col := range virtComputedCols {
			exprStrings = append(exprStrings, col.GetComputeExpr())
		}

		virtComputedExprs, err := parser.ParseExprs(exprStrings)
		if err != nil {
			return nil, err
		}

		for _, expr := range virtComputedExprs {
			refColIDs, err := schemaexpr.ExtractColumnIDs(desc, expr)
			if err != nil {
				return nil, err
			}
			refColIDs.ForEach(func(c descpb.ColumnID) {
				if !tableColSet.Contains(c) {
					tableColSet.Add(c)
					// Add the referenced column to the scan.
					colCfg.wantedColumns = append(colCfg.wantedColumns, c)
				}
			})
		}
	}

	// Create the table readers; for this we initialize a dummy scanNode.
	scan := scanNode{desc: desc}
	if colCfg.wantedColumns == nil {
		// wantedColumns cannot be left nil, and if it is nil at this point,
		// then we only have virtual computed columns, so we'll allocate an
		// empty slice.
		colCfg.wantedColumns = []tree.ColumnID{}
	}
	err := scan.initDescDefaults(colCfg)
	if err != nil {
		return nil, err
	}
	var sb span.Builder
	sb.InitAllowingExternalRowData(
		planCtx.EvalContext(), planCtx.ExtendedEvalCtx.Codec, desc, scan.index,
	)
	scan.spans, err = sb.UnconstrainedSpans()
	if err != nil {
		return nil, err
	}
	scan.isFull = true

	p, err := dsp.createTableReaders(ctx, planCtx, &scan)
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

	// Add rendering of virtual computed columns.
	if len(virtComputedCols) != 0 {
		// Resolve names and types.
		virtComputedExprs, _, err := schemaexpr.MakeComputedExprs(
			ctx,
			virtComputedCols,
			scan.cols,
			desc,
			tree.NewUnqualifiedTableName(tree.Name(desc.GetName())),
			planCtx.EvalContext(),
			semaCtx,
		)
		if err != nil {
			return nil, err
		}

		// Build render expressions for all requested columns.
		exprs := make(tree.TypedExprs, len(requestedCols))
		resultCols := colinfo.ResultColumnsFromColumns(desc.GetID(), requestedCols)

		ivh := tree.MakeIndexedVarHelper(nil /* container */, len(scan.cols))
		var scanIdx, virtIdx int
		var distSQLVisitor distSQLExprCheckVisitor
		for i, col := range requestedCols {
			if col.IsVirtual() {
				if virtIdx >= len(virtComputedExprs) {
					return nil, errors.AssertionFailedf(
						"virtual computed column expressions do not match requested columns: %v vs %v",
						virtComputedExprs, requestedCols,
					)
				}
				// Check that the virtual computed column expression can be distributed.
				// TODO(michae2): Add the ability to run CREATE STATISTICS locally if a
				// local-only virtual computed column expression is needed.
				if err := checkExprForDistSQL(virtComputedExprs[virtIdx], &distSQLVisitor); err != nil {
					return nil, err
				}
				exprs[i] = virtComputedExprs[virtIdx]
				virtIdx++
			} else {
				// Confirm that the scan columns contain the requested column in the
				// expected order.
				if scanIdx >= len(scan.cols) || scan.cols[scanIdx].GetID() != col.GetID() {
					return nil, errors.AssertionFailedf(
						"scan columns do not match requested columns: %v vs %v", scan.cols, requestedCols,
					)
				}
				exprs[i] = ivh.IndexedVarWithType(scanIdx, scan.cols[scanIdx].GetType())
				scanIdx++
			}
		}

		var rb renderBuilder
		rb.init(exec.Node(planNode(&scan)), exec.OutputOrdering{})
		rb.setOutput(exprs, resultCols)

		err = dsp.createPlanForRender(ctx, p, rb.r, planCtx)
		if err != nil {
			return nil, err
		}
	} else {
		// No virtual computed columns. Confirm that the scan columns match the
		// requested columns.
		for i, col := range requestedCols {
			if i >= len(scan.cols) || scan.cols[i].GetID() != col.GetID() {
				return nil, errors.AssertionFailedf(
					"scan columns do not match requested columns: %v vs %v", scan.cols, requestedCols,
				)
			}
		}
	}

	// Output of the scan or render will be in requestedCols order.
	var colIdxMap catalog.TableColMap
	for i, col := range requestedCols {
		colIdxMap.Set(col.GetID(), i)
	}

	var sketchSpecs, invSketchSpecs []execinfrapb.SketchSpec
	sampledColumnIDs := make([]descpb.ColumnID, len(requestedCols))
	for _, s := range reqStats {
		spec := execinfrapb.SketchSpec{
			SketchType:          execinfrapb.SketchType_HLL_PLUS_PLUS_V1,
			GenerateHistogram:   s.histogram,
			HistogramMaxBuckets: s.histogramMaxBuckets,
			Columns:             make([]uint32, len(s.columns)),
			StatName:            s.name,
		}
		for i, colID := range s.columns {
			colIdx, ok := colIdxMap.Get(colID)
			if !ok {
				panic("necessary column not scanned")
			}
			streamColIdx := uint32(p.PlanToStreamColMap[colIdx])
			spec.Columns[i] = streamColIdx
			sampledColumnIDs[streamColIdx] = colID
		}
		if s.inverted {
			// Find the first inverted index on the first column for collecting
			// histograms. Although there may be more than one index, we don't
			// currently have a way of using more than one or deciding which one
			// is better.
			//
			// We do not generate multi-column stats with histograms, so there
			// is no need to find an index for multi-column stats here.
			//
			// TODO(mjibson): allow multiple inverted indexes on the same column
			// (i.e., with different configurations). See #50655.
			if len(s.columns) == 1 {
				col := s.columns[0]
				for _, index := range desc.PublicNonPrimaryIndexes() {
					if index.GetType() == idxtype.INVERTED && index.InvertedColumnID() == col {
						spec.Index = index.IndexDesc()
						break
					}
				}
			}
			// Even if spec.Index is nil because there isn't an inverted index
			// on the requested stats column, we can still proceed. We aren't
			// generating histograms in that case so we don't need an index
			// descriptor to generate the inverted index entries.
			invSketchSpecs = append(invSketchSpecs, spec)
		} else {
			sketchSpecs = append(sketchSpecs, spec)
		}
	}

	var typeResolver *descs.DistSQLTypeResolver
	if p := planCtx.planner; p != nil {
		r := descs.NewDistSQLTypeResolver(p.Descriptors(), p.Txn())
		typeResolver = &r
	}
	tableStats, err := planCtx.ExtendedEvalCtx.ExecCfg.TableStatsCache.GetTableStats(ctx, desc, typeResolver)
	if err != nil {
		return nil, err
	}

	return dsp.createAndAttachSamplers(
		ctx,
		p,
		desc,
		tableStats,
		details,
		sampledColumnIDs,
		jobID,
		reqStats,
		sketchSpecs, invSketchSpecs,
		numIndexes, curIndex), nil
}

func (dsp *DistSQLPlanner) createPlanForCreateStats(
	ctx context.Context,
	planCtx *PlanningCtx,
	semaCtx *tree.SemaContext,
	jobID jobspb.JobID,
	details jobspb.CreateStatsDetails,
	numIndexes int,
	curIndex int,
) (*PhysicalPlan, error) {
	reqStats := make([]requestedStat, len(details.ColumnStats))
	histogramCollectionEnabled := stats.HistogramClusterMode.Get(&dsp.st.SV)
	tableDesc := tabledesc.NewBuilder(&details.Table).BuildImmutableTable()
	defaultHistogramBuckets := stats.GetDefaultHistogramBuckets(&dsp.st.SV, tableDesc)
	for i := 0; i < len(reqStats); i++ {
		histogram := details.ColumnStats[i].HasHistogram && histogramCollectionEnabled
		var histogramMaxBuckets = defaultHistogramBuckets
		if details.ColumnStats[i].HistogramMaxBuckets > 0 {
			histogramMaxBuckets = details.ColumnStats[i].HistogramMaxBuckets
		}
		if details.ColumnStats[i].Inverted && details.UsingExtremes {
			return nil, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState, "cannot create partial statistics on an inverted index column")
		}
		reqStats[i] = requestedStat{
			columns:             details.ColumnStats[i].ColumnIDs,
			histogram:           histogram,
			histogramMaxBuckets: histogramMaxBuckets,
			name:                details.Name,
			inverted:            details.ColumnStats[i].Inverted,
		}
	}

	if len(reqStats) == 0 {
		return nil, errors.New("no stats requested")
	}

	if details.UsingExtremes {
		return dsp.createPartialStatsPlan(ctx, planCtx, tableDesc, reqStats, jobID, details, numIndexes, curIndex)
	}
	return dsp.createStatsPlan(ctx, planCtx, semaCtx, tableDesc, reqStats, jobID, details, numIndexes, curIndex)
}

func (dsp *DistSQLPlanner) planAndRunCreateStats(
	ctx context.Context,
	evalCtx *extendedEvalContext,
	planCtx *PlanningCtx,
	semaCtx *tree.SemaContext,
	txn *kv.Txn,
	resultWriter *RowResultWriter,
	jobId jobspb.JobID,
	details jobspb.CreateStatsDetails,
	numIndexes int,
	curIndex int,
) error {
	ctx = logtags.AddTag(ctx, "create-stats-distsql", nil)

	physPlan, err := dsp.createPlanForCreateStats(ctx, planCtx, semaCtx, jobId, details, numIndexes, curIndex)
	if err != nil {
		return err
	}

	FinalizePlan(ctx, planCtx, physPlan)

	recv := MakeDistSQLReceiver(
		ctx,
		resultWriter,
		tree.DDL,
		evalCtx.ExecCfg.RangeDescriptorCache,
		txn,
		evalCtx.ExecCfg.Clock,
		evalCtx.Tracing,
	)
	defer recv.Release()

	dsp.Run(ctx, planCtx, txn, physPlan, recv, evalCtx, nil /* finishedSetupFn */)
	return resultWriter.Err()
}
