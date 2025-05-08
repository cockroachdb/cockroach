// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowexec

import (
	"context"
	"math"
	"time"

	hllNew "github.com/axiomhq/hyperloglog"
	hllOld "github.com/axiomhq/hyperloglog/000"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execversion"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

// A sample aggregator processor aggregates results from multiple sampler
// processors. See SampleAggregatorSpec for more details.
type sampleAggregator struct {
	execinfra.ProcessorBase

	spec    *execinfrapb.SampleAggregatorSpec
	input   execinfra.RowSource
	inTypes []*types.T
	sr      stats.SampleReservoir

	// memAcc accounts for memory accumulated throughout the life of the
	// sampleAggregator.
	memAcc mon.BoundAccount

	// tempMemAcc is used to account for memory that is allocated temporarily
	// and released before the sampleAggregator is finished.
	tempMemAcc mon.BoundAccount

	tableID     descpb.ID
	sampledCols []descpb.ColumnID
	sketches    []sketchInfo

	// Input column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	numNullsCol  int
	sumSizeCol   int
	sketchCol    int
	invColIdxCol int
	invIdxKeyCol int

	// The sample aggregator tracks sketches and reservoirs for inverted
	// index keys, mapped by column index.
	invSr     map[uint32]*stats.SampleReservoir
	invSketch map[uint32]*sketchInfo
}

var _ execinfra.Processor = &sampleAggregator{}

const sampleAggregatorProcName = "sample aggregator"

// SampleAggregatorProgressInterval is the frequency at which the
// SampleAggregator processor will report progress. It is mutable for testing.
var SampleAggregatorProgressInterval = 5 * time.Second

func newSampleAggregator(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec *execinfrapb.SampleAggregatorSpec,
	input execinfra.RowSource,
	post *execinfrapb.PostProcessSpec,
) (*sampleAggregator, error) {
	for _, s := range spec.Sketches {
		if len(s.Columns) == 0 {
			return nil, errors.Errorf("no columns")
		}
		if s.GenerateHistogram && s.HistogramMaxBuckets == 0 {
			return nil, errors.Errorf("histogram max buckets not specified")
		}
		if s.GenerateHistogram && len(s.Columns) != 1 {
			return nil, errors.Errorf("histograms require one column")
		}
	}
	useNewHLL := execversion.FromContext(ctx) >= execversion.V25_1

	// Limit the memory use by creating a child monitor with a hard limit.
	// The processor will disable histogram collection if this limit is not
	// enough.
	//
	// sampleAggregator doesn't spill to disk, so ensure some reasonable lower
	// bound on the workmem limit.
	var minMemoryLimit int64 = 8 << 20 // 8MiB
	if flowCtx.Cfg.TestingKnobs.MemoryLimitBytes != 0 {
		minMemoryLimit = flowCtx.Cfg.TestingKnobs.MemoryLimitBytes
	}
	memMonitor := execinfra.NewLimitedMonitorWithLowerBound(
		ctx, flowCtx, "sample-aggregator-mem", minMemoryLimit,
	)
	rankCol := len(input.OutputTypes()) - 8
	s := &sampleAggregator{
		spec:         spec,
		input:        input,
		inTypes:      input.OutputTypes(),
		memAcc:       memMonitor.MakeBoundAccount(),
		tempMemAcc:   memMonitor.MakeBoundAccount(),
		tableID:      spec.TableID,
		sampledCols:  spec.SampledColumnIDs,
		sketches:     make([]sketchInfo, len(spec.Sketches)),
		rankCol:      rankCol,
		sketchIdxCol: rankCol + 1,
		numRowsCol:   rankCol + 2,
		numNullsCol:  rankCol + 3,
		sumSizeCol:   rankCol + 4,
		sketchCol:    rankCol + 5,
		invColIdxCol: rankCol + 6,
		invIdxKeyCol: rankCol + 7,
		invSr:        make(map[uint32]*stats.SampleReservoir, len(spec.InvertedSketches)),
		invSketch:    make(map[uint32]*sketchInfo, len(spec.InvertedSketches)),
	}

	var sampleCols intsets.Fast
	for i := range spec.Sketches {
		s.sketches[i] = sketchInfo{
			spec:     spec.Sketches[i],
			numNulls: 0,
			numRows:  0,
		}
		if useNewHLL {
			s.sketches[i].sketchNew = hllNew.New14()
		} else {
			s.sketches[i].sketchOld = hllOld.New14()
		}
		if spec.Sketches[i].GenerateHistogram {
			sampleCols.Add(int(spec.Sketches[i].Columns[0]))
		}
	}

	s.sr.Init(
		int(spec.SampleSize), int(spec.MinSampleSize), input.OutputTypes()[:rankCol], &s.memAcc,
		sampleCols,
	)
	for i := range spec.InvertedSketches {
		var sr stats.SampleReservoir
		// The datums are converted to their inverted index bytes and sent as a
		// single DBytes column. We do not use DEncodedKey here because it would
		// introduce backward compatibility complications.
		var srCols intsets.Fast
		srCols.Add(0)
		sr.Init(int(spec.SampleSize), int(spec.MinSampleSize), bytesRowType, &s.memAcc, srCols)
		col := spec.InvertedSketches[i].Columns[0]
		s.invSr[col] = &sr
		s.invSketch[col] = &sketchInfo{
			spec:     spec.InvertedSketches[i],
			numNulls: 0,
			numRows:  0,
		}
		if useNewHLL {
			s.invSketch[col].sketchNew = hllNew.New14()
		} else {
			s.invSketch[col].sketchOld = hllOld.New14()
		}
	}

	if err := s.Init(
		ctx, nil, post, input.OutputTypes(), flowCtx, processorID, memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				s.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the Processor interface.
func (s *sampleAggregator) Run(ctx context.Context, output execinfra.RowReceiver) {
	ctx = s.StartInternal(ctx, sampleAggregatorProcName)
	s.input.Start(ctx)

	earlyExit, err := s.mainLoop(ctx, output)
	if err != nil {
		execinfra.DrainAndClose(ctx, s.FlowCtx, s.input, output, err)
	} else if !earlyExit {
		execinfra.SendTraceData(ctx, s.FlowCtx, output)
		s.input.ConsumerClosed()
		output.ProducerDone()
	}
	s.MoveToDraining(nil /* err */)
}

// Close is part of the execinfra.Processor interface.
func (s *sampleAggregator) Close(context.Context) {
	s.input.ConsumerClosed()
	s.close()
}

func (s *sampleAggregator) close() {
	if s.InternalClose() {
		s.memAcc.Close(s.Ctx())
		s.tempMemAcc.Close(s.Ctx())
		s.MemMonitor.Stop(s.Ctx())
	}
}

func (s *sampleAggregator) mainLoop(
	ctx context.Context, output execinfra.RowReceiver,
) (earlyExit bool, err error) {
	var job *jobs.Job
	jobID := s.spec.JobID
	// Some tests run this code without a job, so check if the jobID is 0.
	if jobID != 0 {
		job, err = s.FlowCtx.Cfg.JobRegistry.LoadJob(ctx, s.spec.JobID)
		if err != nil {
			return false, err
		}
	}

	lastReportedFractionCompleted := float32(-1)
	// Report progress (0 to 1).
	progFn := func(fractionCompleted float32) error {
		if jobID == 0 {
			return nil
		}
		// If it changed by less than 1%, just check for cancellation (which is more
		// efficient).
		if fractionCompleted < 1.0 && fractionCompleted < lastReportedFractionCompleted+0.01 {
			return job.NoTxn().CheckState(ctx)
		}
		lastReportedFractionCompleted = fractionCompleted
		return job.NoTxn().FractionProgressed(ctx, jobs.FractionUpdater(fractionCompleted))
	}

	var rowsProcessed uint64
	progressUpdates := util.Every(SampleAggregatorProgressInterval)
	var da tree.DatumAlloc
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if meta.SamplerProgress != nil {
				rowsProcessed += meta.SamplerProgress.RowsProcessed
				if progressUpdates.ShouldProcess(timeutil.Now()) {
					// Periodically report fraction progressed and check that the job has
					// not been paused or canceled.
					var fractionCompleted float32
					if s.spec.RowsExpected > 0 {
						// Compute the fraction of rows processed so far for the current
						// index.
						fractionCompleted = min(float32(float64(rowsProcessed)/float64(s.spec.RowsExpected)), 1.0)

						if s.spec.NumIndexes > 0 {
							// Adjust the fraction to account for the indexes that have already
							// been processed.
							fractionCompleted = (float32(s.spec.CurIndex) + fractionCompleted) / float32(s.spec.NumIndexes)
						}

						const maxProgress = 0.99
						if fractionCompleted > maxProgress {
							// Since the total number of rows expected is just an estimate,
							// don't report more than 99% completion until the very end.
							fractionCompleted = maxProgress
						}
					}

					if err := progFn(fractionCompleted); err != nil {
						return false, err
					}
				}
				if meta.SamplerProgress.HistogramDisabled {
					// One of the sampler processors probably ran out of memory while
					// collecting histogram samples. Disable sample collection so we
					// don't create a biased histogram.
					s.sr.Disable()
					for _, sr := range s.invSr {
						sr.Disable()
					}
				}
			} else if !emitHelper(ctx, s.FlowCtx, s.input, output, &s.OutputHelper, nil /* row */, meta) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			break
		}

		// There are four kinds of rows. They should be identified in this order:
		//  - an inverted sample has invColIdxCol and rankCol
		//  - an inverted sketch has invColIdxCol
		//  - a normal sketch has sketchIdxCol
		//  - a normal sample has rankCol
		if invColIdx, err := row[s.invColIdxCol].GetInt(); err == nil {
			colIdx := uint32(invColIdx)
			if rank, err := row[s.rankCol].GetInt(); err == nil {
				// Inverted sample row.
				// Shrink capacity to match the child samplerProcessor and then retain
				// the row if it had one of the top (smallest) ranks.
				s.maybeDecreaseSamples(ctx, s.invSr[colIdx], row)
				sampleRow := row[s.invIdxKeyCol : s.invIdxKeyCol+1]
				if err := s.sampleRow(ctx, s.invSr[colIdx], sampleRow, uint64(rank)); err != nil {
					return false, err
				}
				continue
			}
			// Inverted sketch row.
			invSketch, ok := s.invSketch[colIdx]
			if !ok {
				return false, errors.AssertionFailedf("unknown inverted sketch")
			}
			if err := s.processSketchRow(invSketch, row, &da); err != nil {
				return false, err
			}
			continue
		}
		if rank, err := row[s.rankCol].GetInt(); err == nil {
			// Sample row.
			// Shrink capacity to match the child samplerProcessor and then retain the
			// row if it had one of the top (smallest) ranks.
			s.maybeDecreaseSamples(ctx, &s.sr, row)
			if err := s.sampleRow(ctx, &s.sr, row[:s.rankCol], uint64(rank)); err != nil {
				return false, err
			}
			continue
		}
		// Sketch row.
		sketchIdx, err := row[s.sketchIdxCol].GetInt()
		if err != nil {
			return false, err
		}
		if sketchIdx < 0 || sketchIdx > int64(len(s.sketches)) {
			return false, errors.Errorf("invalid sketch index %d", sketchIdx)
		}
		if err := s.processSketchRow(&s.sketches[sketchIdx], row, &da); err != nil {
			return false, err
		}
	}
	// Report progress one last time if this is the last index being scanned, so
	// we don't write results if the job was canceled.
	if s.spec.CurIndex+1 == s.spec.NumIndexes || s.spec.NumIndexes == 0 {
		if err = progFn(1.0); err != nil {
			return false, err
		}
	}
	return false, s.writeResults(ctx)
}

func (s *sampleAggregator) processSketchRow(
	sketch *sketchInfo, row rowenc.EncDatumRow, da *tree.DatumAlloc,
) error {
	numRows, err := row[s.numRowsCol].GetInt()
	if err != nil {
		return err
	}
	sketch.numRows += numRows

	numNulls, err := row[s.numNullsCol].GetInt()
	if err != nil {
		return err
	}
	sketch.numNulls += numNulls

	size, err := row[s.sumSizeCol].GetInt()
	if err != nil {
		return err
	}
	sketch.size += size

	// Decode the sketch.
	if err := row[s.sketchCol].EnsureDecoded(s.inTypes[s.sketchCol], da); err != nil {
		return err
	}
	d := row[s.sketchCol].Datum
	if d == tree.DNull {
		return errors.AssertionFailedf("NULL sketch data")
	}
	if sketch.sketchNew != nil {
		var tmpSketch hllNew.Sketch
		if err := tmpSketch.UnmarshalBinary([]byte(*d.(*tree.DBytes))); err != nil {
			return err
		}
		if err := sketch.sketchNew.Merge(&tmpSketch); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "merging sketch data")
		}
	} else {
		var tmpSketch hllOld.Sketch
		if err := tmpSketch.UnmarshalBinary([]byte(*d.(*tree.DBytes))); err != nil {
			return err
		}
		if err := sketch.sketchOld.Merge(&tmpSketch); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "merging sketch data")
		}
	}
	return nil
}

// maybeDecreaseSamples shrinks the capacity of the aggregate reservoir to be <=
// the capacity of the child reservoir. This is done to prevent biasing the
// sampling in favor of child sampleProcessors with larger reservoirs.
func (s *sampleAggregator) maybeDecreaseSamples(
	ctx context.Context, sr *stats.SampleReservoir, row rowenc.EncDatumRow,
) {
	if capacity, err := row[s.numRowsCol].GetInt(); err == nil {
		prevCapacity := sr.Cap()
		if sr.MaybeResize(ctx, int(capacity)) {
			log.Infof(
				ctx, "histogram samples reduced from %d to %d to match sampler processor",
				prevCapacity, sr.Cap(),
			)
		}
	}
}

func (s *sampleAggregator) sampleRow(
	ctx context.Context, sr *stats.SampleReservoir, sampleRow rowenc.EncDatumRow, rank uint64,
) error {
	prevCapacity := sr.Cap()
	if err := sr.SampleRow(ctx, s.FlowCtx.EvalCtx, sampleRow, rank); err != nil {
		if code := pgerror.GetPGCode(err); code != pgcode.OutOfMemory {
			return err
		}
		// We hit an out of memory error. Clear the sample reservoir and
		// disable histogram sample collection.
		sr.Disable()
		log.Info(ctx, "disabling histogram collection due to excessive memory utilization")
		telemetry.Inc(sqltelemetry.StatsHistogramOOMCounter)
	} else if sr.Cap() != prevCapacity {
		log.Infof(
			ctx, "histogram samples reduced from %d to %d due to excessive memory utilization",
			prevCapacity, sr.Cap(),
		)
	}
	return nil
}

// writeResults inserts the new statistics into system.table_statistics.
func (s *sampleAggregator) writeResults(ctx context.Context) error {
	// Turn off tracing so these writes don't affect the results of EXPLAIN
	// ANALYZE.
	if span := tracing.SpanFromContext(ctx); span != nil && span.RecordingType() != tracingpb.RecordingOff {
		// TODO(rytaft): this also hides writes in this function from SQL session
		// traces.
		ctx = tracing.ContextWithSpan(ctx, nil)
	}

	// TODO(andrei): This method would benefit from a session interface on the
	// internal executor instead of doing this weird thing where it uses the
	// internal executor to execute one statement at a time inside a db.Txn()
	// closure.
	if err := s.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		for _, si := range s.sketches {
			var histogram *stats.HistogramData
			if si.spec.GenerateHistogram {
				colIdx := int(si.spec.Columns[0])
				typ := s.inTypes[colIdx]

				var lowerBound tree.Datum
				if si.spec.PrevLowerBound != "" {
					lbExpr, err := parser.ParseExpr(si.spec.PrevLowerBound)
					if err != nil {
						return err
					}
					lbTypedExpr, err := lbExpr.TypeCheck(ctx, &s.SemaCtx, typ)
					if err != nil {
						return err
					}
					// Lower bounds are serialized datums, so evaluating the
					// expression shouldn't modify the eval context.
					lowerBound, err = eval.Expr(ctx, s.FlowCtx.EvalCtx, lbTypedExpr)
					if err != nil {
						return err
					}
				}

				h, err := s.generateHistogram(
					ctx,
					s.FlowCtx.EvalCtx,
					&s.sr,
					colIdx,
					typ,
					si.numRows-si.numNulls,
					s.getDistinctCount(&si, false /* includeNulls */),
					int(si.spec.HistogramMaxBuckets),
					lowerBound,
				)
				if err != nil {
					return err
				}
				histogram = &h
			} else if invSr, ok := s.invSr[si.spec.Columns[0]]; ok && len(invSr.Get()) != 0 {
				invSketch, ok := s.invSketch[si.spec.Columns[0]]
				if !ok {
					return errors.Errorf("no associated inverted sketch")
				}
				// GenerateHistogram is false for sketches
				// with inverted index columns. Instead, the
				// presence of those histograms is indicated
				// by the existence of an inverted sketch on
				// the column.

				invDistinctCount := s.getDistinctCount(invSketch, false /* includeNulls */)
				// Use 0 for the colIdx here because it refers
				// to the column index of the samples, which
				// only has a single bytes column with the
				// inverted keys.
				h, err := s.generateHistogram(
					ctx,
					s.FlowCtx.EvalCtx,
					invSr,
					0, /* colIdx */
					types.Bytes,
					invSketch.numRows-invSketch.numNulls,
					invDistinctCount,
					int(invSketch.spec.HistogramMaxBuckets),
					nil,
				)
				if err != nil {
					return err
				}
				histogram = &h
			}

			columnIDs := make([]descpb.ColumnID, len(si.spec.Columns))
			for i, c := range si.spec.Columns {
				columnIDs[i] = s.sampledCols[c]
			}

			// Delete old stats that have been superseded,
			// if the new statistic is not partial
			if si.spec.PartialPredicate == "" {
				if err := stats.DeleteOldStatsForColumns(
					ctx,
					txn,
					s.tableID,
					columnIDs,
				); err != nil {
					return err
				}
			}

			// Insert the new stat.
			if err := stats.InsertNewStat(
				ctx,
				s.FlowCtx.Cfg.Settings,
				txn,
				s.tableID,
				si.spec.StatName,
				columnIDs,
				si.numRows,
				s.getDistinctCount(&si, true /* includeNulls */),
				si.numNulls,
				s.getAvgSize(&si),
				histogram,
				si.spec.PartialPredicate,
				si.spec.FullStatisticID,
			); err != nil {
				return err
			}

			// Release any memory temporarily used for this statistic.
			s.tempMemAcc.Clear(ctx)
		}

		return nil
	}); err != nil {
		return err
	}

	if s.spec.DeleteOtherStats {
		columnsUsed := make([][]descpb.ColumnID, len(s.sketches))
		for i, si := range s.sketches {
			columnIDs := make([]descpb.ColumnID, len(si.spec.Columns))
			for j, c := range si.spec.Columns {
				columnIDs[j] = s.sampledCols[c]
			}
			columnsUsed[i] = columnIDs
		}
		keepTime := stats.TableStatisticsRetentionPeriod.Get(&s.FlowCtx.Cfg.Settings.SV)
		if err := s.FlowCtx.Cfg.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// Delete old stats from columns that were not collected. This is
			// important to prevent single-column stats from deleted columns or
			// multi-column stats from deleted indexes from persisting indefinitely.
			return stats.DeleteOldStatsForOtherColumns(
				ctx,
				txn,
				s.tableID,
				columnsUsed,
				keepTime,
			)
		}); err != nil {
			return err
		}
	}

	return nil
}

// getAvgSize returns the average number of bytes per row in the given
// sketch.
func (s *sampleAggregator) getAvgSize(si *sketchInfo) int64 {
	if si.numRows == 0 {
		return 0
	}
	return int64(math.Ceil(float64(si.size) / float64(si.numRows)))
}

// getDistinctCount returns the number of distinct values in the given sketch,
// optionally including null values.
func (s *sampleAggregator) getDistinctCount(si *sketchInfo, includeNulls bool) int64 {
	var distinctCount int64
	if si.sketchNew != nil {
		distinctCount = int64(si.sketchNew.Estimate())
	} else {
		distinctCount = int64(si.sketchOld.Estimate())
	}
	if si.numNulls > 0 && !includeNulls {
		// Nulls are included in the estimate, so reduce the count by 1 if nulls are
		// not requested.
		distinctCount--
	}

	// The maximum number of distinct values is the number of non-null rows plus 1
	// if there are any nulls. It's possible that distinctCount was calculated to
	// be greater than this number due to the approximate nature of HyperLogLog.
	// If this is the case, set it equal to the max.
	maxDistinctCount := si.numRows - si.numNulls
	if si.numNulls > 0 && includeNulls {
		maxDistinctCount++
	}
	if distinctCount > maxDistinctCount {
		distinctCount = maxDistinctCount
	}
	return distinctCount
}

// generateHistogram returns a histogram (on a given column) from a set of
// samples.
// numRows is the total number of rows from which values were sampled
// (excluding rows that have NULL values on the histogram column).
func (s *sampleAggregator) generateHistogram(
	ctx context.Context,
	evalCtx *eval.Context,
	sr *stats.SampleReservoir,
	colIdx int,
	colType *types.T,
	numRows int64,
	distinctCount int64,
	maxBuckets int,
	lowerBound tree.Datum,
) (stats.HistogramData, error) {
	prevCapacity := sr.Cap()
	values, err := sr.GetNonNullDatums(ctx, &s.tempMemAcc, colIdx)
	if err != nil {
		return stats.HistogramData{}, err
	}

	if sr.Cap() != prevCapacity {
		log.Infof(
			ctx, "histogram samples reduced from %d to %d due to excessive memory utilization",
			prevCapacity, sr.Cap(),
		)
	}

	if lowerBound != nil {
		h, buckets, err := stats.ConstructExtremesHistogram(ctx, evalCtx, colType, values, numRows, distinctCount, maxBuckets, lowerBound, evalCtx.Settings)
		_ = buckets
		return h, err
	}

	// TODO(michae2): Instead of using the flowCtx's evalCtx, investigate
	// whether this can use a nil *eval.Context.
	h, _, err := stats.EquiDepthHistogram(ctx, evalCtx, colType, values, numRows, distinctCount, maxBuckets, evalCtx.Settings)
	return h, err
}

var _ execinfra.DoesNotUseTxn = &sampleAggregator{}

// DoesNotUseTxn implements the DoesNotUseTxn interface.
func (s *sampleAggregator) DoesNotUseTxn() bool {
	txnUser, ok := s.input.(execinfra.DoesNotUseTxn)
	return ok && txnUser.DoesNotUseTxn()
}
