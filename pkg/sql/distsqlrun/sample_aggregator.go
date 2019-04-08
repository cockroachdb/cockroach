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

package distsqlrun

import (
	"context"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// A sample aggregator processor aggregates results from multiple sampler
// processors. See SampleAggregatorSpec for more details.
type sampleAggregator struct {
	ProcessorBase

	spec    *distsqlpb.SampleAggregatorSpec
	input   RowSource
	inTypes []types.T
	sr      stats.SampleReservoir

	tableID     sqlbase.ID
	sampledCols []sqlbase.ColumnID
	sketches    []sketchInfo

	// Input column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	numNullsCol  int
	sketchCol    int
}

var _ Processor = &sampleAggregator{}

const sampleAggregatorProcName = "sample aggregator"

// SampleAggregatorProgressInterval is the frequency at which the
// SampleAggregator processor will report progress. It is mutable for testing.
var SampleAggregatorProgressInterval = 5 * time.Second

func newSampleAggregator(
	flowCtx *FlowCtx,
	processorID int32,
	spec *distsqlpb.SampleAggregatorSpec,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
) (*sampleAggregator, error) {
	for _, s := range spec.Sketches {
		if len(s.Columns) == 0 {
			return nil, errors.Errorf("no columns")
		}
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
		if s.GenerateHistogram && s.HistogramMaxBuckets == 0 {
			return nil, errors.Errorf("histogram max buckets not specified")
		}
		if s.GenerateHistogram && len(s.Columns) != 1 {
			return nil, errors.Errorf("histograms require one column")
		}
	}

	rankCol := len(input.OutputTypes()) - 5
	s := &sampleAggregator{
		spec:         spec,
		input:        input,
		inTypes:      input.OutputTypes(),
		tableID:      spec.TableID,
		sampledCols:  spec.SampledColumnIDs,
		sketches:     make([]sketchInfo, len(spec.Sketches)),
		rankCol:      rankCol,
		sketchIdxCol: rankCol + 1,
		numRowsCol:   rankCol + 2,
		numNullsCol:  rankCol + 3,
		sketchCol:    rankCol + 4,
	}

	for i := range spec.Sketches {
		s.sketches[i] = sketchInfo{
			spec:     spec.Sketches[i],
			sketch:   hyperloglog.New14(),
			numNulls: 0,
			numRows:  0,
		}
	}

	s.sr.Init(int(spec.SampleSize), input.OutputTypes()[:rankCol])

	if err := s.Init(
		nil, post, []types.T{}, flowCtx, processorID, output, nil, /* memMonitor */
		// this proc doesn't implement RowSource and doesn't use ProcessorBase to drain
		ProcStateOpts{},
	); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *sampleAggregator) pushTrailingMeta(ctx context.Context) {
	sendTraceData(ctx, s.out.output)
}

// Run is part of the Processor interface.
func (s *sampleAggregator) Run(ctx context.Context) {
	s.input.Start(ctx)
	s.StartInternal(ctx, sampleAggregatorProcName)
	defer tracing.FinishSpan(s.span)

	earlyExit, err := s.mainLoop(s.Ctx)
	if err != nil {
		DrainAndClose(s.Ctx, s.out.output, err, s.pushTrailingMeta, s.input)
	} else if !earlyExit {
		s.pushTrailingMeta(s.Ctx)
		s.input.ConsumerClosed()
		s.out.Close()
	}
}

func (s *sampleAggregator) mainLoop(ctx context.Context) (earlyExit bool, err error) {
	var job *jobs.Job
	jobID := s.spec.JobID
	// Some tests run this code without a job, so check if the jobID is 0.
	if jobID != 0 {
		job, err = s.flowCtx.JobRegistry.LoadJob(ctx, s.spec.JobID)
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
			return job.CheckStatus(ctx)
		}
		lastReportedFractionCompleted = fractionCompleted
		return job.FractionProgressed(ctx, jobs.FractionUpdater(fractionCompleted))
	}

	var rowsProcessed uint64
	progressUpdates := util.Every(SampleAggregatorProgressInterval)
	var da sqlbase.DatumAlloc
	var tmpSketch hyperloglog.Sketch
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
						fractionCompleted = float32(float64(rowsProcessed) / float64(s.spec.RowsExpected))
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
			} else if !emitHelper(ctx, &s.out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			break
		}

		// The row is either:
		//  - a sampled row, which has NULLs on all columns from sketchIdxCol
		//    onward, or
		//  - a sketch row, which has all NULLs on all columns before sketchIdxCol.
		if row[s.sketchIdxCol].IsNull() {
			// This must be a sampled row.
			rank, err := row[s.rankCol].GetInt()
			if err != nil {
				return false, pgerror.NewAssertionErrorWithWrappedErrf(err, "decoding rank column")
			}
			// Retain the rows with the top ranks.
			if err := s.sr.SampleRow(row[:s.rankCol], uint64(rank)); err != nil {
				return false, err
			}
			continue
		}
		// This is a sketch row.
		sketchIdx, err := row[s.sketchIdxCol].GetInt()
		if err != nil {
			return false, err
		}
		if sketchIdx < 0 || sketchIdx > int64(len(s.sketches)) {
			return false, errors.Errorf("invalid sketch index %d", sketchIdx)
		}

		numRows, err := row[s.numRowsCol].GetInt()
		if err != nil {
			return false, err
		}
		s.sketches[sketchIdx].numRows += numRows

		numNulls, err := row[s.numNullsCol].GetInt()
		if err != nil {
			return false, err
		}
		s.sketches[sketchIdx].numNulls += numNulls

		// Decode the sketch.
		if err := row[s.sketchCol].EnsureDecoded(&s.inTypes[s.sketchCol], &da); err != nil {
			return false, err
		}
		d := row[s.sketchCol].Datum
		if d == tree.DNull {
			return false, pgerror.NewAssertionErrorf("NULL sketch data")
		}
		if err := tmpSketch.UnmarshalBinary([]byte(*d.(*tree.DBytes))); err != nil {
			return false, err
		}
		if err := s.sketches[sketchIdx].sketch.Merge(&tmpSketch); err != nil {
			return false, pgerror.NewAssertionErrorWithWrappedErrf(err, "merging sketch data")
		}
	}
	// Report progress one last time so we don't write results if the job was
	// canceled.
	if err = progFn(1.0); err != nil {
		return false, err
	}
	return false, s.writeResults(ctx)
}

// writeResults inserts the new statistics into system.table_statistics.
func (s *sampleAggregator) writeResults(ctx context.Context) error {
	// TODO(andrei): This method would benefit from a session interface on the
	// internal executor instead of doing this weird thing where it uses the
	// internal executor to execute one statement at a time inside a db.Txn()
	// closure.
	if err := s.flowCtx.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		for _, si := range s.sketches {
			var histogram *stats.HistogramData
			if si.spec.GenerateHistogram && len(s.sr.Get()) != 0 {
				colIdx := int(si.spec.Columns[0])
				typ := &s.inTypes[colIdx]

				h, err := generateHistogram(
					s.evalCtx,
					s.sr.Get(),
					colIdx,
					typ,
					si.numRows,
					int(si.spec.HistogramMaxBuckets),
				)
				if err != nil {
					return err
				}
				histogram = &h
			}

			columnIDs := make([]sqlbase.ColumnID, len(si.spec.Columns))
			for i, c := range si.spec.Columns {
				columnIDs[i] = s.sampledCols[c]
			}

			// Delete old stats that have been superseded.
			if err := stats.DeleteOldStatsForColumns(
				ctx,
				s.flowCtx.executor,
				txn,
				s.tableID,
				columnIDs,
			); err != nil {
				return err
			}

			// Insert the new stat.
			if err := stats.InsertNewStat(
				ctx,
				s.flowCtx.executor,
				txn,
				s.tableID,
				si.spec.StatName,
				columnIDs,
				si.numRows,
				int64(si.sketch.Estimate()),
				si.numNulls,
				histogram,
			); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}

	// Gossip invalidation of the stat caches for this table.
	return stats.GossipTableStatAdded(s.flowCtx.Gossip, s.tableID)
}

// generateHistogram returns a histogram (on a given column) from a set of
// samples.
// numRows is the total number of rows from which values were sampled.
func generateHistogram(
	evalCtx *tree.EvalContext,
	samples []stats.SampledRow,
	colIdx int,
	colType *types.T,
	numRows int64,
	maxBuckets int,
) (stats.HistogramData, error) {
	var da sqlbase.DatumAlloc
	values := make(tree.Datums, 0, len(samples))
	for _, s := range samples {
		ed := &s.Row[colIdx]
		// Ignore NULLs (they are counted separately).
		if !ed.IsNull() {
			if err := ed.EnsureDecoded(colType, &da); err != nil {
				return stats.HistogramData{}, err
			}
			values = append(values, ed.Datum)
		}
	}
	return stats.EquiDepthHistogram(evalCtx, values, numRows, maxBuckets)
}
