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
	"sync"

	"github.com/axiomhq/hyperloglog"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// A sample aggregator processor aggregates results from multiple sampler
// processors. See SampleAggregatorSpec for more details.
type sampleAggregator struct {
	processorBase

	input   RowSource
	inTypes []sqlbase.ColumnType
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

func newSampleAggregator(
	flowCtx *FlowCtx,
	spec *SampleAggregatorSpec,
	input RowSource,
	post *PostProcessSpec,
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

	rankCol := len(spec.SampledColumnIDs)
	s := &sampleAggregator{
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

	s.sr.Init(int(spec.SampleSize))

	if err := s.init(post, []sqlbase.ColumnType{}, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the Processor interface.
func (s *sampleAggregator) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := processorSpan(s.flowCtx.Ctx, "sample aggregator")
	defer tracing.FinishSpan(span)

	earlyExit, err := s.mainLoop(ctx)
	if err != nil {
		DrainAndClose(ctx, s.out.output, err, s.input)
	} else if !earlyExit {
		sendTraceData(ctx, s.out.output)
		s.input.ConsumerClosed()
		s.out.Close()
	}
}

func (s *sampleAggregator) mainLoop(ctx context.Context) (earlyExit bool, _ error) {
	var da sqlbase.DatumAlloc
	var tmpSketch hyperloglog.Sketch
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if !emitHelper(ctx, &s.out, nil /* row */, meta, s.input) {
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
				return false, errors.Wrapf(err, "decoding rank column")
			}
			// Retain the rows with the top ranks.
			s.sr.SampleRow(row[:s.rankCol], uint64(rank))
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
			return false, errors.Errorf("NULL sketch data")
		}
		if err := tmpSketch.UnmarshalBinary([]byte(*d.(*tree.DBytes))); err != nil {
			return false, err
		}
		if err := s.sketches[sketchIdx].sketch.Merge(&tmpSketch); err != nil {
			return false, errors.Wrapf(err, "merging sketch data")
		}
	}
	return false, s.writeResults(ctx)
}

// writeResults inserts the new statistics into system.table_statistics.
func (s *sampleAggregator) writeResults(ctx context.Context) error {
	return s.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		for _, si := range s.sketches {
			var histogram []byte
			if si.spec.GenerateHistogram {
				colIdx := int(si.spec.Columns[0])
				typ := s.inTypes[colIdx]

				h, err := generateHistogram(
					&s.flowCtx.EvalCtx,
					s.sr.Get(),
					colIdx,
					typ,
					si.numRows,
					int(si.spec.HistogramMaxBuckets),
				)
				if err != nil {
					return err
				}
				histogram, err = protoutil.Marshal(&h)
				if err != nil {
					return err
				}
			}

			columnIDs := tree.NewDArray(types.Int)
			for _, c := range si.spec.Columns {
				if err := columnIDs.Append(tree.NewDInt(tree.DInt(int(s.sampledCols[c])))); err != nil {
					return err
				}
			}

			var name interface{}
			if si.spec.StatName != "" {
				name = si.spec.StatName
			}

			if _, err := s.flowCtx.executor.ExecuteStatementInTransaction(
				ctx, "insert-statistic", txn,
				`INSERT INTO system.public.table_statistics (
					"tableID",
					"name",
					"columnIDs",
					"rowCount",
					"distinctCount",
					"nullCount",
					histogram
				) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				s.tableID,
				name,
				columnIDs,
				si.numRows,
				si.sketch.Estimate(),
				si.numNulls,
				histogram,
			); err != nil {
				return err
			}

			// TODO(radu): we need to clear out old stats that are superseded.
		}
		return nil
	})
}

// generateHistogram returns a histogram (on a given column) from a set of
// samples.
// numRows is the total number of rows from which values were sampled.
func generateHistogram(
	evalCtx *tree.EvalContext,
	samples []stats.SampledRow,
	colIdx int,
	colType sqlbase.ColumnType,
	numRows int64,
	maxBuckets int,
) (stats.HistogramData, error) {
	var da sqlbase.DatumAlloc
	values := make(tree.Datums, 0, len(samples))
	for _, s := range samples {
		ed := &s.Row[colIdx]
		// Ignore NULLs (they are counted separately).
		if !ed.IsNull() {
			if err := ed.EnsureDecoded(&colType, &da); err != nil {
				return stats.HistogramData{}, err
			}
			values = append(values, ed.Datum)
		}
	}
	return stats.EquiDepthHistogram(evalCtx, values, numRows, maxBuckets)
}
