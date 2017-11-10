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
	"fmt"
	"sync"

	"golang.org/x/net/context"

	"github.com/axiomhq/hyperloglog"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// A sample aggregator processor aggregates results from multiple sampler
// processors. See SampleAggregatorSpec for more details.
type sampleAggregator struct {
	processorBase

	flowCtx *FlowCtx
	input   RowSource
	inTypes []sqlbase.ColumnType
	sr      stats.SampleReservoir

	desc        *sqlbase.TableDescriptor
	sampledCols []sqlbase.ColumnID
	sketchSpecs []SampleAggregatorSpec_SketchSpec
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
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
		if s.GenerateHistogram && s.HistogramMaxBuckets == 0 {
			return nil, errors.Errorf("histogram max buckets not specified")
		}
	}

	s := &sampleAggregator{
		flowCtx:     flowCtx,
		input:       input,
		inTypes:     input.Types(),
		sampledCols: spec.SampledColumnIDs,
		sketchSpecs: spec.Sketches,
	}

	s.sr.Init(int(spec.SampleSize))

	if err := s.out.Init(post, []sqlbase.ColumnType{}, &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the Processor interface.
func (s *sampleAggregator) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := processorSpan(ctx, "sample aggregator")
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
	colIDMap := make(map[sqlbase.ColumnID]int, len(s.sampledCols))
	for i, c := range s.sampledCols {
		colIDMap[c] = i
	}
	rankCol := len(s.sampledCols)
	sketchIdxCol := rankCol + 1
	numRowsCol := sketchIdxCol + 1
	numNullsCol := numRowsCol + 1
	sketchCol := numNullsCol + 1

	sketches := make([]struct {
		s        *hyperloglog.Sketch
		numRows  int64
		numNulls int64
	}, len(s.sketchSpecs))

	for i := range sketches {
		sketches[i].s = hyperloglog.New14()
	}

	var da sqlbase.DatumAlloc
	var tmpSketch hyperloglog.Sketch
	for {
		row, meta := s.input.Next()
		if !meta.Empty() {
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
		if row[sketchIdxCol].IsNull() {
			// This must be a sampled row.
			rank, err := row[rankCol].GetInt()
			if err != nil {
				return false, errors.Wrapf(err, "decoding rank column")
			}
			// Retain the rows with the top ranks.
			s.sr.SampleRow(row[:rankCol], uint64(rank))
			continue
		}
		// This is a sketch row.
		sketchIdx, err := row[sketchIdxCol].GetInt()
		if err != nil {
			return false, err
		}
		if sketchIdx < 0 || sketchIdx > int64(len(sketches)) {
			return false, errors.Errorf("invalid sketch index %d", sketchIdx)
		}

		numRows, err := row[numRowsCol].GetInt()
		if err != nil {
			return false, err
		}
		sketches[sketchIdx].numRows += numRows

		numNulls, err := row[numNullsCol].GetInt()
		if err != nil {
			return false, err
		}
		sketches[sketchIdx].numNulls += numNulls

		// Decode the sketch.
		if err := row[sketchCol].EnsureDecoded(&s.inTypes[sketchCol], &da); err != nil {
			return false, err
		}
		d := row[sketchCol].Datum
		if d == tree.DNull {
			return false, errors.Errorf("NULL sketch data")
		}
		if err := tmpSketch.UnmarshalBinary([]byte(*d.(*tree.DBytes))); err != nil {
			return false, err
		}
		if err := sketches[sketchIdx].s.Merge(&tmpSketch); err != nil {
			return false, errors.Wrapf(err, "merging sketch data")
		}
	}

	for i, sketch := range sketches {
		spec := s.sketchSpecs[i]
		// TODO(radu): for now there is no system table to write to, just dump the
		// results for inspection.
		fmt.Printf(
			"colIDs: %v  numRows: %d  numNulls: %d  cardinality: %d\n",
			spec.ColumnIDs, sketch.numRows, sketch.numNulls,
			sketch.s.Estimate(),
		)
		colIdx, ok := colIDMap[spec.ColumnIDs[0]]
		if !ok {
			return false, errors.Errorf("columnID %d not sampled", spec.ColumnIDs[0])
		}
		typ := s.inTypes[colIdx]

		if s.sketchSpecs[i].GenerateHistogram {
			h, err := generateHistogram(
				&s.flowCtx.EvalCtx,
				s.sr.Get(),
				colIdx,
				typ,
				sketch.numRows,
				int(spec.HistogramMaxBuckets),
			)
			if err != nil {
				return false, err
			}
			fmt.Printf("histogram:\n")
			for _, b := range h.Buckets {
				ed, _, err := sqlbase.EncDatumFromBuffer(&typ, sqlbase.DatumEncoding_ASCENDING_KEY, b.UpperBound)
				if err != nil {
					return false, err
				}
				fmt.Printf("  %s: less=%d eq=%d\n", ed.String(&typ), b.NumRange, b.NumEq)
			}
		}
	}
	return false, nil
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
