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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// sketchInfo contains the specification and run-time state for each sketch.
type sketchInfo struct {
	spec     SketchSpec
	sketch   *hyperloglog.Sketch
	numNulls int64
	numRows  int64
}

// A sampler processor returns a random sample of rows, as well as "global"
// statistics (including cardinality estimation sketch data). See SamplerSpec
// for more details.
type samplerProcessor struct {
	processorBase

	flowCtx  *FlowCtx
	input    RowSource
	sr       stats.SampleReservoir
	sketches []sketchInfo
	outTypes []sqlbase.ColumnType
	// Output column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	numNullsCol  int
	sketchCol    int
}

var _ Processor = &samplerProcessor{}

var supportedSketchTypes = map[SketchType]struct{}{
	// The code currently hardcodes the use of this single type of sketch
	// (which avoids the extra complexity until we actually have multiple types).
	SketchType_HLL_PLUS_PLUS_V1: {},
}

func newSamplerProcessor(
	flowCtx *FlowCtx, spec *SamplerSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*samplerProcessor, error) {
	for _, s := range spec.Sketches {
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
		if len(s.Columns) != 1 {
			return nil, errors.Errorf("multi-column sketches not supported yet")
		}
	}

	s := &samplerProcessor{
		flowCtx:  flowCtx,
		input:    input,
		sketches: make([]sketchInfo, len(spec.Sketches)),
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

	inTypes := input.OutputTypes()
	outTypes := make([]sqlbase.ColumnType, 0, len(inTypes)+5)

	// First columns are the same as the input.
	outTypes = append(outTypes, inTypes...)

	// An INT column for the rank of each row.
	s.rankCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})

	// An INT column indicating the sketch index.
	s.sketchIdxCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})

	// An INT column indicating the number of rows processed.
	s.numRowsCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})

	// An INT column indicating the number of rows that have a NULL in any sketch
	// column.
	s.numNullsCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})

	// A BYTES column with the sketch data.
	s.sketchCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES})
	s.outTypes = outTypes

	if err := s.init(post, outTypes, flowCtx, nil /* evalCtx */, output); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *samplerProcessor) pushTrailingMeta(ctx context.Context) {
	sendTraceData(ctx, s.out.output)
}

// Run is part of the Processor interface.
func (s *samplerProcessor) Run(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := processorSpan(s.flowCtx.Ctx, "sampler")
	defer tracing.FinishSpan(span)

	earlyExit, err := s.mainLoop(ctx)
	if err != nil {
		DrainAndClose(ctx, s.out.output, err, s.pushTrailingMeta, s.input)
	} else if !earlyExit {
		s.pushTrailingMeta(ctx)
		s.input.ConsumerClosed()
		s.out.Close()
	}
}

func (s *samplerProcessor) mainLoop(ctx context.Context) (earlyExit bool, _ error) {
	rng, _ := randutil.NewPseudoRand()
	var da sqlbase.DatumAlloc
	var buf []byte
	for {
		row, meta := s.input.Next()
		if meta != nil {
			if !emitHelper(ctx, &s.out, nil /* row */, meta, s.pushTrailingMeta, s.input) {
				// No cleanup required; emitHelper() took care of it.
				return true, nil
			}
			continue
		}
		if row == nil {
			break
		}

		for i := range s.sketches {
			// TODO(radu): for multi-column sketches, we will need to do this for all
			// columns.
			col := s.sketches[i].spec.Columns[0]
			s.sketches[i].numRows++
			if row[col].IsNull() {
				s.sketches[i].numNulls++
				continue
			}
			// We need to use a KEY encoding because equal values should have the same
			// encoding.
			// TODO(radu): a fast path for simple columns (like integer)?
			var err error
			buf, err = row[col].Encode(&s.outTypes[col], &da, sqlbase.DatumEncoding_ASCENDING_KEY, buf[:0])
			if err != nil {
				return false, err
			}
			s.sketches[i].sketch.Insert(buf)
		}

		// Use Int63 so we don't have headaches converting to DInt.
		rank := uint64(rng.Int63())
		s.sr.SampleRow(row, rank)
	}

	outRow := make(sqlbase.EncDatumRow, len(s.outTypes))
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}
	// Emit the sampled rows.
	for _, sample := range s.sr.Get() {
		copy(outRow, sample.Row)
		outRow[s.rankCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(sample.Rank))}
		if !emitHelper(ctx, &s.out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
			return true, nil
		}
	}
	// Release the memory for the sampled rows.
	s.sr = stats.SampleReservoir{}

	// Emit the sketch rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}

	for i, si := range s.sketches {
		outRow[s.sketchIdxCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
		outRow[s.numRowsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numRows))}
		outRow[s.numNullsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(si.numNulls))}
		data, err := si.sketch.MarshalBinary()
		if err != nil {
			return false, err
		}
		outRow[s.sketchCol] = sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(data))}
		if !emitHelper(ctx, &s.out, outRow, nil /* meta */, s.pushTrailingMeta, s.input) {
			return true, nil
		}
	}
	return false, nil
}
