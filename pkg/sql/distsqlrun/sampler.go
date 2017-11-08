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
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// A sampler processor returns a random sample of rows, as well as "global"
// statistics (including cardinality estimation sketch data). See SamplerSpec
// for more details.
type samplerProcessor struct {
	processorBase

	flowCtx  *FlowCtx
	input    RowSource
	sr       sqlbase.SampleReservoir
	sketches []SamplerSpec_SketchInfo
	outTypes []sqlbase.ColumnType
	// Output column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	nullValsCol  int
	sketchCol    int
}

var _ Processor = &samplerProcessor{}

// TODO(radu): no sketches supported yet
var supportedSketchTypes = map[SketchType]struct{}{}

func newSamplerProcessor(
	flowCtx *FlowCtx, spec *SamplerSpec, input RowSource, post *PostProcessSpec, output RowReceiver,
) (*samplerProcessor, error) {
	for _, s := range spec.Sketches {
		if _, ok := supportedSketchTypes[s.SketchType]; !ok {
			return nil, errors.Errorf("unsupported sketch type %s", s.SketchType)
		}
	}

	s := &samplerProcessor{
		flowCtx:  flowCtx,
		input:    input,
		sketches: spec.Sketches,
	}

	s.sr.Init(int(spec.SampleSize))

	inTypes := input.Types()
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

	// An INT column indicating the number of NULL values on the first column
	// of the sketch.
	s.nullValsCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT})

	// A BYTES column with the sketch data.
	s.sketchCol = len(outTypes)
	outTypes = append(outTypes, sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_BYTES})
	s.outTypes = outTypes

	if err := s.out.Init(post, outTypes, &flowCtx.EvalCtx, output); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is part of the Processor interface.
func (s *samplerProcessor) Run(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	ctx, span := processorSpan(ctx, "sampler")
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

func (s *samplerProcessor) mainLoop(ctx context.Context) (earlyExit bool, _ error) {
	rng, _ := randutil.NewPseudoRand()
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
		// Use Int63 so we don't have headaches converting to DInt.
		rank := uint64(rng.Int63())
		s.sr.SampleRow(row, rank)
	}

	outRow := make(sqlbase.EncDatumRow, len(s.outTypes))
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], parser.DNull)
	}
	// Emit the sampled rows.
	for _, sample := range s.sr.Get() {
		copy(outRow, sample.Row)
		outRow[s.rankCol] = sqlbase.DatumToEncDatum(
			s.outTypes[s.rankCol], parser.NewDInt(parser.DInt(sample.Rank)),
		)
		if !emitHelper(ctx, &s.out, outRow, ProducerMetadata{}, s.input) {
			return true, nil
		}
	}
	return false, nil
}
