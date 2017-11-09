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

	"github.com/axiomhq/hyperloglog"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// TODO(radu): for now just force the import.
var _ hyperloglog.Sketch

// A sampler processor returns a random sample of rows, as well as "global"
// statistics (including cardinality estimation sketch data). See SamplerSpec
// for more details.
type samplerProcessor struct {
	processorBase

	flowCtx    *FlowCtx
	input      RowSource
	sr         sqlbase.SampleReservoir
	sketchInfo []SamplerSpec_SketchInfo
	outTypes   []sqlbase.ColumnType
	// Output column indices for special columns.
	rankCol      int
	sketchIdxCol int
	numRowsCol   int
	nullValsCol  int
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
		flowCtx:    flowCtx,
		input:      input,
		sketchInfo: spec.Sketches,
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

	if err := s.init(post, outTypes, flowCtx, output); err != nil {
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
	sketches := make([]*hyperloglog.Sketch, len(s.sketchInfo))
	for i := range sketches {
		sketches[i] = hyperloglog.New14()
	}

	var numRows int64
	numNulls := make([]int64, len(s.sketchInfo))

	rng, _ := randutil.NewPseudoRand()
	var da sqlbase.DatumAlloc
	var buf []byte
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
		numRows++

		for i := range s.sketchInfo {
			col := s.sketchInfo[i].Columns[0]
			if row[col].IsNull() {
				numNulls[i]++
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
			sketches[i].Insert(buf)
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
		if !emitHelper(ctx, &s.out, outRow, ProducerMetadata{}, s.input) {
			return true, nil
		}
	}
	// Release the memory for the sampled rows.
	s.sr = sqlbase.SampleReservoir{}

	// Emit the sketch rows.
	for i := range outRow {
		outRow[i] = sqlbase.DatumToEncDatum(s.outTypes[i], tree.DNull)
	}

	for i := range s.sketchInfo {
		outRow[s.sketchIdxCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
		outRow[s.numRowsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(numRows))}
		outRow[s.nullValsCol] = sqlbase.EncDatum{Datum: tree.NewDInt(tree.DInt(numNulls[i]))}
		data, err := sketches[i].MarshalBinary()
		if err != nil {
			return false, err
		}
		outRow[s.sketchCol] = sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(data))}
		if !emitHelper(ctx, &s.out, outRow, ProducerMetadata{}, s.input) {
			return true, nil
		}
	}
	return false, nil
}
