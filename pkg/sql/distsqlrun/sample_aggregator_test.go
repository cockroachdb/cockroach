// Copyright 2016 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"golang.org/x/net/context"
)

func TestSampleAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		EvalCtx:  evalCtx,
	}

	inputRows := [][]int{
		{-1, 1},
		{1, 1},
		{2, 2},
		{1, 3},
		{2, 4},
		{1, 5},
		{2, 6},
		{1, 7},
		{2, 8},
		{-1, 3},
		{1, -1},
	}
	//cardinalities := []int{2, 8}
	//numNulls := []int{2, 1}

	// We randomly distribute the input rows between multiple Samplers and
	// aggregate the results.
	numSamplers := 3

	samplerOutTypes := []sqlbase.ColumnType{
		intType, // original column
		intType, // original column
		intType, // rank
		intType, // sketch index
		intType, // num rows
		intType, // null vals
		{SemanticType: sqlbase.ColumnType_BYTES}, // sketch data
	}

	sketchSpecs := []SketchSpec{
		{
			SketchType:        SketchType_HLL_PLUS_PLUS_V1,
			Columns:           []uint32{0},
			GenerateHistogram: false,
		},
		{
			SketchType:          SketchType_HLL_PLUS_PLUS_V1,
			Columns:             []uint32{1},
			GenerateHistogram:   true,
			HistogramMaxBuckets: 4,
		},
	}

	rng, _ := randutil.NewPseudoRand()
	rowPartitions := make([][][]int, numSamplers)
	for _, row := range inputRows {
		j := rng.Intn(numSamplers)
		rowPartitions[j] = append(rowPartitions[j], row)
	}

	outputs := make([]*RowBuffer, numSamplers)
	for i := 0; i < numSamplers; i++ {
		rows := genEncDatumRowsInt(rowPartitions[i])
		in := NewRowBuffer(twoIntCols, rows, RowBufferArgs{})
		outputs[i] = NewRowBuffer(samplerOutTypes, nil /* rows */, RowBufferArgs{})

		spec := &SamplerSpec{SampleSize: 100, Sketches: sketchSpecs}
		p, err := newSamplerProcessor(&flowCtx, spec, in, &PostProcessSpec{}, outputs[i])
		if err != nil {
			t.Fatal(err)
		}
		p.Run(context.Background(), nil /* wg */)
	}
	// Randomly interleave the output rows from the samplers into a single buffer.
	samplerResults := NewRowBuffer(samplerOutTypes, nil /* rows */, RowBufferArgs{})
	for len(outputs) > 0 {
		i := rng.Intn(len(outputs))
		row := outputs[i].NextNoMeta(t)
		if row == nil {
			outputs = append(outputs[:i], outputs[i+1:]...)
		} else {
			samplerResults.Push(row, ProducerMetadata{})
		}
	}

	// Now run the sample aggregator.
	finalOut := NewRowBuffer([]sqlbase.ColumnType{}, nil /* rows*/, RowBufferArgs{})
	spec := &SampleAggregatorSpec{
		SampleSize:       100,
		Sketches:         sketchSpecs,
		SampledColumnIDs: []sqlbase.ColumnID{100, 101},
	}

	agg, err := newSampleAggregator(&flowCtx, spec, samplerResults, &PostProcessSpec{}, finalOut)
	if err != nil {
		t.Fatal(err)
	}
	agg.Run(context.Background(), nil /* wg */)
	// Make sure there was no error.
	finalOut.GetRowsNoMeta(t)
}
