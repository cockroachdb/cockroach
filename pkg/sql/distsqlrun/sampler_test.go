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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// runSampler runs the sampler aggregator on numRows and returns numSamples rows.
func runSampler(t *testing.T, numRows, numSamples int) []int {
	rows := make([]sqlbase.EncDatumRow, numRows)
	for i := range rows {
		rows[i] = sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(intType, parser.NewDInt(parser.DInt(i)))}
	}
	in := NewRowBuffer(oneIntCol, rows, RowBufferArgs{})
	outTypes := []sqlbase.ColumnType{
		intType, // original column
		intType, // rank
		intType, // sketch index
		intType, // num rows
		intType, // null vals
		{SemanticType: sqlbase.ColumnType_BYTES},
	}

	out := NewRowBuffer(outTypes, nil /* rows */, RowBufferArgs{})

	evalCtx := parser.MakeTestingEvalContext()
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		EvalCtx:  evalCtx,
	}

	spec := &SamplerSpec{SampleSize: uint32(numSamples)}
	p, err := newSamplerProcessor(&flowCtx, spec, in, &PostProcessSpec{}, out)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(context.Background(), nil)

	// Verify we have numSamples distinct rows.
	res := make([]int, 0, numSamples)
	seen := make(map[parser.DInt]bool)
	n := 0
	for {
		row := out.NextNoMeta(t)
		if row == nil {
			break
		}
		for i := 2; i < len(outTypes); i++ {
			if row[i].Datum != parser.DNull {
				t.Fatalf("expected NULL on column %d, got %s", i, row[i].Datum)
			}
		}
		v := *row[0].Datum.(*parser.DInt)
		if seen[v] {
			t.Fatalf("duplicate row %d", v)
		}
		seen[v] = true
		res = append(res, int(v))
		n++
	}
	if n != numSamples {
		t.Fatalf("expected %d rows, got %d", numSamples, n)
	}
	return res
}

func TestSampler(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We run many samplings and record the frequencies.
	numRows := 100
	numSamples := 20
	numRuns := 1000
	delta := 0.5

	freq := make([]int, numRows)
	for r := 0; r < numRuns; r++ {
		for _, v := range runSampler(t, numRows, numSamples) {
			freq[v]++
		}
	}

	// The expected frequency of each row is f = numRuns * (numSamples / numRows).
	f := float64(numRuns) * float64(numSamples) / float64(numRows)

	// Verify that no frequency is outside of the range (f / (1+delta), f * (1+delta));
	// the probability of a given row violating this is subject to the Chernoff
	// bound which decreases exponentially (with exponent f).
	for i := range freq {
		if float64(freq[i]) < f/(1+delta) || float64(freq[i]) > f*(1+delta) {
			t.Errorf("frequency %d out of bound (expected value %f)", freq[i], f)
		}
	}
}
