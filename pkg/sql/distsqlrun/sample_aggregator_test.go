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
	"context"
	gosql "database/sql"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSampleAggregator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	server, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(context.TODO())

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(context.Background())
	flowCtx := FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
		Gossip:   server.Gossip(),
		ClientDB: kvDB,
		executor: server.InternalExecutor().(sqlutil.InternalExecutor),
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

	// We randomly distribute the input rows between multiple Samplers and
	// aggregate the results.
	numSamplers := 3

	samplerOutTypes := []types.T{
		*types.Int,   // original column
		*types.Int,   // original column
		*types.Int,   // rank
		*types.Int,   // sketch index
		*types.Int,   // num rows
		*types.Int,   // null vals
		*types.Bytes, // sketch data
	}

	sketchSpecs := []distsqlpb.SketchSpec{
		{
			SketchType:        distsqlpb.SketchType_HLL_PLUS_PLUS_V1,
			Columns:           []uint32{0},
			GenerateHistogram: false,
			StatName:          "a",
		},
		{
			SketchType:          distsqlpb.SketchType_HLL_PLUS_PLUS_V1,
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
		rows := sqlbase.GenEncDatumRowsInt(rowPartitions[i])
		in := NewRowBuffer(sqlbase.TwoIntCols, rows, RowBufferArgs{})
		outputs[i] = NewRowBuffer(samplerOutTypes, nil /* rows */, RowBufferArgs{})

		spec := &distsqlpb.SamplerSpec{SampleSize: 100, Sketches: sketchSpecs}
		p, err := newSamplerProcessor(&flowCtx, 0 /* processorID */, spec, in, &distsqlpb.PostProcessSpec{}, outputs[i])
		if err != nil {
			t.Fatal(err)
		}
		p.Run(context.Background())
	}
	// Randomly interleave the output rows from the samplers into a single buffer.
	samplerResults := NewRowBuffer(samplerOutTypes, nil /* rows */, RowBufferArgs{})
	for len(outputs) > 0 {
		i := rng.Intn(len(outputs))
		row, meta := outputs[i].Next()
		if meta != nil {
			if meta.SamplerProgress == nil {
				t.Fatalf("unexpected metadata: %v", meta)
			}
		} else if row == nil {
			outputs = append(outputs[:i], outputs[i+1:]...)
		} else {
			samplerResults.Push(row, nil /* meta */)
		}
	}

	// Now run the sample aggregator.
	finalOut := NewRowBuffer([]types.T{}, nil /* rows*/, RowBufferArgs{})
	spec := &distsqlpb.SampleAggregatorSpec{
		SampleSize:       100,
		Sketches:         sketchSpecs,
		SampledColumnIDs: []sqlbase.ColumnID{100, 101},
		TableID:          13,
	}

	agg, err := newSampleAggregator(
		&flowCtx, 0 /* processorID */, spec, samplerResults, &distsqlpb.PostProcessSpec{}, finalOut,
	)
	if err != nil {
		t.Fatal(err)
	}
	agg.Run(context.Background())
	// Make sure there was no error.
	finalOut.GetRowsNoMeta(t)
	r := sqlutils.MakeSQLRunner(sqlDB)

	rows := r.Query(t, `
	  SELECT "tableID",
					 "name",
					 "columnIDs",
					 "rowCount",
					 "distinctCount",
					 "nullCount",
					 histogram
	  FROM system.table_statistics
  `)
	defer rows.Close()

	type resultBucket struct {
		numEq, numRange, upper int
	}

	type result struct {
		tableID                            int
		name, colIDs                       string
		rowCount, distinctCount, nullCount int
		buckets                            []resultBucket
	}

	expected := []result{
		{
			tableID:       13,
			name:          "a",
			colIDs:        "{100}",
			rowCount:      11,
			distinctCount: 2,
			nullCount:     2,
		},
		{
			tableID:       13,
			name:          "<NULL>",
			colIDs:        "{101}",
			rowCount:      11,
			distinctCount: 8,
			nullCount:     1,
			buckets: []resultBucket{
				{numEq: 2, numRange: 0, upper: 1},
				{numEq: 2, numRange: 1, upper: 3},
				{numEq: 1, numRange: 1, upper: 5},
				{numEq: 1, numRange: 2, upper: 8},
			},
		},
	}

	for _, exp := range expected {
		if !rows.Next() {
			t.Fatal("fewer rows than expected")
		}

		var histData []byte
		var name gosql.NullString
		var r result
		if err := rows.Scan(
			&r.tableID, &name, &r.colIDs, &r.rowCount, &r.distinctCount, &r.nullCount, &histData,
		); err != nil {
			t.Fatal(err)
		}
		if name.Valid {
			r.name = name.String
		} else {
			r.name = "<NULL>"
		}

		if len(histData) > 0 {
			var h stats.HistogramData
			if err := protoutil.Unmarshal(histData, &h); err != nil {
				t.Fatal(err)
			}

			for _, b := range h.Buckets {
				ed, _, err := sqlbase.EncDatumFromBuffer(
					types.Int, sqlbase.DatumEncoding_ASCENDING_KEY, b.UpperBound,
				)
				if err != nil {
					t.Fatal(err)
				}
				var d sqlbase.DatumAlloc
				if err := ed.EnsureDecoded(types.Int, &d); err != nil {
					t.Fatal(err)
				}
				r.buckets = append(r.buckets, resultBucket{
					numEq:    int(b.NumEq),
					numRange: int(b.NumRange),
					upper:    int(*ed.Datum.(*tree.DInt)),
				})
			}
		} else if len(exp.buckets) > 0 {
			t.Error("no histogram")
		}

		if !reflect.DeepEqual(exp, r) {
			t.Errorf("Expected:\n  %v\ngot:\n  %v", exp, r)
		}
	}
	if rows.Next() {
		t.Fatal("more rows than expected")
	}
}
