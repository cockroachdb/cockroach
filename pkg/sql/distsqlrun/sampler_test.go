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

func TestSampler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rows := make([]sqlbase.EncDatumRow, 1000)
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

	const k = 50
	p, err := newSamplerProcessor(&flowCtx, &SamplerSpec{SampleSize: k}, in, &PostProcessSpec{}, out)
	if err != nil {
		t.Fatal(err)
	}
	p.Run(context.Background(), nil)

	// Verify we have k distinct rows.
	seen := make(map[parser.DInt]bool)
	n := 0
	for {
		row, meta := out.Next()
		if !meta.Empty() {
			t.Fatalf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		for i := 2; i < len(outTypes); i++ {
			if row[i].Datum != parser.DNull {
				t.Errorf("expected NULL on column %d, got %s", i, row[i].Datum)
			}
		}
		v := *row[0].Datum.(*parser.DInt)
		if seen[v] {
			t.Errorf("duplicate row %d", v)
		}
		seen[v] = true
		n++
	}
	if n != k {
		t.Errorf("expected %d rows, got %d", k, n)
	}
}
