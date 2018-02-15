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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestRowContainerReplaceMax verifies that MaybeReplaceMax correctly adjusts
// the memory accounting.
func TestRowContainerReplaceMax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rng, _ := randutil.NewPseudoRand()

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	typeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	typeStr := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_STRING}

	makeRow := func(intVal int, strLen int) sqlbase.EncDatumRow {
		var b []byte
		for i := 0; i < strLen; i++ {
			b = append(b, 'a')
		}
		return sqlbase.EncDatumRow{
			sqlbase.DatumToEncDatum(typeInt, tree.NewDInt(tree.DInt(intVal))),
			sqlbase.DatumToEncDatum(typeStr, tree.NewDString(string(b))),
		}
	}

	m := mon.MakeUnlimitedMonitor(
		context.Background(), "test", mon.MemoryResource, nil, nil, math.MaxInt64, st,
	)
	defer m.Stop(ctx)

	var mc memRowContainer
	mc.initWithMon(
		sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
		[]sqlbase.ColumnType{typeInt, typeStr}, evalCtx, &m,
	)
	defer mc.Close(ctx)

	// Initialize the heap with small rows.
	for i := 0; i < 1000; i++ {
		err := mc.AddRow(ctx, makeRow(rng.Intn(10000), rng.Intn(10)))
		if err != nil {
			t.Fatal(err)
		}
	}
	mc.InitMaxHeap()
	// Replace some of the rows with large rows.
	for i := 0; i < 1000; i++ {
		err := mc.MaybeReplaceMax(ctx, makeRow(rng.Intn(10000), rng.Intn(100)))
		if err != nil {
			t.Fatal(err)
		}
	}
	// Now pop the rows, which shrinks the memory account according to the current
	// row sizes. If we did not account for the larger rows, this will panic.
	for mc.Len() > 0 {
		mc.PopFirst()
	}
}
