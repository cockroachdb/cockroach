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

package stats

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runSampleTest feeds rows with the given ranks through a reservoir
// of a given size and verifies the results are correct.
func runSampleTest(t *testing.T, numSamples int, ranks []int) {
	typeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	var sr SampleReservoir
	sr.Init(numSamples)
	for _, r := range ranks {
		d := sqlbase.DatumToEncDatum(typeInt, tree.NewDInt(tree.DInt(r)))
		sr.SampleRow(sqlbase.EncDatumRow{d}, uint64(r))
	}
	samples := sr.Get()
	sampledRanks := make([]int, len(samples))

	// Verify that the row and the ranks weren't mishandled.
	for i, s := range samples {
		if *s.Row[0].Datum.(*tree.DInt) != tree.DInt(s.Rank) {
			t.Fatalf(
				"mismatch between row %s and rank %d", s.Row.String([]sqlbase.ColumnType{typeInt}), s.Rank,
			)
		}
		sampledRanks[i] = int(s.Rank)
	}

	// Verify the top ranks made it.
	sort.Ints(sampledRanks)
	expected := append([]int(nil), ranks...)
	sort.Ints(expected)
	if len(expected) > numSamples {
		expected = expected[:numSamples]
	}
	if !reflect.DeepEqual(expected, sampledRanks) {
		t.Errorf("invalid ranks: %v vs %v", sampledRanks, expected)
	}
}

func TestSampleReservoir(t *testing.T) {
	for _, n := range []int{10, 100, 1000, 10000} {
		rng, _ := randutil.NewPseudoRand()
		ranks := make([]int, n)
		for i := range ranks {
			ranks[i] = rng.Int()
		}
		for _, k := range []int{1, 5, 10, 100} {
			t.Run(fmt.Sprintf("%d/%d", n, k), func(t *testing.T) {
				runSampleTest(t, k, ranks)
			})
		}
	}
}
