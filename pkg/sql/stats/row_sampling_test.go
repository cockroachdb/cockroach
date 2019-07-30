// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runSampleTest feeds rows with the given ranks through a reservoir
// of a given size and verifies the results are correct.
func runSampleTest(t *testing.T, numSamples int, ranks []int) {
	ctx := context.Background()
	var sr SampleReservoir
	sr.Init(numSamples, []types.T{*types.Int}, nil /* memAcc */)
	for _, r := range ranks {
		d := sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(r)))
		if err := sr.SampleRow(ctx, sqlbase.EncDatumRow{d}, uint64(r)); err != nil {
			t.Errorf("%v", err)
		}
	}
	samples := sr.Get()
	sampledRanks := make([]int, len(samples))

	// Verify that the row and the ranks weren't mishandled.
	for i, s := range samples {
		if *s.Row[0].Datum.(*tree.DInt) != tree.DInt(s.Rank) {
			t.Fatalf(
				"mismatch between row %s and rank %d",
				s.Row.String([]types.T{*types.Int}), s.Rank,
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
