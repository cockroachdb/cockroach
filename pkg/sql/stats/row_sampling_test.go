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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runSampleTest feeds rows with the given ranks through a reservoir
// of a given size and verifies the results are correct.
func runSampleTest(t *testing.T, evalCtx *tree.EvalContext, numSamples int, ranks []int) {
	ctx := context.Background()
	var sr SampleReservoir
	sr.Init(numSamples, []*types.T{types.Int}, nil /* memAcc */, util.MakeFastIntSet(0))
	for _, r := range ranks {
		d := sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(r)))
		if err := sr.SampleRow(ctx, evalCtx, sqlbase.EncDatumRow{d}, uint64(r)); err != nil {
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
				s.Row.String([]*types.T{types.Int}), s.Rank,
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
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, n := range []int{10, 100, 1000, 10000} {
		rng, _ := randutil.NewPseudoRand()
		ranks := make([]int, n)
		for i := range ranks {
			ranks[i] = rng.Int()
		}
		for _, k := range []int{1, 5, 10, 100} {
			t.Run(fmt.Sprintf("%d/%d", n, k), func(t *testing.T) {
				runSampleTest(t, &evalCtx, k, ranks)
			})
		}
	}
}

func TestTruncateDatum(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	runTest := func(d, expected tree.Datum) {
		actual := truncateDatum(&evalCtx, d, 10 /* maxBytes */)
		if actual.Compare(&evalCtx, expected) != 0 {
			t.Fatalf("expected %s but found %s", expected.String(), actual.String())
		}
	}

	original1, err := tree.ParseDBitArray("0110110101111100001100110110101111100001100110110101111" +
		"10000110011011010111110000110011011010111110000110011011010111110000110")
	if err != nil {
		t.Fatal(err)
	}
	expected1, err := tree.ParseDBitArray("0110110101111100001100110110101111100001100110110101111" +
		"1000011001101101011111000")
	if err != nil {
		t.Fatal(err)
	}
	runTest(original1, expected1)

	original2 := tree.DBytes("deadbeef1234567890")
	expected2 := tree.DBytes("deadbeef12")
	runTest(&original2, &expected2)

	original3 := tree.DString("Hello 世界")
	expected3 := tree.DString("Hello 世")
	runTest(&original3, &expected3)

	original4, err := tree.NewDCollatedString(`IT was lovely summer weather in the country, and the golden
corn, the green oats, and the haystacks piled up in the meadows looked beautiful`,
		"en_US", &tree.CollationEnvironment{})
	if err != nil {
		t.Fatal(err)
	}
	expected4, err := tree.NewDCollatedString("IT was lov", "en_US", &tree.CollationEnvironment{})
	if err != nil {
		t.Fatal(err)
	}
	runTest(original4, expected4)
}
