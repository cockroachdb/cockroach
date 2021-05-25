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
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// runSampleTest feeds rows with the given ranks through a reservoir
// of a given size and verifies the results are correct.
func runSampleTest(
	t *testing.T,
	evalCtx *tree.EvalContext,
	numSamples, expectedNumSamples int,
	ranks []int,
	memAcc *mon.BoundAccount,
) {
	ctx := context.Background()
	var sr SampleReservoir
	sr.Init(numSamples, 1, []*types.T{types.Int}, memAcc, util.MakeFastIntSet(0))
	for _, r := range ranks {
		d := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(r)))
		prevCapacity := sr.Cap()
		if err := sr.SampleRow(ctx, evalCtx, rowenc.EncDatumRow{d}, uint64(r)); err != nil {
			t.Fatal(err)
		} else if sr.Cap() != prevCapacity {
			t.Logf(
				"samples reduced from %d to %d during SampleRow",
				prevCapacity, sr.Cap(),
			)
		}
	}

	// Verify that the row and the ranks weren't mishandled.
	for _, s := range sr.Get() {
		if *s.Row[0].Datum.(*tree.DInt) != tree.DInt(s.Rank) {
			t.Fatalf(
				"mismatch between row %s and rank %d",
				s.Row.String([]*types.T{types.Int}), s.Rank,
			)
		}
	}

	prevCapacity := sr.Cap()
	values, err := sr.GetNonNullDatums(ctx, memAcc, 0 /* colIdx */)
	if err != nil {
		t.Fatal(err)
	} else if sr.Cap() != prevCapacity {
		t.Logf(
			"samples reduced from %d to %d during GetNonNullDatums",
			prevCapacity, sr.Cap(),
		)
	}

	sampledRanks := make([]int, len(values))
	for i, v := range values {
		sampledRanks[i] = int(*v.(*tree.DInt))
	}

	// Verify that the top (smallest) ranks made it.
	sort.Ints(sampledRanks)
	expected := append([]int(nil), ranks...)
	sort.Ints(expected)
	if len(expected) > expectedNumSamples {
		expected = expected[:expectedNumSamples]
	}
	if !reflect.DeepEqual(expected, sampledRanks) {
		t.Errorf("invalid ranks: %v vs %v", sampledRanks, expected)
	}
}

func TestSampleReservoir(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)

	for _, n := range []int{10, 100, 1000, 10000} {
		rng, _ := randutil.NewPseudoRand()
		ranks := make([]int, n)
		for i := range ranks {
			ranks[i] = rng.Int()
		}
		for _, k := range []int{1, 5, 10, 100} {
			t.Run(fmt.Sprintf("n=%d/k=%d/mem=nolimit", n, k), func(t *testing.T) {
				runSampleTest(t, &evalCtx, k, k, ranks, nil)
			})
			for _, mem := range []int64{1 << 8, 1 << 10, 1 << 12} {
				t.Run(fmt.Sprintf("n=%d/k=%d/mem=%d", n, k, mem), func(t *testing.T) {
					monitor := mon.NewMonitorWithLimit(
						"test-monitor",
						mon.MemoryResource,
						mem,
						nil,
						nil,
						1,
						math.MaxInt64,
						st,
					)
					monitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
					memAcc := monitor.MakeBoundAccount()
					expectedK := k
					if mem == 1<<8 && n > 1 && k > 1 {
						expectedK = 1
					} else if mem == 1<<10 && n > 10 && k > 10 {
						expectedK = 6
					} else if mem == 1<<12 && n > 10 && k > 10 {
						expectedK = 25
					}
					runSampleTest(t, &evalCtx, k, expectedK, ranks, &memAcc)
				})
			}
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
