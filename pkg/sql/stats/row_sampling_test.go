// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// runSampleTest feeds rows with the given ranks through a reservoir
// of a given size and verifies the results are correct.
func runSampleTest(
	t *testing.T,
	evalCtx *eval.Context,
	numSamples, expectedNumSamples int,
	ranks []int,
	memAcc *mon.BoundAccount,
) {
	ctx := context.Background()
	var sr SampleReservoir
	sr.Init(numSamples, 1, []*types.T{types.Int}, memAcc, intsets.MakeFast(0))
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
	evalCtx := eval.MakeTestingEvalContext(st)

	for _, n := range []int{10, 100, 1000, 10000} {
		rng, _ := randutil.NewTestRand()
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
					monitor := mon.NewMonitor(mon.Options{
						Name:      mon.MakeMonitorName("test-monitor"),
						Limit:     mem,
						Increment: 1,
						Settings:  st,
					})
					monitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
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
	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	runTest := func(d, expected tree.Datum) {
		actual := truncateDatum(&evalCtx, d, 10 /* maxBytes */)
		if cmp, err := actual.Compare(ctx, &evalCtx, expected); err != nil {
			t.Fatal(err)
		} else if cmp != 0 {
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

// TestSampleReservoirMemAccounting is a regression test for a bug in the memory
// accounting that could lead to "no bytes in account to release" error
// (#128241).
//
// In particular, it constructs such sequence of events that we hit the memory
// budget error in the middle of copying a new row into the reservoir, and then
// later (before the fix was applied) we pop the partially modified row from it,
// deviating from the accounting done so far.
func TestSampleReservoirMemAccounting(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)

	getStringDatum := func(l int) rowenc.EncDatum {
		d := tree.DString(strings.Repeat("a", l))
		return rowenc.DatumToEncDatum(types.String, &d)
	}
	// First two rows need 152 bytes each. The third row has the smallest rank,
	// so it wants to replace one of the other rows, but it has a large datum
	// exceeding the memory limit altogether.
	const memLimit = 304
	rows := []rowenc.EncDatumRow{
		{getStringDatum(0), getStringDatum(0)},                 // rank 3
		{getStringDatum(0), getStringDatum(0)},                 // rank 2
		{getStringDatum(maxBytesPerSample), getStringDatum(0)}, // rank 1
	}
	monitor := mon.NewMonitor(mon.Options{
		Name:      mon.MakeMonitorName("test-monitor"),
		Limit:     memLimit,
		Increment: 1,
		Settings:  st,
	})
	monitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
	memAcc := monitor.MakeBoundAccount()
	var sr SampleReservoir
	sr.Init(2, 1, []*types.T{types.String, types.String}, &memAcc, intsets.MakeFast(0, 1))
	require.NoError(t, sr.SampleRow(ctx, &evalCtx, rows[0], 3))
	require.NoError(t, sr.SampleRow(ctx, &evalCtx, rows[1], 2))
	err := sr.SampleRow(ctx, &evalCtx, rows[2], 1)
	require.Error(t, err)
	require.True(t, testutils.IsError(err, "memory budget exceeded"))
}
