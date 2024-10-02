// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecsel

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestLikeOperators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	rng, _ := randutil.NewTestRand()
	for _, caseInsensitive := range []bool{false, true} {
		for _, tc := range []struct {
			pattern    string
			isConstant bool
			negate     bool
			tups       colexectestutils.Tuples
			expected   colexectestutils.Tuples
		}{
			{
				pattern:    "def",
				isConstant: true,
				tups:       colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected:   colexectestutils.Tuples{{"def"}},
			},
			{
				pattern:    "def",
				isConstant: true,
				negate:     true,
				tups:       colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected:   colexectestutils.Tuples{{"abc"}, {"ghi"}},
			},
			{
				pattern:  "de%",
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"def"}},
			},
			{
				pattern:  "de%",
				negate:   true,
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"abc"}, {"ghi"}},
			},
			{
				pattern:  "%ef",
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"def"}},
			},
			{
				pattern:  "%ef",
				negate:   true,
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"abc"}, {"ghi"}},
			},
			{
				pattern:  "_e_",
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"def"}},
			},
			{
				pattern:  "_e_",
				negate:   true,
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"abc"}, {"ghi"}},
			},
			{
				pattern:  "%e%",
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"def"}},
			},
			{
				pattern:  "%e%",
				negate:   true,
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"abc"}, {"ghi"}},
			},
			// These two cases are equivalent to the two previous ones, but the
			// pattern is not normalized, so the slow regex matcher will be used.
			{
				pattern:  "%%e%",
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"def"}},
			},
			{
				pattern:  "%%e%",
				negate:   true,
				tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
				expected: colexectestutils.Tuples{{"abc"}, {"ghi"}},
			},
			{
				pattern:  "%a%e%",
				tups:     colexectestutils.Tuples{{"abc"}, {"adef"}, {"gahie"}, {"beb"}, {"ae"}},
				expected: colexectestutils.Tuples{{"adef"}, {"gahie"}, {"ae"}},
			},
			{
				pattern:  "%a%e%",
				negate:   true,
				tups:     colexectestutils.Tuples{{"abc"}, {"adef"}, {"gahie"}, {"beb"}, {"ae"}},
				expected: colexectestutils.Tuples{{"abc"}, {"beb"}},
			},
			{
				pattern: "%1%22%333%",
				tups: colexectestutils.Tuples{
					{"a1bc22def333fghi"},
					{"abc22def333fghi"}, // 1 is missing.
					{"a1bc2def333fghi"}, // 2 is missing.
					{"a1bc22def33fghi"}, // 3 is missing.
					{"122333"},
				},
				expected: colexectestutils.Tuples{{"a1bc22def333fghi"}, {"122333"}},
			},
			{
				pattern: "%1%22%333%",
				negate:  true,
				tups: colexectestutils.Tuples{
					{"a1bc22def333fghi"},
					{"abc22def333fghi"}, // 1 is missing.
					{"a1bc2def333fghi"}, // 2 is missing.
					{"a1bc22def33fghi"}, // 3 is missing.
					{"122333"},
				},
				expected: colexectestutils.Tuples{{"abc22def333fghi"}, {"a1bc2def333fghi"}, {"a1bc22def33fghi"}},
			},
		} {
			if caseInsensitive && tc.isConstant {
				// This case is currently not supported.
				continue
			}
			pattern, tups, expected := tc.pattern, tc.tups, tc.expected
			if caseInsensitive {
				// If we don't care about the case sensitivity, then randomly
				// modify the strings and / or the pattern to add some capital
				// letters.
				if rng.Float64() < 0.5 {
					pattern = strings.ToUpper(pattern)
				}
				var newExpected colexectestutils.Tuples
				for i := range tups {
					expectedIdx := len(newExpected)
					matched := expectedIdx < len(expected) && tups[i][0].(string) == expected[expectedIdx][0].(string)
					if rng.Float64() < 0.5 {
						tups[i][0] = strings.ToUpper(tups[i][0].(string))
					}
					if matched {
						newExpected = append(newExpected, tups[i])
					}
				}
				expected = newExpected
			}
			colexectestutils.RunTests(
				t, testAllocator, []colexectestutils.Tuples{tups}, expected, colexectestutils.OrderedVerifier,
				func(input []colexecop.Operator) (colexecop.Operator, error) {
					ctx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
					return GetLikeOperator(&ctx, input[0], 0, pattern, tc.negate, caseInsensitive)
				})
		}
	}
}

func BenchmarkLikeOps(b *testing.B) {
	defer log.Scope(b).Close(b)
	rng, _ := randutil.NewTestRand()
	ctx := context.Background()

	typs := []*types.T{types.Bytes}
	batch := testAllocator.NewMemBatchWithMaxCapacity(typs)
	col := batch.ColVec(0).Bytes()
	width := 64
	for i := 0; i < coldata.BatchSize(); i++ {
		col.Set(i, randutil.RandBytes(rng, width))
	}

	// Set a known prefix and suffix on half the batch so we're not filtering
	// everything out.
	prefix := "abc"
	suffix := "xyz"
	contains := "lmn"
	for i := 0; i < coldata.BatchSize()/2; i++ {
		copy(col.Get(i)[:3], prefix)
		copy(col.Get(i)[width-3:], suffix)
		copy(col.Get(i)[width/2:], contains)
	}

	batch.SetLength(coldata.BatchSize())
	source := colexecop.NewRepeatableBatchSource(testAllocator, batch, typs)
	source.Init(ctx)

	base := selConstOpBase{
		OneInputHelper: colexecop.MakeOneInputHelper(source),
		colIdx:         0,
	}
	prefixOp := &selPrefixBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(prefix),
	}
	suffixOp := &selSuffixBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(suffix),
	}
	containsOp := &selContainsBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(contains),
	}
	pattern := fmt.Sprintf("^%s.*%s$", prefix, suffix)
	regexpOp := &selRegexpBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       regexp.MustCompile(pattern),
	}
	skeletonOp := &selSkeletonBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       [][]byte{[]byte(prefix), []byte(contains), []byte(suffix)},
	}
	// Use the same pattern as we do for the skeleton case above to see what the
	// performance improvement of the optimized skeleton operator is.
	patternSkeleton := fmt.Sprintf("^%s.*%s.*%s$", prefix, contains, suffix)
	regexpSkeletonOp := &selRegexpBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       regexp.MustCompile(patternSkeleton),
	}

	testCases := []struct {
		name string
		op   colexecop.Operator
	}{
		{name: "selPrefixBytesBytesConstOp", op: prefixOp},
		{name: "selSuffixBytesBytesConstOp", op: suffixOp},
		{name: "selContainsBytesBytesConstOp", op: containsOp},
		{name: "selRegexpBytesBytesConstOp", op: regexpOp},
		{name: "selSkeletonBytesBytesConstOp", op: skeletonOp},
		{name: "selRegexpSkeleton", op: regexpSkeletonOp},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			tc.op.Init(ctx)
			b.SetBytes(int64(width * coldata.BatchSize()))
			for i := 0; i < b.N; i++ {
				tc.op.Next()
			}
		})
	}
}
