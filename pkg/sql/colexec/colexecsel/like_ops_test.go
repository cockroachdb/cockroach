// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecsel

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestLikeOperators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, tc := range []struct {
		pattern  string
		negate   bool
		tups     colexectestutils.Tuples
		expected colexectestutils.Tuples
	}{
		{
			pattern:  "def",
			tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: colexectestutils.Tuples{{"def"}},
		},
		{
			pattern:  "def",
			negate:   true,
			tups:     colexectestutils.Tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: colexectestutils.Tuples{{"abc"}, {"ghi"}},
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
	} {
		colexectestutils.RunTests(
			t, testAllocator, []colexectestutils.Tuples{tc.tups}, tc.expected, colexectestutils.OrderedVerifier,
			func(input []colexecop.Operator) (colexecop.Operator, error) {
				ctx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
				return GetLikeOperator(&ctx, input[0], 0, tc.pattern, tc.negate)
			})
	}
}

func BenchmarkLikeOps(b *testing.B) {
	defer log.Scope(b).Close(b)
	rng, _ := randutil.NewPseudoRand()
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

	testCases := []struct {
		name string
		op   colexecop.Operator
	}{
		{name: "selPrefixBytesBytesConstOp", op: prefixOp},
		{name: "selSuffixBytesBytesConstOp", op: suffixOp},
		{name: "selContainsBytesBytesConstOp", op: containsOp},
		{name: "selRegexpBytesBytesConstOp", op: regexpOp},
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
