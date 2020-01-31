// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestLikeOperators(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range []struct {
		pattern  string
		negate   bool
		tups     tuples
		expected tuples
	}{
		{
			pattern:  "def",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "def",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "de%",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "de%",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "%ef",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "%ef",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "_e_",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "_e_",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
		{
			pattern:  "%e%",
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"def"}},
		},
		{
			pattern:  "%e%",
			negate:   true,
			tups:     tuples{{"abc"}, {"def"}, {"ghi"}},
			expected: tuples{{"abc"}, {"ghi"}},
		},
	} {
		runTests(
			t, []tuples{tc.tups}, tc.expected, orderedVerifier,
			func(input []Operator) (Operator, error) {
				ctx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
				return GetLikeOperator(&ctx, input[0], 0, tc.pattern, tc.negate)
			})
	}
}

func BenchmarkLikeOps(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	batch := testAllocator.NewMemBatch([]coltypes.T{coltypes.Bytes})
	col := batch.ColVec(0).Bytes()
	width := 64
	for i := 0; i < int(coldata.BatchSize()); i++ {
		col.Set(i, randutil.RandBytes(rng, width))
	}

	// Set a known prefix and suffix on half the batch so we're not filtering
	// everything out.
	prefix := "abc"
	suffix := "xyz"
	for i := 0; i < int(coldata.BatchSize())/2; i++ {
		copy(col.Get(i)[:3], prefix)
		copy(col.Get(i)[width-3:], suffix)
	}

	batch.SetLength(coldata.BatchSize())
	source := NewRepeatableBatchSource(testAllocator, batch)
	source.Init()

	base := selConstOpBase{
		OneInputNode: NewOneInputNode(source),
		colIdx:       0,
	}
	prefixOp := &selPrefixBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(prefix),
	}
	suffixOp := &selSuffixBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       []byte(suffix),
	}
	pattern := fmt.Sprintf("^%s.*%s$", prefix, suffix)
	regexpOp := &selRegexpBytesBytesConstOp{
		selConstOpBase: base,
		constArg:       regexp.MustCompile(pattern),
	}

	testCases := []struct {
		name string
		op   Operator
	}{
		{name: "selPrefixBytesBytesConstOp", op: prefixOp},
		{name: "selSuffixBytesBytesConstOp", op: suffixOp},
		{name: "selRegexpBytesBytesConstOp", op: regexpOp},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			tc.op.Init()
			b.SetBytes(int64(width * int(coldata.BatchSize())))
			for i := 0; i < b.N; i++ {
				tc.op.Next(ctx)
			}
		})
	}
}
