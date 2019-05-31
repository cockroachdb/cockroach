// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestSelPrefixBytesBytesConstOp(t *testing.T) {
	tups := tuples{{"abc"}, {"def"}, {"ghi"}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selPrefixBytesBytesConstOp{
			input:    input[0],
			colIdx:   0,
			constArg: []byte("de"),
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{"def"}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelSuffixBytesBytesConstOp(t *testing.T) {
	tups := tuples{{"abc"}, {"def"}, {"ghi"}}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selSuffixBytesBytesConstOp{
			input:    input[0],
			colIdx:   0,
			constArg: []byte("ef"),
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{"def"}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func TestSelRegexpBytesBytesConstOp(t *testing.T) {
	tups := tuples{{"abc"}, {"def"}, {"ghi"}}
	pattern, err := regexp.Compile(".e.")
	if err != nil {
		t.Fatal(err)
	}
	runTests(t, []tuples{tups}, func(t *testing.T, input []Operator) {
		op := selRegexpBytesBytesConstOp{
			input:    input[0],
			colIdx:   0,
			constArg: pattern,
		}
		op.Init()
		out := newOpTestOutput(&op, []int{0}, tuples{{"def"}})
		if err := out.Verify(); err != nil {
			t.Error(err)
		}
	})
}

func BenchmarkLikeOps(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	batch := coldata.NewMemBatch([]types.T{types.Bytes})
	col := batch.ColVec(0).Bytes()
	width := 64
	for i := int64(0); i < coldata.BatchSize; i++ {
		col[i] = randutil.RandBytes(rng, width)
	}

	// Set a known prefix and suffix on half the batch so we're not filtering
	// everything out.
	prefix := "abc"
	suffix := "xyz"
	for i := 0; i < coldata.BatchSize/2; i++ {
		copy(col[i][:3], prefix)
		copy(col[i][width-3:], suffix)
	}

	batch.SetLength(coldata.BatchSize)
	source := NewRepeatableBatchSource(batch)
	source.Init()

	prefixOp := &selPrefixBytesBytesConstOp{
		input:    source,
		colIdx:   0,
		constArg: []byte(prefix),
	}
	suffixOp := &selSuffixBytesBytesConstOp{
		input:    source,
		colIdx:   0,
		constArg: []byte(suffix),
	}
	pattern := fmt.Sprintf("^%s.*%s$", prefix, suffix)
	regexpOp := &selRegexpBytesBytesConstOp{
		input:    source,
		colIdx:   0,
		constArg: regexp.MustCompile(pattern),
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
			b.SetBytes(int64(width * coldata.BatchSize))
			for i := 0; i < b.N; i++ {
				tc.op.Next(ctx)
			}
		})
	}
}
