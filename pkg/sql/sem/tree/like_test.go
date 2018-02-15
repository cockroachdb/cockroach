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

package tree

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestSimilarEscape(t *testing.T) {
	testData := []struct {
		expr     string
		expected string
	}{
		{`test`, `test`},
		{`test%`, `test.*`},
		{`_test_`, `.test.`},
		{`_%*`, `..**`},
		{`[_%]*`, `[_%]*`},
		{`.^$`, `\.\^\$`},
		{`%(b|d)%`, `.*(?:b|d).*`},
		{`se\"arch\"[\"]`, `se(arch)[\"]`},
	}
	for _, d := range testData {
		s := SimilarEscape(d.expr)
		if s != d.expected {
			t.Errorf("%s: expected %s, but found %v", d.expr, d.expected, s)
		}
	}
}

var benchmarkLikePatterns = []string{
	`test%`,
	`%test%`,
	`%test`,
	``,
	`%`,
	`_`,
	`test`,
	`bad`,
	`also\%`,
}

func benchmarkLike(b *testing.B, ctx *EvalContext, caseInsensitive bool) {
	op := Like
	if caseInsensitive {
		op = ILike
	}
	likeFn, _ := CmpOps[op].lookupImpl(types.String, types.String)
	iter := func() {
		for _, p := range benchmarkLikePatterns {
			if _, err := likeFn.fn(ctx, NewDString("test"), NewDString(p)); err != nil {
				b.Fatalf("LIKE evaluation failed with error: %v", err)
			}
		}
	}
	// Warm up cache, if applicable
	iter()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		iter()
	}
}

func BenchmarkLikeWithCache(b *testing.B) {
	ctx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	ctx.ReCache = NewRegexpCache(len(benchmarkLikePatterns))
	defer ctx.Mon.Stop(context.Background())

	benchmarkLike(b, ctx, false)
}

func BenchmarkLikeWithoutCache(b *testing.B) {
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	benchmarkLike(b, evalCtx, false)
}

func BenchmarkILikeWithCache(b *testing.B) {
	ctx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	ctx.ReCache = NewRegexpCache(len(benchmarkLikePatterns))
	defer ctx.Mon.Stop(context.Background())

	benchmarkLike(b, ctx, true)
}

func BenchmarkILikeWithoutCache(b *testing.B) {
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	benchmarkLike(b, evalCtx, true)
}
