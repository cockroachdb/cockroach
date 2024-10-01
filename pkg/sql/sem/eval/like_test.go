// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestLike(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr      string
		pattern   string
		matches   tree.Datum
		erroneous bool
	}{
		{``, `{`, tree.DBoolFalse, false},
		{``, `%%%%%`, tree.DBoolTrue, false},
		{`a[b]`, `%[[_]`, tree.DBoolFalse, false},
		{`+`, `++`, tree.DBoolFalse, false},
		{`a`, `}`, tree.DBoolFalse, false},
		{`a{}%`, `%\}\%`, tree.DBoolTrue, false},
		{`a{}%a`, `%\}\%\`, tree.DBoolFalse, true},
		{`G\n%`, `%__%`, tree.DBoolTrue, false},
		{``, `\`, tree.DBoolFalse, false},
		{`_%\b\n`, `%__`, tree.DBoolTrue, false},
		{`_\nL_`, `%_%`, tree.DBoolTrue, false},
	}
	ctx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, d := range testData {
		if matches, err := matchLike(ctx, tree.NewDString(d.expr), tree.NewDString(d.pattern), false); err != nil && !d.erroneous {
			t.Error(err)
		} else if err == nil && d.erroneous {
			t.Errorf("%s matching the pattern %s: expected to return an error",
				d.expr,
				d.pattern,
			)
		} else if matches != d.matches {
			t.Errorf("%s matching the pattern %s: expected %v but found %v",
				d.expr,
				d.pattern,
				d.matches,
				matches,
			)
		}
	}
}

func TestLikeEscape(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		expr      string
		pattern   string
		escape    string
		matches   tree.Datum
		erroneous bool
	}{
		{``, `{`, string('\x7f'), tree.DBoolFalse, false},
		{``, `}`, `}`, tree.DBoolFalse, false},
		{``, `%%%%%`, ``, tree.DBoolTrue, false},
		{``, `%%%%%`, `\`, tree.DBoolTrue, false},
		{`a[b]`, `%[[_]`, `[`, tree.DBoolTrue, false},
		{`+`, `++`, `+`, tree.DBoolTrue, false},
		{`a`, `}`, `}`, tree.DBoolFalse, true},
		{`a{}%`, `%}}}%`, `}`, tree.DBoolTrue, false},
		{`BG_`, `%__`, `.`, tree.DBoolTrue, false},
		{`_%\b\n`, `%__`, ``, tree.DBoolTrue, false},
		{`_\nL_`, `%_%`, `{`, tree.DBoolTrue, false},
		{`_\nL_`, `%_%`, `%`, tree.DBoolFalse, true},
		{`\n\t`, `_%%_`, string('\x7f'), tree.DBoolTrue, false},
	}
	ctx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, d := range testData {
		if matches, err := MatchLikeEscape(ctx, d.expr, d.pattern, d.escape, false); err != nil && !d.erroneous {
			t.Error(err)
		} else if err == nil && d.erroneous {
			t.Errorf("%s matching the pattern %s with escape character %s: expected to return an error",
				d.expr,
				d.pattern,
				d.escape,
			)
		} else if matches != d.matches {
			t.Errorf("%s matching the pattern %s with escape character %s: expected %v but found %v",
				d.expr,
				d.pattern,
				d.escape,
				d.matches,
				matches,
			)
		}
	}
}

func TestSimilarEscape(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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

func benchmarkLike(b *testing.B, evalCtx *Context, caseInsensitive bool) {
	op := treecmp.Like
	if caseInsensitive {
		op = treecmp.ILike
	}
	likeFn, _ := tree.CmpOps[op].LookupImpl(types.String, types.String)
	iter := func() {
		for _, p := range benchmarkLikePatterns {
			if _, err := BinaryOp(
				context.Background(), evalCtx, likeFn.EvalOp, tree.NewDString("test"), tree.NewDString(p),
			); err != nil {
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
	ctx.ReCache = tree.NewRegexpCache(len(benchmarkLikePatterns))
	defer ctx.Stop(context.Background())

	benchmarkLike(b, ctx, false)
}

func BenchmarkLikeWithoutCache(b *testing.B) {
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	benchmarkLike(b, evalCtx, false)
}

func BenchmarkILikeWithCache(b *testing.B) {
	ctx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	ctx.ReCache = tree.NewRegexpCache(len(benchmarkLikePatterns))
	defer ctx.Stop(context.Background())

	benchmarkLike(b, ctx, true)
}

func BenchmarkILikeWithoutCache(b *testing.B) {
	evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	benchmarkLike(b, evalCtx, true)
}
