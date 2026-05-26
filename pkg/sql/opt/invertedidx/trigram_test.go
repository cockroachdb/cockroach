// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package invertedidx_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestTryFilterTrigram(t *testing.T) {
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)
	evalCtx.SessionData().TrigramSimilarityThreshold = 0.3

	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (s STRING, INVERTED INDEX (s gin_trgm_ops))",
	); err != nil {
		t.Fatal(err)
	}
	var f norm.Factory
	f.Init(context.Background(), evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)
	trigramOrd := 1

	// If we can create an inverted filter with the given filter expression and
	// index, ok=true. If the spans in the resulting inverted index constraint
	// do not have duplicate primary keys, unique=true. If the spans are tight,
	// tight=true and remainingFilters="". Otherwise, tight is false and
	// remainingFilters contains some or all of the original filters.
	testCases := []struct {
		filters string
		ok      bool
		unique  bool
	}{
		// Test LIKE with percents on both sides.
		// TODO(jordan): we could make expressions with just a single trigram
		// tight, because we would know for sure that we wouldn't need to recheck
		// the condition once the row is returned. But, it's probably not that
		// important of an optimization.
		{filters: "s LIKE '%foo%'", ok: true, unique: true},
		{filters: "s LIKE '%blorp%'", ok: true, unique: false},
		{filters: "s LIKE 'foo%'", ok: true, unique: true},
		{filters: "s LIKE 'blorp%'", ok: true, unique: false},
		{filters: "s ILIKE '%foo%'", ok: true, unique: true},
		{filters: "s ILIKE '%blorp%'", ok: true, unique: false},
		// Queries that are too short to have any trigrams do not produce filters.
		{filters: "s LIKE '%fo%'", ok: false},
		{filters: "s ILIKE '%fo%'", ok: false},
		{filters: "s LIKE '%fo%ab%ru%'", ok: false},

		// AND and OR for two LIKE queries behave as expected.
		{filters: "s LIKE '%lkj%' AND s LIKE '%bla%'", ok: true, unique: true},
		{filters: "s LIKE '%lkj%' OR s LIKE '%bla%'", ok: true, unique: false},

		// LIKE with variables on the right-hand side.
		{filters: "'abc' LIKE s", ok: false},
		{filters: "'abc' ILIKE s", ok: false},
		{filters: "'abc%' LIKE s", ok: false},
		{filters: "'%abc' LIKE s", ok: false},

		// Similarity queries.
		{filters: "s % 'lkjsdlkj'", ok: true, unique: false},
		{filters: "s % 'lkj'", ok: true, unique: false},
		{filters: "s % 'lj'", ok: true, unique: false},

		// AND and OR for two similarity queries behave as expected.
		{filters: "s % 'lkj' AND s % 'bla'", ok: true, unique: false},
		{filters: "s % 'lkj' OR s % 'bla'", ok: true, unique: false},

		// Can combine similarity and LIKE queries and still get inverted
		// expressions.
		{filters: "s % 'lkj' AND s LIKE 'blort'", ok: true, unique: false},
		{filters: "s % 'lkj' OR s LIKE 'blort'", ok: true, unique: false},

		// Equality queries.
		{filters: "s = 'lkjsdlkj'", ok: true, unique: false},
		{filters: "s = 'lkj'", ok: true, unique: true},
		{filters: "s = 'lkj' OR s LIKE 'blah'", ok: true, unique: true},
	}

	for _, tc := range testCases {
		t.Logf("test case: %v", tc)
		filters := testutils.BuildFilters(t, &f, &semaCtx, evalCtx, tc.filters)

		// We're not testing that the correct SpanExpression is returned here;
		// that is tested elsewhere. This is just testing that we are constraining
		// the index when we expect to and we have the correct values for tight,
		// unique, and remainingFilters.
		spanExpr, _, remainingFilters, _, ok := invertedidx.TryFilterInvertedIndex(
			context.Background(),
			evalCtx,
			&f,
			filters,
			nil, /* optionalFilters */
			tab,
			md.Table(tab).Index(trigramOrd),
			nil,       /* computedColumns */
			func() {}, /* checkCancellation */
		)
		if tc.ok != ok {
			t.Fatalf("expected %v, got %v", tc.ok, ok)
		}
		if !ok {
			continue
		}

		if spanExpr.Tight {
			t.Fatalf("We never expected our inverted expression to be tight")
		}
		if tc.unique != spanExpr.Unique {
			t.Fatalf("For (%s), expected unique=%v, but got %v", tc.filters, tc.unique, spanExpr.Unique)
		}

		require.Equal(t, filters.String(), remainingFilters.String(),
			"mismatched remaining filters")
	}
}
