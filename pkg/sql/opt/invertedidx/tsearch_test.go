// Copyright 2023 The Cockroach Authors.
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

func TestTryFilterTSVector(t *testing.T) {
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.NewTestingEvalContext(st)

	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (t tsvector, INVERTED INDEX (t))",
	); err != nil {
		t.Fatal(err)
	}
	var f norm.Factory
	f.Init(context.Background(), evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)
	tsvectorOrd := 1

	// If we can create an inverted filter with the given filter expression and
	// index, ok=true. If the spans in the resulting inverted index constraint
	// do not have duplicate primary keys, unique=true. If the spans are tight,
	// tight=true and remainingFilters="". Otherwise, tight is false and
	// remainingFilters contains some or all of the original filters.
	testCases := []struct {
		filters string
		ok      bool
		tight   bool
		unique  bool
	}{
		{filters: "t @@ 'a'", ok: true, tight: true, unique: true},
		{filters: "t @@ '!a'", ok: false, tight: false, unique: false},
		// Prefix match.
		{filters: "t @@ 'a:*'", ok: true, tight: true, unique: false},
		// Weight match.
		{filters: "t @@ 'a:C'", ok: true, tight: false, unique: true},
		// Weight and prefix match.
		{filters: "t @@ 'a:C*'", ok: true, tight: false, unique: false},

		{filters: "t @@ 'a | b'", ok: true, tight: true, unique: false},
		{filters: "t @@ 'a & b'", ok: true, tight: true, unique: true},
		{filters: "t @@ 'a <-> b'", ok: true, tight: false, unique: true},

		// Can't filter with ! in an or clause.
		{filters: "t @@ '!a | b'", ok: false, tight: false, unique: false},
		{filters: "t @@ 'a | !b'", ok: false, tight: false, unique: false},
		{filters: "t @@ '!a | !b'", ok: false, tight: false, unique: false},

		// ! in an and clause is okay - we just re-filter on the ! term.
		{filters: "t @@ 'a & !b'", ok: true, tight: false, unique: true},
		{filters: "t @@ '!a & b'", ok: true, tight: false, unique: true},
		// But not if both are !.
		{filters: "t @@ '!a & !b'", ok: false, tight: false, unique: false},

		// Same as above, except <-> matches are never tight - they always require
		// re-checking because the index doesn't store the lexeme positions.
		{filters: "t @@ 'a <-> !b'", ok: true, tight: false, unique: true},
		{filters: "t @@ '!a <-> b'", ok: true, tight: false, unique: true},
		{filters: "t @@ '!a <-> !b'", ok: false, tight: false, unique: false},

		// Some sanity checks for more than 2 terms, to make sure that the output
		// de-uniqueifies as we travel up the tree with more than 1 lexeme seen.
		{filters: "t @@ '(a & !b) | c'", ok: true, tight: false, unique: false},
		{filters: "t @@ '(a & b) | c'", ok: true, tight: true, unique: true},
		{filters: "t @@ '(a & b) <-> !(c | d)'", ok: true, tight: false, unique: true},
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
			md.Table(tab).Index(tsvectorOrd),
			nil,       /* computedColumns */
			func() {}, /* checkCancellation */
		)
		if tc.ok != ok {
			t.Fatalf("expected %v, got %v", tc.ok, ok)
		}
		if !ok {
			continue
		}

		if tc.tight != spanExpr.Tight {
			t.Fatalf("For (%s), expected tight=%v, but got %v", tc.filters, tc.tight, spanExpr.Tight)
		}
		if tc.unique != spanExpr.Unique {
			t.Fatalf("For (%s), expected unique=%v, but got %v", tc.filters, tc.unique, spanExpr.Unique)
		}

		if tc.tight {
			require.True(t, remainingFilters.IsTrue())
		} else {
			require.Equal(t, filters.String(), remainingFilters.String(),
				"mismatched remaining filters")
		}
	}
}
