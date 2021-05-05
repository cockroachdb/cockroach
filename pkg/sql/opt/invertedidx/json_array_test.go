// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package invertedidx_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestTryJoinJsonOrArrayIndex(t *testing.T) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.NewTestingEvalContext(nil /* st */)

	tc := testcat.New()

	// Create the input table.
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t1 (json1 JSON, array1 INT[], json11 JSONB, array11 INT[], " +
			"inet1 INET)",
	); err != nil {
		t.Fatal(err)
	}

	// Create the indexed table.
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t2 (json2 JSON, array2 INT[], inet2 INET, " +
			"INVERTED INDEX (json2), INVERTED INDEX (array2))",
	); err != nil {
		t.Fatal(err)
	}

	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn1 := tree.NewUnqualifiedTableName("t1")
	tn2 := tree.NewUnqualifiedTableName("t2")
	tab1 := md.AddTable(tc.Table(tn1), tn1)
	tab2 := md.AddTable(tc.Table(tn2), tn2)
	jsonOrd, arrayOrd := 1, 2

	testCases := []struct {
		filters      string
		indexOrd     int
		invertedExpr string
	}{
		{
			// Indexed column can be on either side of @>.
			filters:      "json1 @> json2",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 <@ json1",
		},
		{
			// Indexed column can be on either side of <@.
			filters:      "json1 <@ json2",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1",
		},
		{
			filters:      "json2 @> json1",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1",
		},
		{
			// Indexed column can be on either side of @>.
			filters:      "array1 @> array2",
			indexOrd:     arrayOrd,
			invertedExpr: "array2 <@ array1",
		},
		{
			// Indexed column can be on either side of <@.
			filters:      "array2 <@ array1",
			indexOrd:     arrayOrd,
			invertedExpr: "array2 <@ array1",
		},
		{
			filters:      "array2 @> array1",
			indexOrd:     arrayOrd,
			invertedExpr: "array2 @> array1",
		},
		{
			// Wrong index ordinal.
			filters:      "json2 @> json1",
			indexOrd:     arrayOrd,
			invertedExpr: "",
		},
		{
			// We can perform a join using two comparison operations on the same
			// indexed column, even if the input columns are different.
			filters:      "json2 @> json1 AND json2 @> json11",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1 AND json2 @> json11",
		},
		{
			// We can perform a join using two comparison operations on the same
			// indexed column, even if the input columns are different.
			filters:      "array2 @> array1 AND array11 <@ array2",
			indexOrd:     arrayOrd,
			invertedExpr: "array2 @> array1 AND array2 @> array11",
		},
		{
			// We can perform a join using two comparison operations on the same
			// indexed column, even if the input columns are different.
			filters:      "json2 @> json1 OR json2 @> json11",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1 OR json2 @> json11",
		},
		{
			// When operations affecting two different variables are OR-ed, we
			// cannot perform an inverted join.
			filters:      "json2 @> json1 OR array2 @> array1",
			indexOrd:     jsonOrd,
			invertedExpr: "",
		},
		{
			// We can constrain either index when the operations are AND-ed.
			filters:      "json2 @> json1 AND array2 @> array1",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1",
		},
		{
			// We can constrain either index when the operations are AND-ed.
			filters:      "json2 @> json1 AND array2 @> array1",
			indexOrd:     arrayOrd,
			invertedExpr: "array2 @> array1",
		},
		{
			// Join conditions can be combined with index constraints.
			filters:      "json2 @> json1 AND json2 @> '{\"a\": \"b\"}'::json",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1 AND json2 @> '{\"a\": \"b\"}'::json",
		},
		{
			// Join conditions can be combined with index constraints.
			filters: "json2 @> json1 AND json2 @> '{\"a\": \"b\"}'::json AND " +
				"json1 @> '{\"a\": \"b\"}'::json",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1 AND json2 @> '{\"a\": \"b\"}'::json",
		},
		{
			// At least one column from the input is required.
			filters:      "json2 @> '{\"a\": \"b\"}'::json",
			indexOrd:     jsonOrd,
			invertedExpr: "",
		},
		{
			// AND with a non-json function.
			filters:      "json2 @> json1 AND inet_same_family(inet1, inet2)",
			indexOrd:     jsonOrd,
			invertedExpr: "json2 @> json1",
		},
		{
			// OR with a non-json function.
			filters:      "json2 @> json1 OR inet_same_family(inet1, inet2)",
			indexOrd:     jsonOrd,
			invertedExpr: "",
		},
		{
			// Arbitrarily complex join condition.
			filters: "array2 @> array1 OR (" +
				"array2 @> array11 AND json2 @> json1 AND " +
				"array2 @> '{1,2}'::int[]) AND " +
				"json2 @> json1 AND " +
				"array2 @> '{3}'::int[]",
			indexOrd: arrayOrd,
			invertedExpr: "array2 @> array1 OR (" +
				"array2 @> array11 AND " +
				"array2 @> '{1,2}'::int[]) AND " +
				"array2 @> '{3}'::int[]",
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %v", tc)
		filters := testutils.BuildFilters(t, &f, &semaCtx, evalCtx, tc.filters)

		var inputCols opt.ColSet
		for i, n := 0, md.Table(tab1).ColumnCount(); i < n; i++ {
			inputCols.Add(tab1.ColumnID(i))
		}

		actInvertedExpr := invertedidx.TryJoinInvertedIndex(
			evalCtx.Context, &f, filters, tab2, md.Table(tab2).Index(tc.indexOrd), inputCols,
		)

		if actInvertedExpr == nil {
			if tc.invertedExpr != "" {
				t.Fatalf("expected %s, got <nil>", tc.invertedExpr)
			}
			continue
		}

		if tc.invertedExpr == "" {
			t.Fatalf("expected <nil>, got %v", actInvertedExpr)
		}

		expInvertedExpr := testutils.BuildScalar(t, &f, &semaCtx, evalCtx, tc.invertedExpr)
		if actInvertedExpr.String() != expInvertedExpr.String() {
			t.Errorf("expected %v, got %v", expInvertedExpr, actInvertedExpr)
		}
	}
}

func TestTryFilterJsonOrArrayIndex(t *testing.T) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.NewTestingEvalContext(nil /* st */)

	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (j JSON, a INT[], INVERTED INDEX (j), INVERTED INDEX (a))",
	); err != nil {
		t.Fatal(err)
	}
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)
	jsonOrd, arrayOrd := 1, 2

	testCases := []struct {
		filters          string
		indexOrd         int
		ok               bool
		tight            bool
		unique           bool
		remainingFilters string
	}{
		// If we can create an inverted filter with the given filter expression and
		// index, ok=true. If the spans in the resulting inverted index constraint
		// do not have duplicate primary keys, unique=true. If the spans are tight,
		// tight=true and remainingFilters="". Otherwise, tight is false and
		// remainingFilters contains some or all of the original filters.
		{
			filters:  "j @> '1'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			// Contained by is supported for json.
			filters:          "j <@ '1'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j <@ '1'",
		},
		{
			filters:          `j <@ '{"a": 1}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: `j <@ '{"a": 1}'`,
		},
		{
			filters:  "a @> '{1}'",
			indexOrd: arrayOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			// Contained by is supported for arrays.
			filters:          "a <@ '{1}'",
			indexOrd:         arrayOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "a <@ '{1}'",
		},
		{
			filters:          "a <@ '{}'",
			indexOrd:         arrayOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "a <@ '{}'",
		},
		{
			// Wrong index ordinal.
			filters:  "a @> '{1}'",
			indexOrd: jsonOrd,
			ok:       false,
		},
		{
			// Wrong index ordinal.
			filters:  "j @> '1'",
			indexOrd: arrayOrd,
			ok:       false,
		},
		{
			// When operations affecting two different variables are OR-ed, we cannot
			// constrain either index.
			filters:  "j @> '1' OR a @> '{1}'",
			indexOrd: jsonOrd,
			ok:       false,
		},
		{
			// When operations affecting two different variables are OR-ed, we cannot
			// constrain either index.
			filters:  "j <@ '1' OR a <@ '{1}'",
			indexOrd: jsonOrd,
			ok:       false,
		},
		{
			// When operations affecting two different variables are OR-ed, we cannot
			// constrain either index.
			filters:  "j <@ '1' OR a <@ '{1}'",
			indexOrd: arrayOrd,
			ok:       false,
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters:          "j @> '1' AND a @> '{1}'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "a @> '{1}'",
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters:          "j @> '1' AND a @> '{1}'",
			indexOrd:         arrayOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j @> '1'",
		},
		{
			// We can constrain the array index when the functions are AND-ed.
			filters:          "j <@ '1' AND a <@ '{1}'",
			indexOrd:         arrayOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j <@ '1' AND a <@ '{1}'",
		},
		{
			// We can constrain the JSON index when the functions are AND-ed.
			filters:          "j <@ '1' AND a <@ '{1}'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j <@ '1' AND a <@ '{1}'",
		},
		{
			// We can guarantee unique primary keys when there are multiple paths
			// that are each unique.
			filters:  "j @> '[1, 2]'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			// We can guarantee unique primary keys when there are multiple paths
			// that are each unique.
			filters:  "a @> '{1, 2}'",
			indexOrd: arrayOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			// We cannot guarantee that the span expression is tight when there is a
			// nested array. This is because '[[1, 2]]' has the same keys as
			// '[[1], [2]]', but '[[1], [2]]' @> '[[1, 2]]' is false.
			filters:          "j @> '[[1, 2]]'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j @> '[[1, 2]]'",
		},
		{
			// A more complex expression.
			filters:          "j @> '2' AND (j @> '1' OR a @> '{1}')",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j @> '1' OR a @> '{1}'",
		},
		{
			// If the left child of an OR condition is not tight, the remaining
			// filters are the entire condition.
			filters:          "j @> '[[1, 2]]' OR j @> '[3, 4]'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j @> '[[1, 2]]' OR j @> '[3, 4]'",
		},
		{
			// If the right child of an OR condition is not tight, the remaining
			// filters are the entire condition.
			filters:          "j @> '[1, 2]' OR j @> '[[3, 4]]'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j @> '[1, 2]' OR j @> '[[3, 4]]'",
		},
		{
			// If both expressions in an OR condition are tight, there are no
			// remaining filters.
			filters:  "j @> '[1, 2]' OR j @> '[3, 4]'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   false,
		},
		{
			// With AND conditions the remaining filters may be a subset of the
			// original condition.
			filters:          "j @> '{\"a\": [1, 2]}' AND (j @> '[1, 2]' AND j @> '[[3, 4]]')",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j @> '{\"a\": [1, 2]}' AND j @> '[[3, 4]]'",
		},
		{
			// With AND conditions the remaining filters may be a subset of the
			// original condition.
			filters:          "j @> '\"a\"' AND (j @> '[[1, 2]]' AND j @> '[3, 4]')",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j @> '[[1, 2]]'",
		},
		{
			filters:  "j->'a' = '1'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			// Integer indexes are not yet supported.
			filters:  "j->0 = '1'",
			indexOrd: jsonOrd,
			ok:       false,
		},
		{
			// Arrays on the right side of the equality are supported.
			filters:          "j->'a' = '[1]'",
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j->'a' = '[1]'",
		},
		{
			// Objects on the right side of the equality are supported.
			filters:          `j->'a' = '{"b": "c"}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: `j->'a' = '{"b": "c"}'`,
		},
		{
			// Wrong index ordinal.
			filters:  "j->'a' = '1'",
			indexOrd: arrayOrd,
			ok:       false,
		},
		{
			filters:  "j->'a'->'b' = '1'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			filters:  "j->'a'->'b'->'c' = '1'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			// Integer indexes are not yet supported.
			filters:  "j->0->'b' = '1'",
			indexOrd: jsonOrd,
			ok:       false,
		},
		{
			// The inner most expression is not a fetch val expression with an
			// indexed column on the left.
			filters:  "(j-'c')->'a'->'b' = '1'",
			indexOrd: jsonOrd,
			ok:       false,
		},
		{
			filters:  "j->'a' = '1' AND j->'b' = '2'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			filters:  "j->'a' = '1' OR j->'b' = '2'",
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   false,
		},
		{
			filters:  `j->'a' = '1' AND j @> '{"b": "c"}'`,
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   true,
		},
		{
			filters:  `j->'a' = '1' OR j @> '{"b": "c"}'`,
			indexOrd: jsonOrd,
			ok:       true,
			tight:    true,
			unique:   false,
		},
		{
			filters:          `j->'a' = '1' AND j @> '[[1, 2]]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j @> '[[1, 2]]'",
		},
		{
			// Contains is supported with a fetch val operator on the left.
			filters:          `j->'a' @> '1'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            true,
			unique:           false,
			remainingFilters: "",
		},
		{
			// Contains is supported with chained fetch val operators on the left.
			filters:          `j->'a'->'b' @> '1'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            true,
			unique:           false,
			remainingFilters: "",
		},
		{
			// Contains with a fetch val is supported for JSON arrays.
			filters:          `j->'a'->'b' @> '[1, 2]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j->'a'->'b' @> '[1, 2]'",
		},
		{
			filters:          `j->'a'->'b' @> '[[1, 2]]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j->'a'->'b' @> '[[1, 2]]'",
		},
		{
			// Contains with a fetch val is supported for JSON objects.
			filters:          `j->'a'->'b' @> '{"c": 1}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            true,
			unique:           true,
			remainingFilters: "",
		},
		{
			filters:          `j->'a'->'b' @> '{"c": {"d": "e"}}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            true,
			unique:           true,
			remainingFilters: "",
		},
		{
			filters:          `j->'a'->'b' @> '[{"c": 1, "d": "2"}]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j->'a'->'b' @> '[{\"c\": 1, \"d\": \"2\"}]'",
		},
		{
			filters:          `j->'a'->'b' @> '{"c": [1, 2], "d": "2"}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           true,
			remainingFilters: "j->'a'->'b' @> '{\"c\": [1, 2], \"d\": \"2\"}'",
		},
		{
			// ContainedBy is supported with a fetch val operator on the left.
			filters:          `j->'a' <@ '1'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a' <@ '1'",
		},
		{
			// ContainedBy is supported with chained fetch val operators on the left.
			filters:          `j->'a'->'b' <@ '1'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '1'",
		},
		{
			// ContainedBy with a fetch val is supported for JSON arrays.
			filters:          `j->'a'->'b' <@ '[1, 2]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '[1, 2]'",
		},
		{
			filters:          `j->'a'->'b' <@ '[[1, 2]]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '[[1, 2]]'",
		},
		{
			// ContainedBy with a fetch val is supported for JSON objects.
			filters:          `j->'a'->'b' <@ '{"c": 1}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '{\"c\": 1}'",
		},
		{
			filters:          `j->'a'->'b' <@ '{"c": {"d": "e"}}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '{\"c\": {\"d\": \"e\"}}'",
		},
		{
			filters:          `j->'a'->'b' <@ '[{"c": 1, "d": "2"}]'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '[{\"c\": 1, \"d\": \"2\"}]'",
		},
		{
			filters:          `j->'a'->'b' <@ '{"c": [1, 2], "d": "2"}'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "j->'a'->'b' <@ '{\"c\": [1, 2], \"d\": \"2\"}'",
		},
		{
			// Contains is supported with a fetch val operator on the right.
			filters:          `'1' @> j->'a'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            false,
			unique:           false,
			remainingFilters: "'1' @> j->'a'",
		},
		{
			// ContainedBy is supported with a fetch val operator on the right.
			filters:          `'1' <@ j->'a'`,
			indexOrd:         jsonOrd,
			ok:               true,
			tight:            true,
			unique:           false,
			remainingFilters: "",
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %v", tc)
		filters := testutils.BuildFilters(t, &f, &semaCtx, evalCtx, tc.filters)

		// We're not testing that the correct SpanExpression is returned here;
		// that is tested elsewhere. This is just testing that we are constraining
		// the index when we expect to and we have the correct values for tight,
		// unique, and remainingFilters.
		spanExpr, _, remainingFilters, _, ok := invertedidx.TryFilterInvertedIndex(
			evalCtx,
			&f,
			filters,
			nil, /* optionalFilters */
			tab,
			md.Table(tab).Index(tc.indexOrd),
			nil, /* computedColumns */
		)
		if tc.ok != ok {
			t.Fatalf("expected %v, got %v", tc.ok, ok)
		}
		if !ok {
			continue
		}

		if tc.tight != spanExpr.Tight {
			t.Fatalf("expected tight=%v, but got %v", tc.tight, spanExpr.Tight)
		}
		if tc.unique != spanExpr.Unique {
			t.Fatalf("expected unique=%v, but got %v", tc.unique, spanExpr.Unique)
		}

		if remainingFilters == nil {
			if tc.remainingFilters != "" {
				t.Fatalf("expected remainingFilters=%s, got <nil>", tc.remainingFilters)
			}
			continue
		}
		if tc.remainingFilters == "" {
			t.Fatalf("expected remainingFilters=<nil>, got %v", remainingFilters)
		}
		expRemainingFilters := testutils.BuildFilters(t, &f, &semaCtx, evalCtx, tc.remainingFilters)
		if remainingFilters.String() != expRemainingFilters.String() {
			t.Errorf("expected remainingFilters=%v, got %v", expRemainingFilters, remainingFilters)
		}
	}
}
