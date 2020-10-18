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
			// Indexed column must be first with @>.
			filters:      "json1 @> json2",
			indexOrd:     jsonOrd,
			invertedExpr: "",
		},
		{
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
			// Indexed column must be first with @>.
			filters:      "array1 @> array2",
			indexOrd:     arrayOrd,
			invertedExpr: "",
		},
		{
			// Indexed column must be second with <@.
			filters:      "array2 <@ array1",
			indexOrd:     arrayOrd,
			invertedExpr: "",
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
