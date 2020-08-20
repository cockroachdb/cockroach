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
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func TestTryJoinGeoIndex(t *testing.T) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.NewTestingEvalContext(nil /* st */)

	tc := testcat.New()

	// Create the input table.
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t1 (geom1 GEOMETRY, geog1 GEOGRAPHY, geom11 GEOMETRY, geog11 GEOGRAPHY, " +
			"inet1 INET, bbox1 box2d)",
	); err != nil {
		t.Fatal(err)
	}

	// Create the indexed table.
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t2 (geom2 GEOMETRY, geog2 GEOGRAPHY, inet2 INET, bbox2 box2d, " +
			"INVERTED INDEX (geom2), INVERTED INDEX (geog2))",
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
	geomOrd, geogOrd := 1, 2

	testCases := []struct {
		filters      string
		indexOrd     int
		invertedExpr string
	}{
		{
			filters:      "st_covers(geom1, geom2)",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(geom1, geom2)",
		},
		{
			filters:      "st_covers(geom2, geom1)",
			indexOrd:     geomOrd,
			invertedExpr: "st_coveredby(geom1, geom2)",
		},
		{
			filters:      "st_coveredby(geog1, geog2)",
			indexOrd:     geogOrd,
			invertedExpr: "st_coveredby(geog1, geog2)",
		},
		{
			filters:      "st_coveredby(geog2, geog1)",
			indexOrd:     geogOrd,
			invertedExpr: "st_covers(geog1, geog2)",
		},
		{
			filters:      "st_containsproperly(geom2, geom1)",
			indexOrd:     geomOrd,
			invertedExpr: "st_coveredby(geom1, geom2)",
		},
		{
			filters:      "st_dwithin(geog2, geog1, 1)",
			indexOrd:     geogOrd,
			invertedExpr: "st_dwithin(geog1, geog2, 1)",
		},
		{
			filters:      "st_dfullywithin(geom2, geom1, 1)",
			indexOrd:     geomOrd,
			invertedExpr: "st_dfullywithin(geom1, geom2, 1)",
		},
		{
			filters:      "st_intersects(geom1, geom2)",
			indexOrd:     geomOrd,
			invertedExpr: "st_intersects(geom1, geom2)",
		},
		{
			filters:      "st_overlaps(geom2, geom1)",
			indexOrd:     geomOrd,
			invertedExpr: "st_intersects(geom1, geom2)",
		},
		{
			// Wrong index ordinal.
			filters:      "st_covers(geom1, geom2)",
			indexOrd:     geogOrd,
			invertedExpr: "",
		},
		{
			// We can perform a join using two geospatial functions on the same
			// indexed column, even if the input columns are different.
			filters:      "st_covers(geom1, geom2) AND st_covers(geom2, geom11)",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(geom1, geom2) AND st_coveredby(geom11, geom2)",
		},
		{
			// We can perform a join using two geospatial functions on the same
			// indexed column, even if the input columns are different.
			filters:      "st_covers(geog2, geog1) AND st_dwithin(geog11, geog2, 10)",
			indexOrd:     geogOrd,
			invertedExpr: "st_coveredby(geog1, geog2) AND st_dwithin(geog11, geog2, 10)",
		},
		{
			// We can perform a join using two geospatial functions on the same
			// indexed column, even if the input columns are different.
			filters:      "st_covers(geom1, geom2) OR st_covers(geom2, geom11)",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(geom1, geom2) OR st_coveredby(geom11, geom2)",
		},
		{
			// When functions affecting two different geospatial variables are OR-ed,
			// we cannot perform an inverted join.
			filters:      "st_covers(geom1, geom2) OR st_covers(geog1, geog2)",
			indexOrd:     geomOrd,
			invertedExpr: "",
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters:      "st_covers(geom1, geom2) AND st_covers(geog1, geog2)",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(geom1, geom2)",
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters:      "st_covers(geom1, geom2) AND st_covers(geog1, geog2)",
			indexOrd:     geogOrd,
			invertedExpr: "st_covers(geog1, geog2)",
		},
		{
			// Join conditions can be combined with index constraints.
			filters: "st_covers(geom1, geom2) AND " +
				"st_covers(geom2, 'LINESTRING ( 0 0, 0 2 )'::geometry)",
			indexOrd: geomOrd,
			invertedExpr: "st_covers(geom1, geom2) AND " +
				"st_coveredby('LINESTRING ( 0 0, 0 2 )'::geometry, geom2)",
		},
		{
			// Join conditions can be combined with index constraints.
			filters: "st_covers(geom1, geom2) AND " +
				"st_covers('LINESTRING ( 0 0, 0 2 )'::geometry, geom2) AND " +
				"st_covers('LINESTRING ( 0 0, 0 2 )'::geometry, geom1)",
			indexOrd: geomOrd,
			invertedExpr: "st_covers(geom1, geom2) AND " +
				"st_covers('LINESTRING ( 0 0, 0 2 )'::geometry, geom2)",
		},
		{
			// At least one column from the input is required.
			filters:      "st_covers(geom2, 'LINESTRING ( 0 0, 0 2 )'::geometry)",
			indexOrd:     geomOrd,
			invertedExpr: "",
		},
		{
			// AND with a non-geospatial function.
			filters:      "st_covers(geom1, geom2) AND inet_same_family(inet1, inet2)",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(geom1, geom2)",
		},
		{
			// OR with a non-geospatial function.
			filters:      "st_covers(geom1, geom2) OR inet_same_family(inet1, inet2)",
			indexOrd:     geomOrd,
			invertedExpr: "",
		},
		{
			// Arbitrarily complex join condition.
			filters: "st_covers(geog2, geog1) OR (" +
				"st_dwithin(geog11, geog2, 100) AND st_covers(geom1, geom2) AND " +
				"st_covers(geog2, 'SRID=4326;POINT(-40.23456 70.456772)'::geography)) AND " +
				"st_overlaps(geom2, geom1) AND " +
				"st_covers('SRID=4326;POINT(-42.89456 75.938299)'::geography, geog2)",
			indexOrd: geogOrd,
			invertedExpr: "st_coveredby(geog1, geog2) OR (" +
				"st_dwithin(geog11, geog2, 100) AND " +
				"st_coveredby('SRID=4326;POINT(-40.23456 70.456772)'::geography, geog2)) AND " +
				"st_covers('SRID=4326;POINT(-42.89456 75.938299)'::geography, geog2)",
		},

		// Bounding box operators.
		{
			filters:      "bbox1 ~ geom2",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(bbox1::geometry, geom2)",
		},
		{
			filters:      "geom2 ~ bbox1",
			indexOrd:     geomOrd,
			invertedExpr: "st_coveredby(bbox1::geometry, geom2)",
		},
		{
			filters:      "geom1 ~ geom2",
			indexOrd:     geomOrd,
			invertedExpr: "st_covers(geom1, geom2)",
		},
		{
			filters:      "geom2 ~ geom1",
			indexOrd:     geomOrd,
			invertedExpr: "st_coveredby(geom1, geom2)",
		},
		{
			filters:      "bbox1 && geom2",
			indexOrd:     geomOrd,
			invertedExpr: "st_intersects(bbox1::geometry, geom2)",
		},
		{
			filters:      "geom2 && bbox1",
			indexOrd:     geomOrd,
			invertedExpr: "st_intersects(bbox1::geometry, geom2)",
		},
		{
			filters:      "geom1 && geom2",
			indexOrd:     geomOrd,
			invertedExpr: "st_intersects(geom1, geom2)",
		},
		{
			filters:      "geom2 && geom1",
			indexOrd:     geomOrd,
			invertedExpr: "st_intersects(geom1, geom2)",
		},
		{
			filters:  "geom2 && geom1 AND 'BOX(1 2, 3 4)'::box2d ~ geom2",
			indexOrd: geomOrd,
			invertedExpr: "st_intersects(geom1, geom2) AND " +
				"st_covers('BOX(1 2, 3 4)'::box2d::geometry, geom2)",
		},
		{
			// Wrong index ordinal.
			filters:      "bbox1 ~ geom2",
			indexOrd:     geogOrd,
			invertedExpr: "",
		},
		{
			// At least one column from the input is required.
			filters:      "bbox2 ~ geom2",
			indexOrd:     geomOrd,
			invertedExpr: "",
		},
		{
			// At least one column from the input is required.
			filters:      "'BOX(1 2, 3 4)'::box2d ~ geom2",
			indexOrd:     geomOrd,
			invertedExpr: "",
		},
		{
			// Wrong types.
			filters:      "geom1::string ~ geom2::string",
			indexOrd:     geomOrd,
			invertedExpr: "",
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %v", tc)
		filters, err := buildFilters(tc.filters, &semaCtx, evalCtx, &f)
		if err != nil {
			t.Fatal(err)
		}

		var inputCols opt.ColSet
		for i, n := 0, md.Table(tab1).ColumnCount(); i < n; i++ {
			inputCols.Add(tab1.ColumnID(i))
		}

		actInvertedExpr := invertedidx.TryJoinGeoIndex(
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

		expInvertedExpr, err := buildScalar(tc.invertedExpr, &semaCtx, evalCtx, &f)
		if err != nil {
			t.Fatal(err)
		}

		if actInvertedExpr.String() != expInvertedExpr.String() {
			t.Errorf("expected %v, got %v", expInvertedExpr, actInvertedExpr)
		}
	}
}

func TestTryConstrainGeoIndex(t *testing.T) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.NewTestingEvalContext(nil /* st */)

	tc := testcat.New()
	if _, err := tc.ExecuteDDL(
		"CREATE TABLE t (geom GEOMETRY, geog GEOGRAPHY, INVERTED INDEX (geom), INVERTED INDEX (geog))",
	); err != nil {
		t.Fatal(err)
	}
	var f norm.Factory
	f.Init(evalCtx, tc)
	md := f.Metadata()
	tn := tree.NewUnqualifiedTableName("t")
	tab := md.AddTable(tc.Table(tn), tn)
	geomOrd, geogOrd := 1, 2

	testCases := []struct {
		filters  string
		indexOrd int
		ok       bool
	}{
		{
			filters:  "st_intersects('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			// Still works with arguments commuted.
			filters:  "st_intersects(geom, 'LINESTRING ( 0 0, 0 2 )'::geometry)",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "st_covers('SRID=4326;POINT(-40.23456 70.4567772)'::geography, geog)",
			indexOrd: geogOrd,
			ok:       true,
		},
		{
			// Still works with arguments commuted.
			filters:  "st_covers(geog, 'SRID=4326;POINT(-40.23456 70.4567772)'::geography)",
			indexOrd: geogOrd,
			ok:       true,
		},
		{
			// Wrong index ordinal.
			filters:  "st_covers('SRID=4326;POINT(-40.23456 70.4567772)'::geography, geog)",
			indexOrd: geomOrd,
			ok:       false,
		},
		{
			// Wrong index ordinal.
			filters:  "st_covers('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			indexOrd: geogOrd,
			ok:       false,
		},
		{
			// When functions affecting two different geospatial variables are OR-ed,
			// we cannot constrain either index.
			filters: "st_equals('LINESTRING ( 0 0, 0 2 )'::geometry, geom) OR " +
				"st_coveredby(geog, 'SRID=4326;POINT(-40.23456 70.4567772)'::geography)",
			indexOrd: geomOrd,
			ok:       false,
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters: "st_equals('LINESTRING ( 0 0, 0 2 )'::geometry, geom) AND " +
				"st_coveredby(geog, 'SRID=4326;POINT(-40.23456 70.4567772)'::geography)",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters: "st_equals('LINESTRING ( 0 0, 0 2 )'::geometry, geom) AND " +
				"st_coveredby(geog, 'SRID=4326;POINT(-40.23456 70.4567772)'::geography)",
			indexOrd: geogOrd,
			ok:       true,
		},

		// Bounding box operators.
		{
			filters:  "'BOX(1 2, 3 4)'::box2d ~ geom",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "geom ~ 'BOX(1 2, 3 4)'::box2d",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "'LINESTRING ( 0 0, 0 2 )'::geometry ~ geom",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "geom ~ 'LINESTRING ( 0 0, 0 2 )'::geometry",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "'BOX(1 2, 3 4)'::box2d && geom",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "geom && 'BOX(1 2, 3 4)'::box2d",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "'LINESTRING ( 0 0, 0 2 )'::geometry && geom",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			filters:  "geom && 'LINESTRING ( 0 0, 0 2 )'::geometry",
			indexOrd: geomOrd,
			ok:       true,
		},
		{
			// Wrong index ordinal.
			filters:  "'BOX(1 2, 3 4)'::box2d ~ geom",
			indexOrd: geogOrd,
			ok:       false,
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %v", tc)
		filters, err := buildFilters(tc.filters, &semaCtx, evalCtx, &f)
		if err != nil {
			t.Fatal(err)
		}

		// We're not testing that the correct SpanExpression is returned here;
		// that is tested elsewhere. This is just testing that we are constraining
		// the index when we expect to.
		_, ok := invertedidx.TryConstrainGeoIndex(
			evalCtx.Context, &f, filters, tab, md.Table(tab).Index(tc.indexOrd),
		)
		if tc.ok != ok {
			t.Fatalf("expected %v, got %v", tc.ok, ok)
		}
	}
}

func buildScalar(
	input string, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, f *norm.Factory,
) (opt.ScalarExpr, error) {
	expr, err := parser.ParseExpr(input)
	if err != nil {
		return nil, errors.Newf("falsed to parse %s: %v", input, err)
	}

	b := optbuilder.NewScalar(context.Background(), semaCtx, evalCtx, f)
	if err := b.Build(expr); err != nil {
		return nil, err
	}

	return f.Memo().RootExpr().(opt.ScalarExpr), nil
}

func buildFilters(
	input string, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, f *norm.Factory,
) (memo.FiltersExpr, error) {
	if input == "" {
		return memo.TrueFilter, nil
	}
	root, err := buildScalar(input, semaCtx, evalCtx, f)
	if err != nil {
		return nil, err
	}
	if _, ok := root.(*memo.TrueExpr); ok {
		return memo.TrueFilter, nil
	}
	filters := memo.FiltersExpr{f.ConstructFiltersItem(root)}
	filters = f.CustomFuncs().SimplifyFilters(filters)
	filters = f.CustomFuncs().ConsolidateFilters(filters)
	return filters, nil
}
