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
	"math"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/stretchr/testify/require"
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

func TestTryFilterGeoIndex(t *testing.T) {
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
		filters             string
		indexOrd            int
		ok                  bool
		preFilterExpr       string
		preFilterCol        opt.ColumnID
		preFilterTypeFamily types.Family
	}{
		{
			filters:             "st_intersects('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_intersects('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			// Still works with arguments commuted.
			filters:             "st_intersects(geom, 'LINESTRING ( 0 0, 0 2 )'::geometry)",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_intersects('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "st_covers('SRID=4326;POINT(-40.23456 70.4567772)'::geography, geog)",
			indexOrd:            geogOrd,
			ok:                  true,
			preFilterExpr:       "st_covers('SRID=4326;POINT(-40.23456 70.4567772)'::geography, geog)",
			preFilterCol:        2,
			preFilterTypeFamily: types.GeographyFamily,
		},
		{
			// Still works with arguments commuted.
			filters:             "st_covers(geog, 'SRID=4326;POINT(-40.23456 70.4567772)'::geography)",
			indexOrd:            geogOrd,
			ok:                  true,
			preFilterExpr:       "st_coveredby('SRID=4326;POINT(-40.23456 70.4567772)'::geography, geog)",
			preFilterCol:        2,
			preFilterTypeFamily: types.GeographyFamily,
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
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_equals('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			// We can constrain either index when the functions are AND-ed.
			filters: "st_equals('LINESTRING ( 0 0, 0 2 )'::geometry, geom) AND " +
				"st_coveredby(geog, 'SRID=4326;POINT(-40.23456 70.4567772)'::geography)",
			indexOrd:            geogOrd,
			ok:                  true,
			preFilterExpr:       "st_covers('SRID=4326;POINT(-40.23456 70.4567772)'::geography, geog)",
			preFilterCol:        2,
			preFilterTypeFamily: types.GeographyFamily,
		},

		// Bounding box operators.
		{
			filters:             "'BOX(1 2, 3 4)'::box2d ~ geom",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_covers('POLYGON (( 1 2, 1 4, 3 4, 3 2, 1 2))'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "geom ~ 'BOX(1 2, 3 4)'::box2d",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_coveredby('POLYGON (( 1 2, 1 4, 3 4, 3 2, 1 2))'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "'LINESTRING ( 0 0, 0 2 )'::geometry ~ geom",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_covers('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "geom ~ 'LINESTRING ( 0 0, 0 2 )'::geometry",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_coveredby('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "'BOX(1 2, 3 4)'::box2d && geom",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_intersects('POLYGON (( 1 2, 1 4, 3 4, 3 2, 1 2))'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "geom && 'BOX(1 2, 3 4)'::box2d",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_intersects('POLYGON (( 1 2, 1 4, 3 4, 3 2, 1 2))'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "'LINESTRING ( 0 0, 0 2 )'::geometry && geom",
			indexOrd:            geomOrd,
			ok:                  true,
			preFilterExpr:       "st_intersects('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
		},
		{
			filters:             "geom && 'LINESTRING ( 0 0, 0 2 )'::geometry",
			indexOrd:            geomOrd,
			preFilterExpr:       "st_intersects('LINESTRING ( 0 0, 0 2 )'::geometry, geom)",
			preFilterCol:        1,
			preFilterTypeFamily: types.GeometryFamily,
			ok:                  true,
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
		filters := testutils.BuildFilters(t, &f, &semaCtx, evalCtx, tc.filters)

		// We're not testing that the correct SpanExpression is returned here;
		// that is tested elsewhere. This is just testing that we are constraining
		// the index when we expect to.
		spanExpr, _, remainingFilters, pfState, ok := invertedidx.TryFilterInvertedIndex(
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

		if ok {
			if spanExpr.Unique {
				t.Fatalf("span expressions for geospatial indexes should never have Unique=true")
			}
			if spanExpr.Tight {
				t.Fatalf("span expressions for geospatial indexes should never have Tight=true")
			}
			if remainingFilters.String() != filters.String() {
				t.Errorf("expected remainingFilters=%v, got %v", filters, remainingFilters)
			}

			if len(tc.preFilterExpr) == 0 {
				require.Nil(t, pfState)
			} else {
				require.NotNil(t, pfState)
				pfExpr := testutils.BuildScalar(t, &f, &semaCtx, evalCtx, tc.preFilterExpr)
				require.Equal(t, pfExpr.String(), pfState.Expr.String())
				require.Equal(t, tc.preFilterCol, pfState.Col)
				require.Equal(t, tc.preFilterTypeFamily, pfState.Typ.Family())
			}
		}
	}
}

func TestPreFilterer(t *testing.T) {
	// Test cases do pre-filtering for (geoShapes[i], geoShapes[j]) for all i,
	// j.
	geoShapes := []string{
		"SRID=4326;POINT(0 0)",
		"SRID=4326;POINT(5 5)",
		"SRID=4326;LINESTRING(8 8, 9 9)",
		"SRID=4326;POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))",
	}
	testCases := []struct {
		// The typ, relationship and relationshipParams determine how the
		// PreFilterer works.
		typ                *types.T
		relationship       geoindex.RelationshipType
		relationshipParams []tree.Datum
		shapes             []string
		expected           [][]bool
		// excludeFromPreFilters excludes shapes at the given indexes from being
		// used in Bind calls.
		excludeFromPreFilters []int
	}{
		{
			typ:          types.Geometry,
			relationship: geoindex.Intersects,
			shapes:       geoShapes,
			expected: [][]bool{
				{true, false, false, true},
				{false, true, false, true},
				{false, false, true, false},
				{true, true, false, true},
			},
		},
		{
			typ:          types.Geometry,
			relationship: geoindex.Covers,
			shapes:       geoShapes,
			expected: [][]bool{
				{true, false, false, true},
				{false, true, false, true},
				{false, false, true, false},
				{false, false, false, true},
			},
		},
		{
			typ:          types.Geometry,
			relationship: geoindex.CoveredBy,
			shapes:       geoShapes,
			expected: [][]bool{
				{true, false, false, false},
				{false, true, false, false},
				{false, false, true, false},
				{true, true, false, true},
			},
		},
		{
			typ:                types.Geometry,
			relationship:       geoindex.DWithin,
			relationshipParams: []tree.Datum{tree.NewDFloat(3)},
			shapes:             geoShapes,
			expected: [][]bool{
				{true, false, false, true},
				{false, true, true, true},
				{false, true, true, true},
				{true, true, true, true},
			},
		},
		{
			typ:                types.Geometry,
			relationship:       geoindex.DFullyWithin,
			relationshipParams: []tree.Datum{tree.NewDFloat(3)},
			shapes:             geoShapes,
			expected: [][]bool{
				{true, false, false, false},
				{false, true, false, false},
				{false, true, true, false},
				{true, true, false, true},
			},
		},
		{
			typ:          types.Geography,
			relationship: geoindex.Intersects,
			shapes:       geoShapes,
			expected: [][]bool{
				{true, false, false, true},
				{false, true, false, true},
				{false, false, true, false},
				{true, true, false, true},
			},
		},
		{
			typ:          types.Geography,
			relationship: geoindex.Covers,
			shapes:       geoShapes,
			expected: [][]bool{
				{true, false, false, true},
				{false, true, false, true},
				{false, false, true, false},
				{false, false, false, true},
			},
		},
		{
			typ:          types.Geography,
			relationship: geoindex.CoveredBy,
			shapes:       geoShapes,
			expected: [][]bool{
				{true, false, false, false},
				{false, true, false, false},
				{false, false, true, false},
				{true, true, false, true},
			},
		},
		{
			typ:                types.Geography,
			relationship:       geoindex.DWithin,
			relationshipParams: []tree.Datum{tree.NewDFloat(3)},
			shapes:             geoShapes,
			expected: [][]bool{
				{true, false, false, true},
				{false, true, false, true},
				{false, false, true, false},
				{true, true, false, true},
			},
		},
		{
			typ:                   types.Geography,
			relationship:          geoindex.DWithin,
			relationshipParams:    []tree.Datum{tree.NewDFloat(3)},
			shapes:                geoShapes,
			excludeFromPreFilters: []int{2},
			expected: [][]bool{
				{true, false, true},
				{false, true, true},
				{false, false, false},
				{true, true, true},
			},
		},
	}
	encodeInv := func(bbox geopb.BoundingBox) inverted.EncVal {
		var b []byte
		b = encoding.EncodeGeoInvertedAscending(b)
		// Arbitrary cellid
		b = encoding.EncodeUvarintAscending(b, math.MaxUint32)
		b = encoding.EncodeGeoInvertedBBox(b, bbox.LoX, bbox.LoY, bbox.HiX, bbox.HiY)
		return b
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			filterer := invertedidx.NewPreFilterer(tc.typ, tc.relationship, tc.relationshipParams)
			var toBind []tree.Datum
			var toPreFilter []inverted.EncVal
			includeBind := func(index int) bool {
				for _, exclude := range tc.excludeFromPreFilters {
					if exclude == index {
						return false
					}
				}
				return true
			}
			for i, shape := range tc.shapes {
				switch tc.typ {
				case types.Geometry:
					g, err := geo.ParseGeometry(shape)
					require.NoError(t, err)
					if includeBind(i) {
						toBind = append(toBind, tree.NewDGeometry(g))
					}
					toPreFilter = append(toPreFilter, encodeInv(*g.BoundingBoxRef()))
				case types.Geography:
					g, err := geo.ParseGeography(shape)
					require.NoError(t, err)
					if includeBind(i) {
						toBind = append(toBind, tree.NewDGeography(g))
					}
					rect := g.BoundingRect()
					toPreFilter = append(toPreFilter,
						encodeInv(geopb.BoundingBox{
							LoX: rect.Lng.Lo,
							HiX: rect.Lng.Hi,
							LoY: rect.Lat.Lo,
							HiY: rect.Lat.Hi,
						}))
				}
			}
			var preFilterState []interface{}
			for _, d := range toBind {
				preFilterState = append(preFilterState, filterer.Bind(d))
			}
			result := make([]bool, len(preFilterState))
			for i, enc := range toPreFilter {
				res, err := filterer.PreFilter(enc, preFilterState, result)
				require.NoError(t, err)
				expectedRes := false
				for _, b := range result {
					expectedRes = expectedRes || b
				}
				require.Equal(t, expectedRes, res)
				require.Equal(t, tc.expected[i], result)
			}
		})
	}
}

// TODO(sumeer): test for NewGeoDatumsToInvertedExpr, geoDatumsToInvertedExpr.
