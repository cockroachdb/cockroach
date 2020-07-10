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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedidx"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func TestTryGetInvertedJoinCondFromGeoFunc(t *testing.T) {
	semaCtx := tree.MakeSemaContext()
	evalCtx := tree.NewTestingEvalContext(nil /* st */)

	var f norm.Factory
	f.Init(evalCtx, testcat.New())
	md := f.Metadata()
	md.AddColumn("geom1", types.Geometry)
	md.AddColumn("geog1", types.Geography)
	md.AddColumn("geom2", types.Geometry)
	md.AddColumn("geog2", types.Geography)

	testCases := []struct {
		inFunc         string
		commuteArgs    bool
		expOk          bool
		expOutFunc     string
		expInputGeoCol opt.ColumnID
		expIndexGeoCol opt.ColumnID
	}{
		{
			inFunc:         "st_covers(geom1, geom2)",
			commuteArgs:    false,
			expOk:          true,
			expOutFunc:     "st_covers(geom1, geom2)",
			expInputGeoCol: 1,
			expIndexGeoCol: 3,
		},
		{
			inFunc:      "st_covers(geom2, geom1)",
			commuteArgs: false,
			expOk:       false,
		},
		{
			inFunc:      "st_covers(geom1, geom2)",
			commuteArgs: true,
			expOk:       false,
		},
		{
			inFunc:         "st_covers(geom2, geom1)",
			commuteArgs:    true,
			expOk:          true,
			expOutFunc:     "st_coveredby(geom1, geom2)",
			expInputGeoCol: 1,
			expIndexGeoCol: 3,
		},
		{
			inFunc:         "st_coveredby(geog1, geog2)",
			commuteArgs:    false,
			expOk:          true,
			expOutFunc:     "st_coveredby(geog1, geog2)",
			expInputGeoCol: 2,
			expIndexGeoCol: 4,
		},
		{
			inFunc:         "st_coveredby(geog2, geog1)",
			commuteArgs:    true,
			expOk:          true,
			expOutFunc:     "st_covers(geog1, geog2)",
			expInputGeoCol: 2,
			expIndexGeoCol: 4,
		},
		{
			inFunc:         "st_containsproperly(geom2, geom1)",
			commuteArgs:    true,
			expOk:          true,
			expOutFunc:     "st_coveredby(geom1, geom2)",
			expInputGeoCol: 1,
			expIndexGeoCol: 3,
		},
		{
			inFunc:         "st_dwithin(geog2, geog1, 1)",
			commuteArgs:    true,
			expOk:          true,
			expOutFunc:     "st_dwithin(geog2, geog1, 1)",
			expInputGeoCol: 2,
			expIndexGeoCol: 4,
		},
		{
			inFunc:         "st_dfullywithin(geom2, geom1, 1)",
			commuteArgs:    true,
			expOk:          true,
			expOutFunc:     "st_dfullywithin(geom2, geom1, 1)",
			expInputGeoCol: 1,
			expIndexGeoCol: 3,
		},
		{
			inFunc:         "st_intersects(geom1, geom2)",
			commuteArgs:    false,
			expOk:          true,
			expOutFunc:     "st_intersects(geom1, geom2)",
			expInputGeoCol: 1,
			expIndexGeoCol: 3,
		},
		{
			inFunc:         "st_overlaps(geom2, geom1)",
			commuteArgs:    true,
			expOk:          true,
			expOutFunc:     "st_overlaps(geom2, geom1)",
			expInputGeoCol: 1,
			expIndexGeoCol: 3,
		},
	}

	for _, tc := range testCases {
		t.Logf("test case: %v", tc)
		inFunc, err := buildScalar(tc.inFunc, &semaCtx, evalCtx, &f)
		if err != nil {
			t.Fatal(err)
		}

		// The only part of inputProps that is used here is the OutputCols.
		// We just need to make sure the inputGeoCol is part of the OutputCols.
		inputProps := props.Relational{OutputCols: opt.MakeColSet(1, 2)}

		actOutFunc, actInputGeoCol, actIndexGeoCol, actOk := invertedidx.TryGetInvertedJoinCondFromGeoFunc(
			&f, inFunc, tc.commuteArgs, &inputProps,
		)

		if tc.expOk != actOk {
			t.Fatalf("expected %v, got %v", tc.expOk, actOk)
		}
		if !tc.expOk {
			continue
		}

		expOutFunc, err := buildScalar(tc.expOutFunc, &semaCtx, evalCtx, &f)
		if err != nil {
			t.Fatal(err)
		}

		// We have to test the Args and FunctionPrivate individually, since the
		// ScalarID may be different (due to building with the ScalarBuilder).
		if !reflect.DeepEqual(
			expOutFunc.(*memo.FunctionExpr).Args, actOutFunc.(*memo.FunctionExpr).Args,
		) {
			t.Errorf("expected %v, got %v", expOutFunc, actOutFunc)
		}
		if !reflect.DeepEqual(
			expOutFunc.(*memo.FunctionExpr).FunctionPrivate,
			actOutFunc.(*memo.FunctionExpr).FunctionPrivate,
		) {
			t.Errorf("expected %v, got %v", expOutFunc, actOutFunc)
		}
		if tc.expInputGeoCol != actInputGeoCol {
			t.Errorf("expected %v, got %v", tc.expInputGeoCol, actInputGeoCol)
		}
		if tc.expIndexGeoCol != actIndexGeoCol {
			t.Errorf("expected %v, got %v", tc.expIndexGeoCol, actIndexGeoCol)
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
		_, ok := invertedidx.TryConstrainGeoIndex(evalCtx.Context, filters, tab, md.Table(tab).Index(tc.indexOrd))
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
