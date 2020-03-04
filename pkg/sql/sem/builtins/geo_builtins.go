// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/twpayne/go-geom"
)

var geoBuiltins = map[string]builtinDefinition{
	"st_geometryfromtext": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"wkt", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO: validate SRID has not been set.
				return tree.ParseDGeometry(string(*args[0].(*tree.DString)))
			},
			Info: "Forms a Geometry from WKT.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"wkt", types.String},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO: set SRID
				return tree.ParseDGeometry(string(*args[0].(*tree.DString)))
			},
			Info: "Forms a Geometry from WKT and SRID.",
		},
	),
	"st_geomfromtext": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"wkt", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO: validate SRID has not been set.
				return tree.ParseDGeometry(string(*args[0].(*tree.DString)))
			},
			Info: "Forms a Geometry from WKT.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"wkt", types.String},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO: set SRID
				return tree.ParseDGeometry(string(*args[0].(*tree.DString)))
			},
			Info: "Forms a Geometry from WKT and SRID.",
		},
	),
	"st_geographyfromtext": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"ewkt", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.ParseDGeography(string(*args[0].(*tree.DString)))
			},
			Info: "Forms a Geography from EWKT.",
		},
	),
	"st_astext": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(args[0].(*tree.DGeometry).WKT()), nil
			},
			Info: "Returns the WKT of the geometry.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(args[0].(*tree.DGeography).WKT()), nil
			},
			Info: "Returns the WKT of the geography.",
		},
	),
	"st_distance": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"from_geometry", types.Geometry},
				{"to_geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				distance, err := lhs.GEOS().Distance(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(distance)), nil
			},
			Info: "Returns the distance between two geometries.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"from_geography", types.Geography},
				{"to_geography", types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeography)
				rhs := args[1].(*tree.DGeography)
				d := lhs.GeographicDistance(&rhs.Geometry, true /* useSpheroid */)
				return tree.NewDFloat(tree.DFloat(d)), nil
			},
			Info: "Returns the distance between two geometries on the Earth's sphere.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"from_geography", types.Geography},
				{"to_geography", types.Geography},
				{"use_spheroid", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeography)
				rhs := args[1].(*tree.DGeography)
				useSpheroid := bool(*args[2].(*tree.DBool))
				d := lhs.GeographicDistance(&rhs.Geometry, useSpheroid)
				return tree.NewDFloat(tree.DFloat(d)), nil
			},
			Info: "Returns the distance between two geometries on the Earth's sphere.",
		},
	),
	"st_area": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				area, err := g.GEOS().Area()
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(area)), nil
			},
			Info: "Returns the distance between two geometries.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				return tree.NewDFloat(tree.DFloat(g.GeographicArea() * 6371010 * 6371010)), nil
			},
			Info: "Returns the distance between two geometries on the Earth's sphere.",
		},
	),
	"st_srid": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				return tree.NewDInt(tree.DInt(g.SRID())), nil
			},
			Info: "TODO",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				return tree.NewDInt(tree.DInt(g.SRID())), nil
			},
			Info: "TODO",
		},
	),
	"st_x": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				switch t := g.T.(type) {
				case *geom.Point:
					return tree.NewDFloat(tree.DFloat(t.X())), nil
				}
				return nil, pgerror.New(pgcode.InvalidParameterValue, "Argument to ST_X() must have type POINT")
			},
			Info: "Returns the X coordinate of a geometry.",
		},
	),
	"st_y": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				switch t := g.T.(type) {
				case *geom.Point:
					return tree.NewDFloat(tree.DFloat(t.Y())), nil
				}
				return nil, pgerror.New(pgcode.InvalidParameterValue, "Argument to ST_Y() must have type POINT")
			},
			Info: "Returns the Y coordinate of a geometry.",
		},
	),
	"st_contains": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				contains, err := lhs.GEOS().Contains(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(contains)), nil
			},
			Info: "TODO",
		},
	),
	"st_crosses": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				crosses, err := lhs.GEOS().Crosses(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(crosses)), nil
			},
			Info: "TODO",
		},
	),
	"st_disjoint": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				disjoint, err := lhs.GEOS().Disjoint(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(disjoint)), nil
			},
			Info: "TODO",
		},
	),
	// TODO(#geos): DWithin is supported by GEOS, but not go-geom
	"st_equals": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				equals, err := lhs.GEOS().Equals(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(equals)), nil
			},
			Info: "TODO",
		},
	),
	"st_intersects": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				intersects, err := lhs.GEOS().Intersects(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(intersects)), nil
			},
			Info: "TODO",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geography},
				{"B", types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeography)
				rhs := args[1].(*tree.DGeography)
				return tree.MakeDBool(tree.DBool(lhs.GeographicIntersect(&rhs.Geometry))), nil
			},
			Info: "TODO",
		},
	),
	"st_overlaps": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				overlaps, err := lhs.GEOS().Overlaps(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(overlaps)), nil
			},
			Info: "TODO",
		},
	),
	"st_touches": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				overlaps, err := lhs.GEOS().Touches(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(overlaps)), nil
			},
			Info: "TODO",
		},
	),
	"st_within": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"A", types.Geometry},
				{"B", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lhs := args[0].(*tree.DGeometry)
				rhs := args[1].(*tree.DGeometry)
				within, err := lhs.GEOS().Within(rhs.GEOS())
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(within)), nil
			},
			Info: "TODO",
		},
	),
}

func initGeoBuiltins() {
	for k, v := range geoBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		v.props.Category = categoryCompatibility
		builtins[k] = v
	}
}
