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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geomfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// infoBuilder is used to build a detailed info string that is consistent between
// geospatial data types.
type infoBuilder struct {
	info        string
	usesGEOS    bool
	canUseIndex bool
}

func (ib infoBuilder) String() string {
	var sb strings.Builder
	sb.WriteString(ib.info)
	if ib.usesGEOS {
		sb.WriteString("\n\nThis function uses the GEOS module.")
	}
	if ib.canUseIndex {
		sb.WriteString("\n\nThis function will automatically use any available index.")
	}
	return sb.String()
}

// geometryFromText is the builtin for ST_GeomFromText/ST_GeometryFromText.
var geometryFromText = makeBuiltin(
	defProps(),
	stringOverload1(
		func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			g, err := geo.ParseEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeometry(geo.NewGeometry(g)), nil
		},
		types.Geometry,
		infoBuilder{info: "Returns the Geometry from a WKT or EWKT representation."}.String(),
	),
	tree.Overload{
		Types:      tree.ArgTypes{{"str", types.String}, {"srid", types.Int}},
		ReturnType: tree.FixedReturnType(types.Geometry),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			srid := geopb.SRID(tree.MustBeDInt(args[1]))
			g, err := geo.ParseEWKT(geopb.EWKT(s), srid, geo.DefaultSRIDShouldOverwrite)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeometry(geo.NewGeometry(g)), nil
		},
		Info: infoBuilder{
			info: `Returns the Geometry from a WKT or EWKT representation with an SRID. If the SRID is present in both the EWKT and the argument, the argument value is used.`,
		}.String(),
	},
)

// geographyFromText is the builtin for ST_GeomFromText/ST_GeographyFromText.
var geographyFromText = makeBuiltin(
	defProps(),
	stringOverload1(
		func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			g, err := geo.ParseEWKT(geopb.EWKT(s), geopb.DefaultGeographySRID, geo.DefaultSRIDIsHint)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeography(geo.NewGeography(g)), nil
		},
		types.Geography,
		infoBuilder{info: "Returns the Geography from a WKT or EWKT representation."}.String(),
	),
	tree.Overload{
		Types:      tree.ArgTypes{{"str", types.String}, {"srid", types.Int}},
		ReturnType: tree.FixedReturnType(types.Geography),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			srid := geopb.SRID(tree.MustBeDInt(args[1]))
			g, err := geo.ParseEWKT(geopb.EWKT(s), srid, geo.DefaultSRIDShouldOverwrite)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeography(geo.NewGeography(g)), nil
		},
		Info: infoBuilder{
			info: `Returns the Geography from a WKT or EWKT representation with an SRID. If the SRID is present in both the EWKT and the argument, the argument value is used.`,
		}.String(),
	},
)

var geoBuiltins = map[string]builtinDefinition{
	//
	// Input (Geometry)
	//

	"st_geomfromtext":     geometryFromText,
	"st_geometryfromtext": geometryFromText,
	"st_geomfromewkt": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geo.NewGeometry(g)), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an EWKT representation."}.String(),
		),
	),
	"st_geomfromwkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseWKB([]byte(s), geopb.DefaultGeometrySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geo.NewGeometry(g)), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from a WKB representation."}.String(),
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"bytes", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseWKB(geopb.WKB(b), srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geo.NewGeometry(g)), nil
			},
			Info: infoBuilder{
				info: `Returns the Geometry from a WKB representation with the given SRID set.`,
			}.String(),
		},
	),
	"st_geomfromewkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseEWKB([]byte(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geo.NewGeometry(g)), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an EWKB representation."}.String(),
		),
	),
	"st_geomfromgeojson": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeoJSON([]byte(s), geopb.DefaultGeometrySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geo.NewGeometry(g)), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an GeoJSON representation."}.String(),
		),
		jsonOverload1(
			func(_ *tree.EvalContext, s json.JSON) (tree.Datum, error) {
				// TODO(otan): optimize to not string it first.
				asString, err := s.AsText()
				if err != nil {
					return nil, err
				}
				g, err := geo.ParseGeoJSON([]byte(*asString), geopb.DefaultGeometrySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geo.NewGeometry(g)), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an GeoJSON representation."}.String(),
		),
	),

	//
	// Input (Geography)
	//

	"st_geogfromtext":      geographyFromText,
	"st_geographyfromtext": geographyFromText,
	"st_geogfromewkt": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseEWKT(geopb.EWKT(s), geopb.DefaultGeographySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(geo.NewGeography(g)), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an EWKT representation."}.String(),
		),
	),
	"st_geogfromwkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseWKB([]byte(s), geopb.DefaultGeographySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(geo.NewGeography(g)), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from a WKB representation."}.String(),
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"bytes", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseWKB(geopb.WKB(b), srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(geo.NewGeography(g)), nil
			},
			Info: infoBuilder{
				info: `Returns the Geography from a WKB representation with the given SRID set.`,
			}.String(),
		},
	),
	"st_geogfromewkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseEWKB([]byte(s), geopb.DefaultGeographySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(geo.NewGeography(g)), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an EWKB representation."}.String(),
		),
	),
	"st_geogfromgeojson": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeoJSON([]byte(s), geopb.DefaultGeographySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(geo.NewGeography(g)), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an GeoJSON representation."}.String(),
		),
		jsonOverload1(
			func(_ *tree.EvalContext, s json.JSON) (tree.Datum, error) {
				// TODO(otan): optimize to not string it first.
				asString, err := s.AsText()
				if err != nil {
					return nil, err
				}
				g, err := geo.ParseGeoJSON([]byte(*asString), geopb.DefaultGeographySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(geo.NewGeography(g)), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an GeoJSON representation."}.String(),
		),
	),

	//
	// Output
	//

	"st_astext": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				wkt, err := geo.EWKBToWKT(g.Geometry.EWKB())
				return tree.NewDString(string(wkt)), err
			},
			types.String,
			infoBuilder{info: "Returns the WKT representation of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkt, err := geo.EWKBToWKT(g.Geography.EWKB())
				return tree.NewDString(string(wkt)), err
			},
			types.String,
			infoBuilder{info: "Returns the WKT representation of a given Geography."},
		),
	),
	"st_asewkt": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ewkt, err := geo.EWKBToEWKT(g.Geometry.EWKB())
				return tree.NewDString(string(ewkt)), err
			},
			types.String,
			infoBuilder{info: "Returns the EWKT representation of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				ewkt, err := geo.EWKBToEWKT(g.Geography.EWKB())
				return tree.NewDString(string(ewkt)), err
			},
			types.String,
			infoBuilder{info: "Returns the EWKT representation of a given Geography."},
		),
	),
	"st_asbinary": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				wkb, err := geo.EWKBToWKB(g.Geometry.EWKB())
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			types.Bytes,
			infoBuilder{info: "Returns the WKB representation of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkb, err := geo.EWKBToWKB(g.Geography.EWKB())
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			types.Bytes,
			infoBuilder{info: "Returns the WKB representation of a given Geography."},
		),
	),
	"st_asewkb": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return tree.NewDBytes(tree.DBytes(g.EWKB())), nil
			},
			types.Bytes,
			infoBuilder{info: "Returns the EWKB representation of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				return tree.NewDBytes(tree.DBytes(g.EWKB())), nil
			},
			types.Bytes,
			infoBuilder{info: "Returns the EWKB representation of a given Geography."},
		),
	),
	"st_ashexwkb": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				hexwkb, err := geo.EWKBToWKBHex(g.Geometry.EWKB())
				return tree.NewDString(hexwkb), err
			},
			types.String,
			infoBuilder{info: "Returns the WKB representation in hex of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				hexwkb, err := geo.EWKBToWKBHex(g.Geography.EWKB())
				return tree.NewDString(hexwkb), err
			},
			types.String,
			infoBuilder{info: "Returns the WKB representation in hex of a given Geography."},
		),
	),
	"st_ashexewkb": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", g.EWKB()))), nil
			},
			types.String,
			infoBuilder{info: "Returns the EWKB representation in hex of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", g.EWKB()))), nil
			},
			types.String,
			infoBuilder{info: "Returns the EWKB representation in hex of a given Geography."},
		),
	),
	"st_askml": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				kml, err := geo.EWKBToKML(g.Geometry.EWKB())
				return tree.NewDString(kml), err
			},
			types.String,
			infoBuilder{info: "Returns the KML representation of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				kml, err := geo.EWKBToKML(g.Geography.EWKB())
				return tree.NewDString(kml), err
			},
			types.String,
			infoBuilder{info: "Returns the KML representation of a given Geography."},
		),
	),
	"st_asgeojson": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				geojson, err := geo.EWKBToGeoJSON(g.Geometry.EWKB())
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{info: "Returns the GeoJSON representation of a given Geometry."},
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				geojson, err := geo.EWKBToGeoJSON(g.Geography.EWKB())
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{info: "Returns the GeoJSON representation of a given Geography."},
		),
	),

	//
	// Unary functions.
	//
	// TODO(rytaft, otan): replace this with a real function.
	"st_area": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(0), errors.Newf("ST_Area is not yet supported.")
			},
			Info: "Returns the area of geometry_a",
		},
	),

	//
	// Binary Predicates
	//

	"st_covers": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Covers,
			infoBuilder{
				info:        "Returns true if no point in geometry_b is outside geometry_a.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_coveredby": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.CoveredBy,
			infoBuilder{
				info:        "Returns true if no point in geometry_a is outside geometry_b.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_contains": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Contains,
			infoBuilder{
				info: "Returns true if no points of geometry_b lie in the exterior of geometry_a, " +
					"and there is at least one point in the interior of geometry_b that lies in the interior of geometry_a.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_crosses": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Crosses,
			infoBuilder{
				info:        "Returns true if geometry_a has some - but not all - interior points in common with geometry_b.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_equals": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Equals,
			infoBuilder{
				info: "Returns true if geometry_a is spatially equal to geometry_b, " +
					"i.e. ST_Within(geometry_a, geometry_b) = ST_Within(geometry_b, geometry_a) = true.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_intersects": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Intersects,
			infoBuilder{
				info:        "Returns true if geometry_a shares any portion of space with geometry_b.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_overlaps": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Overlaps,
			infoBuilder{
				info: "Returns true if geometry_a intersects but does not completely contain geometry_b, or vice versa. " +
					`"Does not completely" implies ST_Within(geometry_a, geometry_b) = ST_Within(geometry_b, geometry_a) = false.`,
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_touches": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Touches,
			infoBuilder{
				info: "Returns true if the only points in common between geometry_a and geometry_b are on the boundary. " +
					"Note points do not touch other points.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
	"st_within": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Within,
			infoBuilder{
				info:        "Returns true if geometry_a is completely inside geometry_b.",
				usesGEOS:    true,
				canUseIndex: true,
			},
		),
	),
}

// geometryOverload1 hides the boilerplate for builtins operating on one geometry.
func geometryOverload1(
	f func(*tree.EvalContext, *tree.DGeometry) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
) tree.Overload {
	return tree.Overload{
		Types: tree.ArgTypes{
			{"geometry", types.Geometry},
		},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			a := args[0].(*tree.DGeometry)
			return f(ctx, a)
		},
		Info: ib.String(),
	}
}

// geographyOverload1 hides the boilerplate for builtins operating on one geography.
func geographyOverload1(
	f func(*tree.EvalContext, *tree.DGeography) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
) tree.Overload {
	return tree.Overload{
		Types: tree.ArgTypes{
			{"geography", types.Geography},
		},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			a := args[0].(*tree.DGeography)
			return f(ctx, a)
		},
		Info: ib.String(),
	}
}

// geometryOverload2 hides the boilerplate for builtins operating on two geometries.
func geometryOverload2(
	f func(*tree.EvalContext, *tree.DGeometry, *tree.DGeometry) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
) tree.Overload {
	return tree.Overload{
		Types: tree.ArgTypes{
			{"geometry_a", types.Geometry},
			{"geometry_b", types.Geometry},
		},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			a := args[0].(*tree.DGeometry)
			b := args[1].(*tree.DGeometry)
			return f(ctx, a, b)
		},
		Info: ib.String(),
	}
}

// geometryOverload2 hides the boilerplate for builtins operating on two geometries
// and the overlap wraps a binary predicate.
func geometryOverload2BinaryPredicate(
	f func(*geo.Geometry, *geo.Geometry) (bool, error), ib infoBuilder,
) tree.Overload {
	return geometryOverload2(
		func(_ *tree.EvalContext, a *tree.DGeometry, b *tree.DGeometry) (tree.Datum, error) {
			ret, err := f(a.Geometry, b.Geometry)
			if err != nil {
				return nil, err
			}
			return tree.MakeDBool(tree.DBool(ret)), nil
		},
		types.Bool,
		ib,
	)
}

func initGeoBuiltins() {
	for k, v := range geoBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		v.props.Category = categoryGeospatial
		builtins[k] = v
	}
}
