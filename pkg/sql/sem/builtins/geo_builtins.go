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
	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geomfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// libraryUsage is a masked bit indicating which libraries are used by
// a given geospatial builtin.
type libraryUsage uint64

const (
	usesGEOS libraryUsage = (1 << (iota + 1))
	usesS2
	usesGeographicLib
)

const usesSpheroidMessage = " Uses a spheroid to perform the operation."
const spheroidDistanceMessage = `"\n\nWhen operating on a spheroid, this function will use the sphere to calculate ` +
	`the closest two points using S2. The spheroid distance between these two points is calculated using GeographicLib. ` +
	`This follows observed PostGIS behavior.`

// infoBuilder is used to build a detailed info string that is consistent between
// geospatial data types.
type infoBuilder struct {
	info         string
	libraryUsage libraryUsage
	precision    string
	canUseIndex  bool
}

func (ib infoBuilder) String() string {
	var sb strings.Builder
	sb.WriteString(ib.info)
	if ib.precision != "" {
		sb.WriteString(fmt.Sprintf("\n\nThe calculations performed are have a precision of %s.", ib.precision))
	}
	if ib.libraryUsage&usesGEOS != 0 {
		sb.WriteString("\n\nThis function utilizes the GEOS module.")
	}
	if ib.libraryUsage&usesS2 != 0 {
		sb.WriteString("\n\nThis function utilizes the S2 library for spherical calculations.")
	}
	if ib.libraryUsage&usesGeographicLib != 0 {
		sb.WriteString("\n\nThis function utilizes the GeographicLib library for spheroid calculations.")
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
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeometry(g), nil
		},
		types.Geometry,
		infoBuilder{info: "Returns the Geometry from a WKT or EWKT representation."}.String(),
		tree.VolatilityImmutable,
	),
	tree.Overload{
		Types:      tree.ArgTypes{{"str", types.String}, {"srid", types.Int}},
		ReturnType: tree.FixedReturnType(types.Geometry),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			srid := geopb.SRID(tree.MustBeDInt(args[1]))
			g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), srid, geo.DefaultSRIDShouldOverwrite)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeometry(g), nil
		},
		Info: infoBuilder{
			info: `Returns the Geometry from a WKT or EWKT representation with an SRID. If the SRID is present in both the EWKT and the argument, the argument value is used.`,
		}.String(),
		Volatility: tree.VolatilityImmutable,
	},
)

// geometryFromTextCheckShapeBuiltin is used for the ST_<Shape>FromText builtins.
func geometryFromTextCheckShapeBuiltin(shape geopb.Shape) builtinDefinition {
	return makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				if g.Shape() != shape {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the Geometry from a WKT or EWKT representation. If the shape underneath is not %s, NULL is returned.",
					shape.String(),
				),
			}.String(),
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.String}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), srid, geo.DefaultSRIDShouldOverwrite)
				if err != nil {
					return nil, err
				}
				if g.Shape() != shape {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					`Returns the Geometry from a WKT or EWKT representation with an SRID. If the shape underneath is not %s, NULL is returned. If the SRID is present in both the EWKT and the argument, the argument value is used.`,
					shape.String(),
				),
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	)
}

// geometryFromWKBCheckShapeBuiltin is used for the ST_<Shape>FromWKB builtins.
func geometryFromWKBCheckShapeBuiltin(shape geopb.Shape) builtinDefinition {
	return makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromWKB(geopb.WKB(s), geopb.DefaultGeometrySRID)
				if err != nil {
					return nil, err
				}
				if g.Shape() != shape {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the Geometry from a WKB representation. If the shape underneath is not %s, NULL is returned.",
					shape.String(),
				),
			}.String(),
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"wkb", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseGeometryFromWKB(geopb.WKB(s), srid)
				if err != nil {
					return nil, err
				}
				if g.Shape() != shape {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					`Returns the Geometry from a WKB representation with an SRID. If the shape underneath is not %s, NULL is returned.`,
					shape.String(),
				),
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	)
}

// geographyFromText is the builtin for ST_GeomFromText/ST_GeographyFromText.
var geographyFromText = makeBuiltin(
	defProps(),
	stringOverload1(
		func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			g, err := geo.ParseGeographyFromEWKT(geopb.EWKT(s), geopb.DefaultGeographySRID, geo.DefaultSRIDIsHint)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeography(g), nil
		},
		types.Geography,
		infoBuilder{info: "Returns the Geography from a WKT or EWKT representation."}.String(),
		tree.VolatilityImmutable,
	),
	tree.Overload{
		Types:      tree.ArgTypes{{"str", types.String}, {"srid", types.Int}},
		ReturnType: tree.FixedReturnType(types.Geography),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			srid := geopb.SRID(tree.MustBeDInt(args[1]))
			g, err := geo.ParseGeographyFromEWKT(geopb.EWKT(s), srid, geo.DefaultSRIDShouldOverwrite)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeography(g), nil
		},
		Info: infoBuilder{
			info: `Returns the Geography from a WKT or EWKT representation with an SRID. If the SRID is present in both the EWKT and the argument, the argument value is used.`,
		}.String(),
		Volatility: tree.VolatilityImmutable,
	},
)

var pointCoordsToGeomOverload = tree.Overload{
	Types:      tree.ArgTypes{{"x", types.Float}, {"y", types.Float}},
	ReturnType: tree.FixedReturnType(types.Geometry),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		x := float64(*args[0].(*tree.DFloat))
		y := float64(*args[1].(*tree.DFloat))
		g, err := geo.NewGeometryFromPointCoords(x, y)
		if err != nil {
			return nil, err
		}
		return tree.NewDGeometry(g), nil
	},
	Info:       infoBuilder{info: `Returns a new Point with the given X and Y coordinates.`}.String(),
	Volatility: tree.VolatilityImmutable,
}

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
				g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an EWKT representation."}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_geomfromwkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromWKB([]byte(s), geopb.DefaultGeometrySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from a WKB representation."}.String(),
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"bytes", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseGeometryFromWKB(geopb.WKB(b), srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: `Returns the Geometry from a WKB representation with the given SRID set.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_geomfromewkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromEWKB([]byte(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an EWKB representation."}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_geomfromgeojson": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromGeoJSON([]byte(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an GeoJSON representation."}.String(),
			tree.VolatilityImmutable,
		),
		jsonOverload1(
			func(_ *tree.EvalContext, s json.JSON) (tree.Datum, error) {
				// TODO(otan): optimize to not string it first.
				asString, err := s.AsText()
				if err != nil {
					return nil, err
				}
				if asString == nil {
					return tree.DNull, nil
				}
				g, err := geo.ParseGeometryFromGeoJSON([]byte(*asString))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{info: "Returns the Geometry from an GeoJSON representation."}.String(),
			tree.VolatilityImmutable,
		),
	),

	"st_geomcollfromtext":        geometryFromTextCheckShapeBuiltin(geopb.Shape_GeometryCollection),
	"st_geomcollfromwkb":         geometryFromWKBCheckShapeBuiltin(geopb.Shape_GeometryCollection),
	"st_linefromtext":            geometryFromTextCheckShapeBuiltin(geopb.Shape_LineString),
	"st_linefromwkb":             geometryFromWKBCheckShapeBuiltin(geopb.Shape_LineString),
	"st_linestringfromtext":      geometryFromTextCheckShapeBuiltin(geopb.Shape_LineString), // missing from PostGIS
	"st_linestringfromwkb":       geometryFromWKBCheckShapeBuiltin(geopb.Shape_LineString),
	"st_mlinefromtext":           geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiLineString),
	"st_mlinefromwkb":            geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiLineString),
	"st_mpointfromtext":          geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiPoint),
	"st_mpointfromwkb":           geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiPoint),
	"st_mpolyfromtext":           geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiPolygon),
	"st_mpolyfromwkb":            geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiPolygon),
	"st_multilinefromtext":       geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiLineString), // missing from PostGIS
	"st_multilinefromwkb":        geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiLineString),
	"st_multilinestringfromtext": geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiLineString),
	"st_multilinestringfromwkb":  geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiLineString), // missing from PostGIS
	"st_multipointfromtext":      geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiPoint),     // SRID version missing from PostGIS
	"st_multipointfromwkb":       geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiPoint),
	"st_multipolyfromtext":       geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiPolygon), // missing from PostGIS
	"st_multipolyfromwkb":        geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiPolygon),
	"st_multipolygonfromtext":    geometryFromTextCheckShapeBuiltin(geopb.Shape_MultiPolygon),
	"st_multipolygonfromwkb":     geometryFromWKBCheckShapeBuiltin(geopb.Shape_MultiPolygon), // missing from PostGIS
	"st_pointfromtext":           geometryFromTextCheckShapeBuiltin(geopb.Shape_Point),
	"st_pointfromwkb":            geometryFromWKBCheckShapeBuiltin(geopb.Shape_Point),
	"st_polyfromtext":            geometryFromTextCheckShapeBuiltin(geopb.Shape_Polygon),
	"st_polyfromwkb":             geometryFromWKBCheckShapeBuiltin(geopb.Shape_Polygon),
	"st_polygonfromtext":         geometryFromTextCheckShapeBuiltin(geopb.Shape_Polygon),
	"st_polygonfromwkb":          geometryFromWKBCheckShapeBuiltin(geopb.Shape_Polygon),

	//
	// Input (Geography)
	//

	"st_geogfromtext":      geographyFromText,
	"st_geographyfromtext": geographyFromText,
	"st_geogfromewkt": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeographyFromEWKT(geopb.EWKT(s), geopb.DefaultGeographySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an EWKT representation."}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_geogfromwkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeographyFromWKB([]byte(s), geopb.DefaultGeographySRID)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from a WKB representation."}.String(),
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"bytes", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseGeographyFromWKB(geopb.WKB(b), srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			Info: infoBuilder{
				info: `Returns the Geography from a WKB representation with the given SRID set.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_geogfromewkb": makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeographyFromEWKB([]byte(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an EWKB representation."}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_geogfromgeojson": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeographyFromGeoJSON([]byte(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an GeoJSON representation."}.String(),
			tree.VolatilityImmutable,
		),
		jsonOverload1(
			func(_ *tree.EvalContext, s json.JSON) (tree.Datum, error) {
				// TODO(otan): optimize to not string it first.
				asString, err := s.AsText()
				if err != nil {
					return nil, err
				}
				if asString == nil {
					return tree.DNull, nil
				}
				g, err := geo.ParseGeographyFromGeoJSON([]byte(*asString))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from an GeoJSON representation."}.String(),
			tree.VolatilityImmutable,
		),
	),
	"st_point":     makeBuiltin(defProps(), pointCoordsToGeomOverload),
	"st_makepoint": makeBuiltin(defProps(), pointCoordsToGeomOverload),

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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkt, err := geo.EWKBToWKT(g.Geography.EWKB())
				return tree.NewDString(string(wkt)), err
			},
			types.String,
			infoBuilder{info: "Returns the WKT representation of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				ewkt, err := geo.EWKBToEWKT(g.Geography.EWKB())
				return tree.NewDString(string(ewkt)), err
			},
			types.String,
			infoBuilder{info: "Returns the EWKT representation of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkb, err := geo.EWKBToWKB(g.Geography.EWKB())
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			types.Bytes,
			infoBuilder{info: "Returns the WKB representation of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				return tree.NewDBytes(tree.DBytes(g.EWKB())), nil
			},
			types.Bytes,
			infoBuilder{info: "Returns the EWKB representation of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				hexwkb, err := geo.EWKBToWKBHex(g.Geography.EWKB())
				return tree.NewDString(hexwkb), err
			},
			types.String,
			infoBuilder{info: "Returns the WKB representation in hex of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(fmt.Sprintf("%x", g.EWKB()))), nil
			},
			types.String,
			infoBuilder{info: "Returns the EWKB representation in hex of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				kml, err := geo.EWKBToKML(g.Geography.EWKB())
				return tree.NewDString(kml), err
			},
			types.String,
			infoBuilder{info: "Returns the KML representation of a given Geography."},
			tree.VolatilityImmutable,
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
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				geojson, err := geo.EWKBToGeoJSON(g.Geography.EWKB())
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{info: "Returns the GeoJSON representation of a given Geography."},
			tree.VolatilityImmutable,
		),
	),

	//
	// Unary functions.
	//
	"st_area": makeBuiltin(
		defProps(),
		append(
			geographyOverload1WithUseSpheroid(
				func(ctx *tree.EvalContext, g *tree.DGeography, useSphereOrSpheroid geogfn.UseSphereOrSpheroid) (tree.Datum, error) {
					ret, err := geogfn.Area(g.Geography, useSphereOrSpheroid)
					if err != nil {
						return nil, err
					}
					return tree.NewDFloat(tree.DFloat(ret)), nil
				},
				types.Float,
				infoBuilder{
					info: "Returns the area of the given geography in meters^2.",
				},
				tree.VolatilityImmutable,
			),
			geometryOverload1(
				func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
					ret, err := geomfn.Area(g.Geometry)
					if err != nil {
						return nil, err
					}
					return tree.NewDFloat(tree.DFloat(ret)), nil
				},
				types.Float,
				infoBuilder{
					info:         "Returns the area of the given geometry.",
					libraryUsage: usesGEOS,
				},
				tree.VolatilityImmutable,
			),
		)...,
	),
	"st_length": makeBuiltin(
		defProps(),
		append(
			geographyOverload1WithUseSpheroid(
				func(ctx *tree.EvalContext, g *tree.DGeography, useSphereOrSpheroid geogfn.UseSphereOrSpheroid) (tree.Datum, error) {
					ret, err := geogfn.Length(g.Geography, useSphereOrSpheroid)
					if err != nil {
						return nil, err
					}
					return tree.NewDFloat(tree.DFloat(ret)), nil
				},
				types.Float,
				infoBuilder{
					info: "Returns the length of the given geography in meters.",
				},
				tree.VolatilityImmutable,
			),
			geometryOverload1(
				func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
					ret, err := geomfn.Length(g.Geometry)
					if err != nil {
						return nil, err
					}
					return tree.NewDFloat(tree.DFloat(ret)), nil
				},
				types.Float,
				infoBuilder{
					info: `Returns the length of the given geometry.

Note ST_Length is only valid for LineString - use ST_Perimeter for Polygon.`,
					libraryUsage: usesGEOS,
				},
				tree.VolatilityImmutable,
			),
		)...,
	),
	"st_perimeter": makeBuiltin(
		defProps(),
		append(
			geographyOverload1WithUseSpheroid(
				func(ctx *tree.EvalContext, g *tree.DGeography, useSphereOrSpheroid geogfn.UseSphereOrSpheroid) (tree.Datum, error) {
					ret, err := geogfn.Perimeter(g.Geography, useSphereOrSpheroid)
					if err != nil {
						return nil, err
					}
					return tree.NewDFloat(tree.DFloat(ret)), nil
				},
				types.Float,
				infoBuilder{
					info: "Returns the perimeter of the given geography in meters.",
				},
				tree.VolatilityImmutable,
			),
			geometryOverload1(
				func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
					ret, err := geomfn.Perimeter(g.Geometry)
					if err != nil {
						return nil, err
					}
					return tree.NewDFloat(tree.DFloat(ret)), nil
				},
				types.Float,
				infoBuilder{
					info: `Returns the perimeter of the given geometry in meters.

Note ST_Perimeter is only valid for Polygon - use ST_Length for LineString.`,
					libraryUsage: usesGEOS,
				},
				tree.VolatilityImmutable,
			),
		)...,
	),
	"st_srid": makeBuiltin(
		defProps(),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(g.SRID())), nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the Spatial Reference Identifier (SRID) for the ST_Geography as defined in spatial_ref_sys table.",
			},
			tree.VolatilityImmutable,
		),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(g.SRID())), nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the Spatial Reference Identifier (SRID) for the ST_Geometry as defined in spatial_ref_sys table.",
			},
			tree.VolatilityImmutable,
		),
	),

	//
	// Binary functions
	//
	"st_distance": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.MinDistance(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(ret)), nil
			},
			types.Float,
			infoBuilder{
				info: `Returns the distance between the given geometries.`,
			},
			tree.VolatilityImmutable,
		),
		geographyOverload2(
			func(ctx *tree.EvalContext, a *tree.DGeography, b *tree.DGeography) (tree.Datum, error) {
				ret, err := geogfn.Distance(a.Geography, b.Geography, geogfn.UseSpheroid)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(ret)), nil
			},
			types.Float,
			infoBuilder{
				info:         "Returns the distance in meters between geography_a and geography_b. " + usesSpheroidMessage + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib,
				canUseIndex:  true,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography_a", types.Geography},
				{"geography_b", types.Geography},
				{"use_spheroid", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeography)
				b := args[1].(*tree.DGeography)
				useSpheroid := args[2].(*tree.DBool)

				ret, err := geogfn.Distance(a.Geography, b.Geography, toUseSphereOrSpheroid(useSpheroid))
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(ret)), nil
			},
			Info: infoBuilder{
				info:         "Returns the distance in meters between geography_a and geography_b." + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib | usesS2,
				canUseIndex:  true,
			}.String(),
			Volatility: tree.VolatilityImmutable,
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
				info:         "Returns true if no point in geometry_b is outside geometry_a.",
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
		geographyOverload2BinaryPredicate(
			geogfn.Covers,
			infoBuilder{
				info:         `Returns true if no point in geography_b is outside geography_a.`,
				libraryUsage: usesS2,
				canUseIndex:  true,
			},
		),
	),
	"st_coveredby": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.CoveredBy,
			infoBuilder{
				info:         `Returns true if no point in geometry_a is outside geometry_b`,
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
		geographyOverload2BinaryPredicate(
			geogfn.CoveredBy,
			infoBuilder{
				info:         `Returns true if no point in geography_a is outside geography_b.`,
				libraryUsage: usesS2,
				precision:    "1cm",
				canUseIndex:  true,
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
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
	),
	"st_containsproperly": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.ContainsProperly,
			infoBuilder{
				info:         "Returns true if geometry_b intersects the interior of geometry_a but not the boundary or exterior of geometry_a.",
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
	),
	"st_crosses": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Crosses,
			infoBuilder{
				info:         "Returns true if geometry_a has some - but not all - interior points in common with geometry_b.",
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
	),
	"st_dwithin": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeometry)
				b := args[1].(*tree.DGeometry)
				dist := args[2].(*tree.DFloat)
				ret, err := geomfn.DWithin(a.Geometry, b.Geometry, float64(*dist))
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if any of geometry_a is within distance units of geometry_b.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography_a", types.Geography},
				{"geography_b", types.Geography},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeography)
				b := args[1].(*tree.DGeography)
				dist := args[2].(*tree.DFloat)
				ret, err := geogfn.DWithin(a.Geography, b.Geography, float64(*dist), geogfn.UseSpheroid)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info:         "Returns true if any of geography_a is within distance meters of geography_b." + usesSpheroidMessage + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib,
				precision:    "1cm",
				canUseIndex:  true,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography_a", types.Geography},
				{"geography_b", types.Geography},
				{"distance", types.Float},
				{"use_spheroid", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeography)
				b := args[1].(*tree.DGeography)
				dist := args[2].(*tree.DFloat)
				useSpheroid := args[3].(*tree.DBool)

				ret, err := geogfn.DWithin(a.Geography, b.Geography, float64(*dist), toUseSphereOrSpheroid(useSpheroid))
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info:         "Returns true if any of geography_a is within distance meters of geography_b." + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib | usesS2,
				precision:    "1cm",
				canUseIndex:  true,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_equals": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Equals,
			infoBuilder{
				info: "Returns true if geometry_a is spatially equal to geometry_b, " +
					"i.e. ST_Within(geometry_a, geometry_b) = ST_Within(geometry_b, geometry_a) = true.",
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
	),
	"st_intersects": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Intersects,
			infoBuilder{
				info:         "Returns true if geometry_a shares any portion of space with geometry_b.",
				libraryUsage: usesGEOS,
				precision:    "1cm",
				canUseIndex:  true,
			},
		),
		geographyOverload2BinaryPredicate(
			geogfn.Intersects,
			infoBuilder{
				info:         `Returns true if geography_a shares any portion of space with geography_b.`,
				libraryUsage: usesS2,
				precision:    "1cm",
				canUseIndex:  true,
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
				libraryUsage: usesGEOS,
				canUseIndex:  true,
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
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
	),
	"st_within": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Within,
			infoBuilder{
				info:         "Returns true if geometry_a is completely inside geometry_b.",
				libraryUsage: usesGEOS,
				canUseIndex:  true,
			},
		),
	),

	//
	// DE-9IM related
	//

	"st_relate": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a *tree.DGeometry, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.Relate(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			types.String,
			infoBuilder{
				info:         `Returns the DE-9IM spatial relation between geometry_a and geometry_b.`,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"pattern", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeometry)
				b := args[1].(*tree.DGeometry)
				pattern := args[2].(*tree.DString)
				relation, err := geomfn.Relate(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				ret, err := geomfn.MatchesDE9IM(relation, string(*pattern))
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info:         `Returns whether the DE-9IM spatial relation between geometry_a and geometry_b matches the DE-9IM pattern.`,
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Transformations
	//
	"st_setsrid": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				srid := args[1].(*tree.DInt)
				newGeom, err := g.Geometry.CloneWithSRID(geopb.SRID(*srid))
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: newGeom}, nil
			},
			Info: infoBuilder{
				info: `Sets a Geometry to a new SRID without transforming the coordinates.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				srid := args[1].(*tree.DInt)
				newGeom, err := g.Geography.CloneWithSRID(geopb.SRID(*srid))
				if err != nil {
					return nil, err
				}
				return &tree.DGeography{Geography: newGeom}, nil
			},
			Info: infoBuilder{
				info: `Sets a Geography to a new SRID without transforming the coordinates.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
}

// geometryOverload1 hides the boilerplate for builtins operating on one geometry.
func geometryOverload1(
	f func(*tree.EvalContext, *tree.DGeometry) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
	volatility tree.Volatility,
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
		Info:       ib.String(),
		Volatility: volatility,
	}
}

// geometryOverload2 hides the boilerplate for builtins operating on two geometries.
func geometryOverload2(
	f func(*tree.EvalContext, *tree.DGeometry, *tree.DGeometry) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
	volatility tree.Volatility,
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
		Info:       ib.String(),
		Volatility: volatility,
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
		tree.VolatilityImmutable,
	)
}

// geographyOverload1 hides the boilerplate for builtins operating on one geography.
func geographyOverload1(
	f func(*tree.EvalContext, *tree.DGeography) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
	volatility tree.Volatility,
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
		Info:       ib.String(),
		Volatility: volatility,
	}
}

// geographyOverload1WithUseSpheroid hides the boilerplate for builtins operating on one geography
// with an optional spheroid argument.
func geographyOverload1WithUseSpheroid(
	f func(*tree.EvalContext, *tree.DGeography, geogfn.UseSphereOrSpheroid) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
	volatility tree.Volatility,
) []tree.Overload {
	infoWithSphereAndSpheroid := ib
	infoWithSphereAndSpheroid.libraryUsage = usesS2 | usesGeographicLib
	infoWithSpheroid := ib
	infoWithSpheroid.info += usesSpheroidMessage
	infoWithSpheroid.libraryUsage = usesGeographicLib

	return []tree.Overload{
		{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
			},
			ReturnType: tree.FixedReturnType(returnType),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeography)
				return f(ctx, a, geogfn.UseSpheroid)
			},
			Info:       infoWithSpheroid.String(),
			Volatility: volatility,
		},
		{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"use_spheroid", types.Bool},
			},
			ReturnType: tree.FixedReturnType(returnType),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := args[0].(*tree.DGeography)
				b := args[1].(*tree.DBool)
				return f(ctx, a, toUseSphereOrSpheroid(b))
			},
			Info:       infoWithSphereAndSpheroid.String(),
			Volatility: volatility,
		},
	}
}

// geographyOverload2 hides the boilerplate for builtins operating on two geographys.
func geographyOverload2(
	f func(*tree.EvalContext, *tree.DGeography, *tree.DGeography) (tree.Datum, error),
	returnType *types.T,
	ib infoBuilder,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types: tree.ArgTypes{
			{"geography_a", types.Geography},
			{"geography_b", types.Geography},
		},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			a := args[0].(*tree.DGeography)
			b := args[1].(*tree.DGeography)
			return f(ctx, a, b)
		},
		Info:       ib.String(),
		Volatility: volatility,
	}
}

// geographyOverload2 hides the boilerplate for builtins operating on two geographys
// and the overlap wraps a binary predicate.
func geographyOverload2BinaryPredicate(
	f func(*geo.Geography, *geo.Geography) (bool, error), ib infoBuilder,
) tree.Overload {
	return geographyOverload2(
		func(_ *tree.EvalContext, a *tree.DGeography, b *tree.DGeography) (tree.Datum, error) {
			ret, err := f(a.Geography, b.Geography)
			if err != nil {
				return nil, err
			}
			return tree.MakeDBool(tree.DBool(ret)), nil
		},
		types.Bool,
		ib,
		tree.VolatilityImmutable,
	)
}

// toUseSphereOrSpheroid returns whether to use a sphere or spheroid.
func toUseSphereOrSpheroid(useSpheroid *tree.DBool) geogfn.UseSphereOrSpheroid {
	if *useSpheroid {
		return geogfn.UseSpheroid
	}
	return geogfn.UseSphere
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
