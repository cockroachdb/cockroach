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
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/geo/geotransform"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s1"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// libraryUsage is a masked bit indicating which libraries are used by
// a given geospatial builtin.
type libraryUsage uint64

const (
	usesGEOS libraryUsage = (1 << (iota + 1))
	usesS2
	usesGeographicLib
	usesPROJ
)

const usesSpheroidMessage = " Uses a spheroid to perform the operation."
const spheroidDistanceMessage = `"\n\nWhen operating on a spheroid, this function will use the sphere to calculate ` +
	`the closest two points using S2. The spheroid distance between these two points is calculated using GeographicLib. ` +
	`This follows observed PostGIS behavior.`

const (
	defaultGeoJSONDecimalDigits = 9
	defaultWKTDecimalDigits     = 15
)

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
	if ib.libraryUsage&usesPROJ != 0 {
		sb.WriteString("\n\nThis function utilizes the PROJ library for coordinate projections.")
	}
	if ib.canUseIndex {
		sb.WriteString("\n\nThis function will automatically use any available index.")
	}
	return sb.String()
}

// geometryFromText is the builtin for ST_GeomFromText/ST_GeometryFromText.
var geometryFromText = makeBuiltin(
	defProps(),
	geomFromWKTOverload,
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

// geomFromWKTOverload converts an (E)WKT string to its Geometry form.
var geomFromWKTOverload = stringOverload1(
	func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
		if err != nil {
			return nil, err
		}
		// TODO(otan): to truly have PostGIS behavior, this needs a warning if it starts with SRID=.
		return tree.NewDGeometry(g), nil
	},
	types.Geometry,
	infoBuilder{info: "Returns the Geometry from a WKT or EWKT representation."}.String(),
	tree.VolatilityImmutable,
)

// geomFromWKBOverload converts a WKB bytea to its Geometry form.
var geomFromWKBOverload = bytesOverload1(
	func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		g, err := geo.ParseGeometryFromEWKB([]byte(s))
		if err != nil {
			return nil, err
		}
		return tree.NewDGeometry(g), nil
	},
	types.Geometry,
	infoBuilder{info: "Returns the Geometry from a WKB (or EWKB) representation."}.String(),
	tree.VolatilityImmutable,
)

// geometryFromTextCheckShapeBuiltin is used for the ST_<Shape>FromText builtins.
func geometryFromTextCheckShapeBuiltin(shapeType geopb.ShapeType) builtinDefinition {
	return makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromEWKT(geopb.EWKT(s), geopb.DefaultGeometrySRID, geo.DefaultSRIDIsHint)
				if err != nil {
					return nil, err
				}
				if g.ShapeType() != shapeType {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the Geometry from a WKT or EWKT representation. If the shape underneath is not %s, NULL is returned.",
					shapeType.String(),
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
				if g.ShapeType() != shapeType {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					`Returns the Geometry from a WKT or EWKT representation with an SRID. If the shape underneath is not %s, NULL is returned. If the SRID is present in both the EWKT and the argument, the argument value is used.`,
					shapeType.String(),
				),
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	)
}

// geometryFromWKBCheckShapeBuiltin is used for the ST_<Shape>FromWKB builtins.
func geometryFromWKBCheckShapeBuiltin(shapeType geopb.ShapeType) builtinDefinition {
	return makeBuiltin(
		defProps(),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromEWKB(geopb.EWKB(s))
				if err != nil {
					return nil, err
				}
				if g.ShapeType() != shapeType {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the Geometry from a WKB (or EWKB) representation. If the shape underneath is not %s, NULL is returned.",
					shapeType.String(),
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
				g, err := geo.ParseGeometryFromEWKBAndSRID(geopb.EWKB(s), srid)
				if err != nil {
					return nil, err
				}
				if g.ShapeType() != shapeType {
					return tree.DNull, nil
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					`Returns the Geometry from a WKB (or EWKB) representation with an SRID. If the shape underneath is not %s, NULL is returned.`,
					shapeType.String(),
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

var numInteriorRingsBuiltin = makeBuiltin(
	defProps(),
	geometryOverload1(
		func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
			t, err := g.Geometry.AsGeomT()
			if err != nil {
				return nil, err
			}
			switch t := t.(type) {
			case *geom.Polygon:
				numRings := t.NumLinearRings()
				if numRings <= 1 {
					return tree.NewDInt(0), nil
				}
				return tree.NewDInt(tree.DInt(numRings - 1)), nil
			}
			return tree.DNull, nil
		},
		types.Int,
		infoBuilder{
			info: "Returns the number of interior rings in a Polygon Geometry. Returns NULL if the shape is not a Polygon.",
		},
		tree.VolatilityImmutable,
	),
)

var areaOverloadGeometry1 = geometryOverload1(
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
)

var stBufferInfoBuilder = infoBuilder{
	info: `Returns a Geometry that represents all points whose distance is less than or equal to the given distance
from the given Geometry.`,
	libraryUsage: usesGEOS,
}

var stBufferWithParamsInfoBuilder = infoBuilder{
	info: `Returns a Geometry that represents all points whose distance is less than or equal to the given distance from the
given Geometry.

This variant takes in a space separate parameter string, which will augment the buffer styles. Valid parameters are:
* quad_segs=<int>, default 8
* endcap=<round|flat|butt|square>, default round
* join=<round|mitre|miter|bevel>, default round
* side=<both|left|right>, default both
* mitre_limit=<float>, default 5.0`,
	libraryUsage: usesGEOS,
}

var stBufferWithQuadSegInfoBuilder = infoBuilder{
	info: `Returns a Geometry that represents all points whose distance is less than or equal to the given distance from the
given Geometry.

This variant approximates the circle into quad_seg segments per line (the default is 8).`,
	libraryUsage: usesGEOS,
}

// fitMaxDecimalDigitsToBounds ensures maxDecimalDigits falls within the bounds that
// is permitted by strconv.FormatFloat.
func fitMaxDecimalDigitsToBounds(maxDecimalDigits int) int {
	if maxDecimalDigits < -1 {
		return -1
	}
	if maxDecimalDigits > 64 {
		return 64
	}
	return maxDecimalDigits
}

var geoBuiltins = map[string]builtinDefinition{
	//
	// Meta builtins.
	//
	"postgis_addbbox": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return g, nil
			},
			types.Geometry,
			infoBuilder{
				info: "Compatibility placeholder function with PostGIS. This does not perform any operation on the Geometry.",
			},
			tree.VolatilityImmutable,
		),
	),
	"postgis_dropbbox": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return g, nil
			},
			types.Geometry,
			infoBuilder{
				info: "Compatibility placeholder function with PostGIS. This does not perform any operation on the Geometry.",
			},
			tree.VolatilityImmutable,
		),
	),
	"postgis_hasbbox": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				if g.Geometry.Empty() {
					return tree.DBoolFalse, nil
				}
				if g.Geometry.ShapeType() == geopb.ShapeType_Point {
					return tree.DBoolFalse, nil
				}
				return tree.DBoolTrue, nil
			},
			types.Bool,
			infoBuilder{
				info: "Returns whether a given Geometry has a bounding box. False for points and empty geometries; always true otherwise.",
			},
			tree.VolatilityImmutable,
		),
	),
	"postgis_extensions_upgrade": returnCompatibilityFixedStringBuiltin(
		"Upgrade completed, run SELECT postgis_full_version(); for details",
	),
	"postgis_full_version": returnCompatibilityFixedStringBuiltin(
		`POSTGIS="3.0.1 ec2a9aa" [EXTENSION] PGSQL="120" GEOS="3.8.1-CAPI-1.13.3" PROJ="4.9.3" LIBXML="2.9.10" LIBJSON="0.13.1" LIBPROTOBUF="1.4.2" WAGYU="0.4.3 (Internal)"`,
	),
	"postgis_geos_version":       returnCompatibilityFixedStringBuiltin("3.8.1-CAPI-1.13.3"),
	"postgis_libxml_version":     returnCompatibilityFixedStringBuiltin("2.9.10"),
	"postgis_lib_build_date":     returnCompatibilityFixedStringBuiltin("2020-03-06 18:23:24"),
	"postgis_lib_version":        returnCompatibilityFixedStringBuiltin("3.0.1"),
	"postgis_liblwgeom_version":  returnCompatibilityFixedStringBuiltin("3.0.1 ec2a9aa"),
	"postgis_proj_version":       returnCompatibilityFixedStringBuiltin("4.9.3"),
	"postgis_scripts_build_date": returnCompatibilityFixedStringBuiltin("2020-02-24 13:54:19"),
	"postgis_scripts_installed":  returnCompatibilityFixedStringBuiltin("3.0.1 ec2a9aa"),
	"postgis_scripts_released":   returnCompatibilityFixedStringBuiltin("3.0.1 ec2a9aa"),
	"postgis_version":            returnCompatibilityFixedStringBuiltin("3.0 USE_GEOS=1 USE_PROJ=1 USE_STATS=1"),
	"postgis_wagyu_version":      returnCompatibilityFixedStringBuiltin("0.4.3 (Internal)"),

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
	"st_wkbtosql": makeBuiltin(defProps(), geomFromWKBOverload),
	"st_wkttosql": makeBuiltin(defProps(), geomFromWKTOverload),
	"st_geomfromwkb": makeBuiltin(
		defProps(),
		geomFromWKBOverload,
		tree.Overload{
			Types:      tree.ArgTypes{{"bytes", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseGeometryFromEWKBAndSRID(geopb.EWKB(b), srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: `Returns the Geometry from a WKB (or EWKB) representation with the given SRID set.`,
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

	"st_geomcollfromtext":        geometryFromTextCheckShapeBuiltin(geopb.ShapeType_GeometryCollection),
	"st_geomcollfromwkb":         geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_GeometryCollection),
	"st_linefromtext":            geometryFromTextCheckShapeBuiltin(geopb.ShapeType_LineString),
	"st_linefromwkb":             geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_LineString),
	"st_linestringfromtext":      geometryFromTextCheckShapeBuiltin(geopb.ShapeType_LineString), // missing from PostGIS
	"st_linestringfromwkb":       geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_LineString),
	"st_mlinefromtext":           geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiLineString),
	"st_mlinefromwkb":            geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiLineString),
	"st_mpointfromtext":          geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiPoint),
	"st_mpointfromwkb":           geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiPoint),
	"st_mpolyfromtext":           geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiPolygon),
	"st_mpolyfromwkb":            geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiPolygon),
	"st_multilinefromtext":       geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiLineString), // missing from PostGIS
	"st_multilinefromwkb":        geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiLineString),
	"st_multilinestringfromtext": geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiLineString),
	"st_multilinestringfromwkb":  geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiLineString), // missing from PostGIS
	"st_multipointfromtext":      geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiPoint),     // SRID version missing from PostGIS
	"st_multipointfromwkb":       geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiPoint),
	"st_multipolyfromtext":       geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiPolygon), // missing from PostGIS
	"st_multipolyfromwkb":        geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiPolygon),
	"st_multipolygonfromtext":    geometryFromTextCheckShapeBuiltin(geopb.ShapeType_MultiPolygon),
	"st_multipolygonfromwkb":     geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_MultiPolygon), // missing from PostGIS
	"st_pointfromtext":           geometryFromTextCheckShapeBuiltin(geopb.ShapeType_Point),
	"st_pointfromwkb":            geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_Point),
	"st_polyfromtext":            geometryFromTextCheckShapeBuiltin(geopb.ShapeType_Polygon),
	"st_polyfromwkb":             geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_Polygon),
	"st_polygonfromtext":         geometryFromTextCheckShapeBuiltin(geopb.ShapeType_Polygon),
	"st_polygonfromwkb":          geometryFromWKBCheckShapeBuiltin(geopb.ShapeType_Polygon),

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
				g, err := geo.ParseGeographyFromEWKB([]byte(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			types.Geography,
			infoBuilder{info: "Returns the Geography from a WKB (or EWKB) representation."}.String(),
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"bytes", types.Bytes}, {"srid", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := string(tree.MustBeDBytes(args[0]))
				srid := geopb.SRID(tree.MustBeDInt(args[1]))
				g, err := geo.ParseGeographyFromEWKBAndSRID(geopb.EWKB(b), srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(g), nil
			},
			Info: infoBuilder{
				info: `Returns the Geography from a WKB (or EWKB) representation with the given SRID set.`,
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
				wkt, err := geo.EWKBToWKT(g.Geometry.EWKB(), defaultWKTDecimalDigits)
				return tree.NewDString(string(wkt)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf("Returns the WKT representation of a given Geometry. A maximum of %d decimal digits is used.", defaultWKTDecimalDigits),
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				wkt, err := geo.EWKBToWKT(g.Geometry.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
				return tree.NewDString(string(wkt)), err
			},
			Info: infoBuilder{
				info: "Returns the WKT representation of a given Geometry. The max_decimal_digits parameter controls the maximum decimal digits to print after the `.`. Use -1 to print as many digits as required to rebuild the same number.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkt, err := geo.EWKBToWKT(g.Geography.EWKB(), defaultWKTDecimalDigits)
				return tree.NewDString(string(wkt)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf("Returns the WKT representation of a given Geography. A default of %d decimal digits is used.", defaultWKTDecimalDigits),
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				wkt, err := geo.EWKBToWKT(g.Geography.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
				return tree.NewDString(string(wkt)), err
			},
			Info: infoBuilder{
				info: "Returns the WKT representation of a given Geography. The max_decimal_digits parameter controls the maximum decimal digits to print after the `.`. Use -1 to print as many digits as required to rebuild the same number.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_asewkt": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ewkt, err := geo.EWKBToEWKT(g.Geometry.EWKB(), defaultWKTDecimalDigits)
				return tree.NewDString(string(ewkt)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf("Returns the EWKT representation of a given Geometry. A maximum of %d decimal digits is used.", defaultWKTDecimalDigits),
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				ewkt, err := geo.EWKBToEWKT(g.Geometry.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
				return tree.NewDString(string(ewkt)), err
			},
			Info: infoBuilder{
				info: "Returns the WKT representation of a given Geometry. The max_decimal_digits parameter controls the maximum decimal digits to print after the `.`. Use -1 to print as many digits as required to rebuild the same number.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				ewkt, err := geo.EWKBToEWKT(g.Geography.EWKB(), defaultWKTDecimalDigits)
				return tree.NewDString(string(ewkt)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf("Returns the EWKT representation of a given Geography. A default of %d decimal digits is used.", defaultWKTDecimalDigits),
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				ewkt, err := geo.EWKBToEWKT(g.Geography.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
				return tree.NewDString(string(ewkt)), err
			},
			Info: infoBuilder{
				info: "Returns the EWKT representation of a given Geography. The max_decimal_digits parameter controls the maximum decimal digits to print after the `.`. Use -1 to print as many digits as required to rebuild the same number.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_asbinary": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				wkb, err := geo.EWKBToWKB(g.Geometry.EWKB(), geo.DefaultEWKBEncodingFormat)
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			types.Bytes,
			infoBuilder{info: "Returns the WKB representation of a given Geometry."},
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkb, err := geo.EWKBToWKB(g.Geography.EWKB(), geo.DefaultEWKBEncodingFormat)
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			types.Bytes,
			infoBuilder{info: "Returns the WKB representation of a given Geography."},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"xdr_or_ndr", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				text := string(tree.MustBeDString(args[1]))

				wkb, err := geo.EWKBToWKB(g.Geometry.EWKB(), geo.StringToByteOrder(text))
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			Info: infoBuilder{
				info: "Returns the WKB representation of a given Geometry. " +
					"This variant has a second argument denoting the encoding - `xdr` for big endian and `ndr` for little endian.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"xdr_or_ndr", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				text := string(tree.MustBeDString(args[1]))

				wkb, err := geo.EWKBToWKB(g.Geography.EWKB(), geo.StringToByteOrder(text))
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			Info: infoBuilder{
				info: "Returns the WKB representation of a given Geography. " +
					"This variant has a second argument denoting the encoding - `xdr` for big endian and `ndr` for little endian.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
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
				return tree.NewDString(g.Geometry.EWKBHex()), nil
			},
			types.String,
			infoBuilder{info: "Returns the EWKB representation in hex of a given Geometry."},
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				return tree.NewDString(g.Geography.EWKBHex()), nil
			},
			types.String,
			infoBuilder{info: "Returns the EWKB representation in hex of a given Geography."},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"xdr_or_ndr", types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				text := string(tree.MustBeDString(args[1]))

				byteOrder := geo.StringToByteOrder(text)
				if byteOrder == geo.DefaultEWKBEncodingFormat {
					return tree.NewDString(fmt.Sprintf("%X", g.EWKB())), nil
				}

				geomT, err := g.AsGeomT()
				if err != nil {
					return nil, err
				}

				b, err := ewkb.Marshal(geomT, byteOrder)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(fmt.Sprintf("%X", b)), nil
			},
			Info: infoBuilder{
				info: "Returns the EWKB representation in hex of a given Geometry. " +
					"This variant has a second argument denoting the encoding - `xdr` for big endian and `ndr` for little endian.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"xdr_or_ndr", types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				text := string(tree.MustBeDString(args[1]))

				byteOrder := geo.StringToByteOrder(text)
				if byteOrder == geo.DefaultEWKBEncodingFormat {
					return tree.NewDString(fmt.Sprintf("%X", g.EWKB())), nil
				}

				geomT, err := g.AsGeomT()
				if err != nil {
					return nil, err
				}

				b, err := ewkb.Marshal(geomT, byteOrder)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(fmt.Sprintf("%X", b)), nil
			},
			Info: infoBuilder{
				info: "Returns the EWKB representation in hex of a given Geography. " +
					"This variant has a second argument denoting the encoding - `xdr` for big endian and `ndr` for little endian.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
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
				geojson, err := geo.EWKBToGeoJSON(g.Geometry.EWKB(), defaultGeoJSONDecimalDigits, geo.EWKBToGeoJSONFlagShortCRSIfNot4326)
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geometry. Coordinates have a maximum of %d decimal digits.",
					defaultGeoJSONDecimalDigits,
				),
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				geojson, err := geo.EWKBToGeoJSON(g.Geometry.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), geo.EWKBToGeoJSONFlagShortCRSIfNot4326)
				return tree.NewDString(string(geojson)), err
			},
			Info: infoBuilder{
				info: `Returns the GeoJSON representation of a given Geometry with max_decimal_digits output for each coordinate value.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"max_decimal_digits", types.Int},
				{"options", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				options := geo.EWKBToGeoJSONFlag(tree.MustBeDInt(args[2]))
				geojson, err := geo.EWKBToGeoJSON(g.Geometry.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), options)
				return tree.NewDString(string(geojson)), err
			},
			Info: infoBuilder{
				info: `Returns the GeoJSON representation of a given Geometry with max_decimal_digits output for each coordinate value.

Options is a flag that can be bitmasked. The options are:
* 0: no option
* 1: GeoJSON BBOX
* 2: GeoJSON Short CRS (e.g EPSG:4326)
* 4: GeoJSON Long CRS (e.g urn:ogc:def:crs:EPSG::4326)
* 8: GeoJSON Short CRS if not EPSG:4326 (default for Geometry)
`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				geojson, err := geo.EWKBToGeoJSON(g.Geography.EWKB(), defaultGeoJSONDecimalDigits, geo.EWKBToGeoJSONFlagZero)
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geography. Coordinates have a maximum of %d decimal digits.",
					defaultGeoJSONDecimalDigits,
				),
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				geojson, err := geo.EWKBToGeoJSON(g.Geography.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), geo.EWKBToGeoJSONFlagZero)
				return tree.NewDString(string(geojson)), err
			},
			Info: infoBuilder{
				info: `Returns the GeoJSON representation of a given Geography with max_decimal_digits output for each coordinate value.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"max_decimal_digits", types.Int},
				{"options", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				options := geo.EWKBToGeoJSONFlag(tree.MustBeDInt(args[2]))
				geojson, err := geo.EWKBToGeoJSON(g.Geography.EWKB(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), options)
				return tree.NewDString(string(geojson)), err
			},
			Info: infoBuilder{
				info: `Returns the GeoJSON representation of a given Geography with max_decimal_digits output for each coordinate value.

Options is a flag that can be bitmasked. The options are:
* 0: no option (default for Geography)
* 1: GeoJSON BBOX
* 2: GeoJSON Short CRS (e.g EPSG:4326)
* 4: GeoJSON Long CRS (e.g urn:ogc:def:crs:EPSG::4326)
* 8: GeoJSON Short CRS if not EPSG:4326
`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_project": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"distance", types.Float},
				{"azimuth", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				distance := float64(*args[1].(*tree.DFloat))
				azimuth := float64(*args[2].(*tree.DFloat))

				geog, err := geogfn.Project(g.Geography, distance, s1.Angle(azimuth))
				if err != nil {
					return nil, err
				}

				return &tree.DGeography{Geography: geog}, nil
			},
			Info: infoBuilder{
				info: `Returns a point projected from a start point along a geodesic using a given distance and azimuth (bearing).
This is known as the direct geodesic problem.

The distance is given in meters. Negative values are supported.

The azimuth (also known as heading or bearing) is given in radians. It is measured clockwise from true north (azimuth zero).
East is azimuth π/2 (90 degrees); south is azimuth π (180 degrees); west is azimuth 3π/2 (270 degrees).
Negative azimuth values and values greater than 2π (360 degrees) are supported.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Unary functions.
	//

	"st_ndims": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(t.Stride())), nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of dimensions of a given Geometry.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_startpoint": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.LineString:
					if t.Empty() {
						return tree.DNull, nil
					}
					coord := t.Coord(0)
					retG, err := geo.NewGeometryFromGeom(
						geom.NewPointFlat(geom.XY, []float64{coord.X(), coord.Y()}).SetSRID(t.SRID()),
					)
					if err != nil {
						return nil, err
					}
					return &tree.DGeometry{Geometry: retG}, nil
				}
				return tree.DNull, nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns the first point of a geometry which has shape LineString. Returns NULL if the geometry is not a LineString.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_summary": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.AsGeomT()
				if err != nil {
					return nil, err
				}

				summary, err := geo.Summary(t, g.SpatialObject().BoundingBox != nil, g.ShapeType(), false)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(summary), nil
			},
			types.String,
			infoBuilder{
				info: `Returns a text summary of the contents of the geometry.

Flags shown square brackets after the geometry type have the following meaning:
* M: has M coordinate
* Z: has Z coordinate
* B: has a cached bounding box
* G: is geography
* S: has spatial reference system
`,
			},
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				t, err := g.AsGeomT()
				if err != nil {
					return nil, err
				}

				summary, err := geo.Summary(t, g.SpatialObject().BoundingBox != nil, g.ShapeType(), true)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(summary), nil
			},
			types.String,
			infoBuilder{
				info: `Returns a text summary of the contents of the geography.

Flags shown square brackets after the geometry type have the following meaning:
* M: has M coordinate
* Z: has Z coordinate
* B: has a cached bounding box
* G: is geography
* S: has spatial reference system
`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_endpoint": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.LineString:
					if t.Empty() {
						return tree.DNull, nil
					}
					coord := t.Coord(t.NumCoords() - 1)
					retG, err := geo.NewGeometryFromGeom(
						geom.NewPointFlat(geom.XY, []float64{coord.X(), coord.Y()}).SetSRID(t.SRID()),
					)
					if err != nil {
						return nil, err
					}
					return &tree.DGeometry{Geometry: retG}, nil
				}
				return tree.DNull, nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns the last point of a geometry which has shape LineString. Returns NULL if the geometry is not a LineString.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_numpoints": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.LineString:
					return tree.NewDInt(tree.DInt(t.NumCoords())), nil
				}
				return tree.DNull, nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of points in a LineString. Returns NULL if the Geometry is not a LineString.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_npoints": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.GeometryCollection:
					// FlatCoords() does not work on GeometryCollection.
					numPoints := 0
					for _, g := range t.Geoms() {
						numPoints += len(g.FlatCoords()) / g.Stride()
					}
					return tree.NewDInt(tree.DInt(numPoints)), nil
				default:
					return tree.NewDInt(tree.DInt(len(t.FlatCoords()) / t.Stride())), nil
				}
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of points in a given Geometry. Works for any shape type.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_exteriorring": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Polygon:
					var lineString *geom.LineString
					if t.Empty() {
						lineString = geom.NewLineString(t.Layout())
					} else {
						ring := t.LinearRing(0)
						lineString = geom.NewLineStringFlat(t.Layout(), ring.FlatCoords())
					}
					ret, err := geo.NewGeometryFromGeom(lineString.SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(ret), nil
				}
				return tree.DNull, nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns the exterior ring of a Polygon as a LineString. Returns NULL if the shape is not a Polygon.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_interiorringn": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"n", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := *args[0].(*tree.DGeometry)
				n := int(*args[1].(*tree.DInt))
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Polygon:
					// N is 1-indexed. Ignore the exterior ring and return NULL for that case.
					if n >= t.NumLinearRings() || n <= 0 {
						return tree.DNull, nil
					}
					ring := t.LinearRing(n)
					lineString := geom.NewLineStringFlat(t.Layout(), ring.FlatCoords()).SetSRID(t.SRID())
					ret, err := geo.NewGeometryFromGeom(lineString)
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(ret), nil
				}
				return tree.DNull, nil
			},
			Info: infoBuilder{
				info: `Returns the n-th (1-indexed) interior ring of a Polygon as a LineString. Returns NULL if the shape is not a Polygon, or the ring does not exist.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_pointn": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"n", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := *args[0].(*tree.DGeometry)
				n := int(*args[1].(*tree.DInt)) - 1
				if n < 0 {
					return tree.DNull, nil
				}
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.LineString:
					if n >= t.NumCoords() {
						return tree.DNull, nil
					}
					g, err := geo.NewGeometryFromGeom(geom.NewPointFlat(t.Layout(), t.Coord(n)).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				}
				return tree.DNull, nil
			},
			Info: infoBuilder{
				info: `Returns the n-th Point of a LineString (1-indexed). Returns NULL if out of bounds or not a LineString.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_geometryn": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"n", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := *args[0].(*tree.DGeometry)
				n := int(*args[1].(*tree.DInt)) - 1
				if n < 0 {
					return tree.DNull, nil
				}
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Point, *geom.Polygon, *geom.LineString:
					if n > 0 {
						return tree.DNull, nil
					}
					return args[0], nil
				case *geom.MultiPoint:
					if n >= t.NumPoints() {
						return tree.DNull, nil
					}
					g, err := geo.NewGeometryFromGeom(t.Point(n).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				case *geom.MultiLineString:
					if n >= t.NumLineStrings() {
						return tree.DNull, nil
					}
					g, err := geo.NewGeometryFromGeom(t.LineString(n).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				case *geom.MultiPolygon:
					if n >= t.NumPolygons() {
						return tree.DNull, nil
					}
					g, err := geo.NewGeometryFromGeom(t.Polygon(n).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				case *geom.GeometryCollection:
					if n >= t.NumGeoms() {
						return tree.DNull, nil
					}
					g, err := geo.NewGeometryFromGeom(t.Geom(n))
					if err != nil {
						return nil, err
					}
					gWithSRID, err := g.CloneWithSRID(geopb.SRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(gWithSRID), nil
				}
				return tree.DNull, nil
			},
			Info: infoBuilder{
				info: `Returns the n-th Geometry (1-indexed). Returns NULL if out of bounds.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_numinteriorring":  numInteriorRingsBuiltin,
	"st_numinteriorrings": numInteriorRingsBuiltin,
	"st_nrings": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Polygon:
					return tree.NewDInt(tree.DInt(t.NumLinearRings())), nil
				}
				return tree.NewDInt(0), nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of rings in a Polygon Geometry. Returns 0 if the shape is not a Polygon.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_numgeometries": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Point:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(1), nil
				case *geom.LineString:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(1), nil
				case *geom.Polygon:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(1), nil
				case *geom.MultiPoint:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(tree.DInt(t.NumPoints())), nil
				case *geom.MultiLineString:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(tree.DInt(t.NumLineStrings())), nil
				case *geom.MultiPolygon:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(tree.DInt(t.NumPolygons())), nil
				case *geom.GeometryCollection:
					if t.Empty() {
						return tree.NewDInt(0), nil
					}
					return tree.NewDInt(tree.DInt(t.NumGeoms())), nil
				}
				return nil, errors.Newf("unknown type: %T", t)
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of shapes inside a given Geometry.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_x": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Point:
					if t.Empty() {
						return tree.DNull, nil
					}
					return tree.NewDFloat(tree.DFloat(t.X())), nil
				}
				// Ideally we should return NULL here, but following PostGIS on this.
				return nil, errors.Newf("argument to ST_X() must have shape POINT")
			},
			types.Float,
			infoBuilder{
				info: "Returns the X coordinate of a geometry if it is a Point.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_y": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Point:
					if t.Empty() {
						return tree.DNull, nil
					}
					return tree.NewDFloat(tree.DFloat(t.Y())), nil
				}
				// Ideally we should return NULL here, but following PostGIS on this.
				return nil, errors.Newf("argument to ST_Y() must have shape POINT")
			},
			types.Float,
			infoBuilder{
				info: "Returns the Y coordinate of a geometry if it is a Point.",
			},
			tree.VolatilityImmutable,
		),
	),
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
			areaOverloadGeometry1,
		)...,
	),
	"st_area2d": makeBuiltin(
		defProps(),
		areaOverloadGeometry1,
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
	"geometrytype": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return tree.NewDString(g.ShapeType().String()), nil
			},
			types.String,
			infoBuilder{
				info:         "Returns the type of geometry as a string.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_geometrytype": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("ST_%s", g.ShapeType().String())), nil
			},
			types.String,
			infoBuilder{
				info:         "Returns the type of geometry as a string prefixed with `ST_`.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_lineinterpolatepoint": makeBuiltin(
		defProps(),
		lineInterpolatePointForRepeatOverload(
			false,
			`Returns a point along the given LineString which is at given fraction of LineString's total length.`,
		),
	),
	"st_lineinterpolatepoints": makeBuiltin(
		defProps(),
		lineInterpolatePointForRepeatOverload(
			true,
			`Returns one or more points along the LineString which is at an integral multiples of `+
				`given fraction of LineString's total length.

Note If the result has zero or one points, it will be returned as a POINT. If it has two or more points, it will be returned as a MULTIPOINT.`,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"fraction", types.Float},
				{"repeat", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				fraction := float64(*args[1].(*tree.DFloat))
				repeat := bool(*args[2].(*tree.DBool))
				interpolatedPoints, err := geomfn.LineInterpolatePoints(g.Geometry, fraction, repeat)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(interpolatedPoints), nil
			},
			Info: infoBuilder{
				info: `Returns one or more points along the LineString which is at an integral multiples of given fraction ` +
					`of LineString's total length. If repeat is false (default true) then it returns first point.

Note If the result has zero or one points, it will be returned as a POINT. If it has two or more points, it will be returned as a MULTIPOINT.`,
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Binary functions
	//
	"st_azimuth": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				azimuth, err := geomfn.Azimuth(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}

				if azimuth == nil {
					return tree.DNull, nil
				}

				return tree.NewDFloat(tree.DFloat(*azimuth)), nil
			},
			types.Float,
			infoBuilder{
				info: `Returns the azimuth in radians of the segment defined by the given point geometries, or NULL if the two points are coincident.

The azimuth is angle is referenced from north, and is positive clockwise: North = 0; East = π/2; South = π; West = 3π/2.`,
			},
			tree.VolatilityImmutable,
		),
		geographyOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeography) (tree.Datum, error) {
				azimuth, err := geogfn.Azimuth(a.Geography, b.Geography)
				if err != nil {
					return nil, err
				}

				if azimuth == nil {
					return tree.DNull, nil
				}

				return tree.NewDFloat(tree.DFloat(*azimuth)), nil
			},
			types.Float,
			infoBuilder{
				info: `Returns the azimuth in radians of the segment defined by the given point geographies, or NULL if the two points are coincident. It is solved using the Inverse geodesic problem.

The azimuth is angle is referenced from north, and is positive clockwise: North = 0; East = π/2; South = π; West = 3π/2.`,
				libraryUsage: usesGeographicLib,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_distance": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.MinDistance(a.Geometry, b.Geometry)
				if err != nil {
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
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
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
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
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
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
	"st_maxdistance": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.MaxDistance(a.Geometry, b.Geometry)
				if err != nil {
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(ret)), nil
			},
			types.Float,
			infoBuilder{
				info: `Returns the maximum distance across every pair of points comprising the given geometries. ` +
					`Note if the geometries are the same, it will return the maximum distance between the geometry's vertexes.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_longestline": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				longestLineString, err := geomfn.LongestLineString(a.Geometry, b.Geometry)
				if err != nil {
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
					return nil, err
				}
				return tree.NewDGeometry(longestLineString), nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns the LineString corresponds to the max distance across every pair of points comprising the ` +
					`given geometries. 

Note if geometries are the same, it will return the LineString with the maximum distance between the geometry's ` +
					`vertexes. The function will return the longest line that was discovered first when comparing maximum ` +
					`distances if more than one is found.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_shortestline": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				shortestLineString, err := geomfn.ShortestLineString(a.Geometry, b.Geometry)
				if err != nil {
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
					return nil, err
				}
				return tree.NewDGeometry(shortestLineString), nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns the LineString corresponds to the minimum distance across every pair of points comprising ` +
					`the given geometries. 

Note if geometries are the same, it will return the LineString with the minimum distance between the geometry's ` +
					`vertexes. The function will return the shortest line that was discovered first when comparing minimum ` +
					`distances if more than one is found.`,
			},
			tree.VolatilityImmutable,
		),
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
	"st_dfullywithin": makeBuiltin(
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
				ret, err := geomfn.DFullyWithin(a.Geometry, b.Geometry, float64(*dist))
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if every pair of points comprising geometry_a and geometry_b are within distance units. " +
					"In other words, the ST_MaxDistance between geometry_a and geometry_b is less than or equal to distance units.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
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
				ret, err := geomfn.RelatePattern(a.Geometry, b.Geometry, string(*pattern))
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

	// Topology operations
	"st_centroid": makeBuiltin(
		defProps(),
		append(
			geographyOverload1WithUseSpheroid(
				func(ctx *tree.EvalContext, g *tree.DGeography, useSphereOrSpheroid geogfn.UseSphereOrSpheroid) (tree.Datum, error) {
					ret, err := geogfn.Centroid(g.Geography, useSphereOrSpheroid)
					if err != nil {
						return nil, err
					}
					return tree.NewDGeography(ret), nil
				},
				types.Geography,
				infoBuilder{
					info: "Returns the centroid of given geography.",
				},
				tree.VolatilityImmutable,
			),
			geometryOverload1(
				func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
					centroid, err := geomfn.Centroid(g.Geometry)
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(centroid), err
				},
				types.Geometry,
				infoBuilder{
					info:         "Returns the centroid of the given geometry.",
					libraryUsage: usesGEOS,
				},
				tree.VolatilityImmutable,
			),
			stringOverload1(
				func(ctx *tree.EvalContext, s string) (tree.Datum, error) {
					g, err := geo.ParseGeometry(s)
					if err != nil {
						return nil, err
					}
					centroid, err := geomfn.Centroid(g)
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(centroid), err
				},
				types.Geometry,
				infoBuilder{
					info:         "Returns the centroid of the given string, which will be parsed as a geometry object.",
					libraryUsage: usesGEOS,
				}.String(),
				tree.VolatilityImmutable,
			),
		)...,
	),
	"st_pointonsurface": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				pointOnSurface, err := geomfn.PointOnSurface(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(pointOnSurface), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns a point that intersects with the given Geometry.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_intersection": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a *tree.DGeometry, b *tree.DGeometry) (tree.Datum, error) {
				intersection, err := geomfn.Intersection(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(intersection), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns the point intersections of the given geometries.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_union": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a *tree.DGeometry, b *tree.DGeometry) (tree.Datum, error) {
				union, err := geomfn.Union(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(union), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns the union of the given geometries as a single Geometry object.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
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
	"st_transform": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				srid := geopb.SRID(*args[1].(*tree.DInt))

				fromProj, exists := geoprojbase.Projection(g.SRID())
				if !exists {
					return nil, errors.Newf("projection for srid %d does not exist", g.SRID())
				}
				toProj, exists := geoprojbase.Projection(srid)
				if !exists {
					return nil, errors.Newf("projection for srid %d does not exist", srid)
				}
				ret, err := geotransform.Transform(g.Geometry, fromProj.Proj4Text, toProj.Proj4Text, srid)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info:         `Transforms a geometry into the given SRID coordinate reference system by projecting its coordinates.`,
				libraryUsage: usesPROJ,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"to_proj_text", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				toProj := string(*args[1].(*tree.DString))

				fromProj, exists := geoprojbase.Projection(g.SRID())
				if !exists {
					return nil, errors.Newf("projection for srid %d does not exist", g.SRID())
				}
				ret, err := geotransform.Transform(
					g.Geometry,
					fromProj.Proj4Text,
					geoprojbase.MakeProj4Text(toProj),
					geopb.SRID(0),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info:         `Transforms a geometry into the coordinate reference system referenced by the projection text by projecting its coordinates.`,
				libraryUsage: usesPROJ,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"from_proj_text", types.String},
				{"to_proj_text", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				fromProj := string(*args[1].(*tree.DString))
				toProj := string(*args[2].(*tree.DString))

				ret, err := geotransform.Transform(
					g.Geometry,
					geoprojbase.MakeProj4Text(fromProj),
					geoprojbase.MakeProj4Text(toProj),
					geopb.SRID(0),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info:         `Transforms a geometry into the coordinate reference system assuming the from_proj_text to the new to_proj_text by projecting its coordinates.`,
				libraryUsage: usesPROJ,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"from_proj_text", types.String},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				fromProj := string(*args[1].(*tree.DString))
				srid := geopb.SRID(*args[2].(*tree.DInt))

				toProj, exists := geoprojbase.Projection(srid)
				if !exists {
					return nil, errors.Newf("projection for srid %d does not exist", srid)
				}
				ret, err := geotransform.Transform(
					g.Geometry,
					geoprojbase.MakeProj4Text(fromProj),
					toProj.Proj4Text,
					srid,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info:         `Transforms a geometry into the coordinate reference system assuming the from_proj_text to the new to_proj_text by projecting its coordinates. The supplied SRID is set on the new geometry.`,
				libraryUsage: usesPROJ,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_segmentize": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"max_segment_length_meters", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeography)
				segmentMaxLength := float64(*args[1].(*tree.DFloat))
				segGeography, err := geogfn.Segmentize(g.Geography, segmentMaxLength)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(segGeography), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geography having no segment longer than the given max_segment_length meters.

The calculations are done on a sphere.`,
				libraryUsage: usesS2,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"max_segment_length", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				segmentMaxLength := float64(*args[1].(*tree.DFloat))
				segGeometry, err := geomfn.Segmentize(g.Geometry, segmentMaxLength)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(segGeometry), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry having no segment longer than the given max_segment_length. ` +
					`Length units are in units of spatial reference.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_buffer": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				distance := *args[1].(*tree.DFloat)

				ret, err := geomfn.Buffer(g.Geometry, geomfn.MakeDefaultBufferParams(), float64(distance))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"distance", types.Float},
				{"quad_segs", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				distance := *args[1].(*tree.DFloat)
				quadSegs := *args[2].(*tree.DInt)

				ret, err := geomfn.Buffer(
					g.Geometry,
					geomfn.MakeDefaultBufferParams().WithQuadrantSegments(int(quadSegs)),
					float64(distance),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferWithQuadSegInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"distance", types.Float},
				{"buffer_style_params", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := args[0].(*tree.DGeometry)
				distance := *args[1].(*tree.DFloat)
				paramsString := *args[2].(*tree.DString)

				params, modifiedDistance, err := geomfn.ParseBufferParams(string(paramsString), float64(distance))
				if err != nil {
					return nil, err
				}

				ret, err := geomfn.Buffer(
					g.Geometry,
					params,
					modifiedDistance,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferWithParamsInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_str", types.String},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				gStr := *args[0].(*tree.DString)
				distance := *args[1].(*tree.DFloat)

				g, err := geo.ParseGeometry(string(gStr))
				if err != nil {
					return nil, err
				}
				ret, err := geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), float64(distance))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_str", types.String},
				// This should be float, but for this to work equivalently to the psql type system,
				// we have to make a decimal definition.
				{"distance", types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				gStr := *args[0].(*tree.DString)
				distance, err := args[1].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}

				g, err := geo.ParseGeometry(string(gStr))
				if err != nil {
					return nil, err
				}
				ret, err := geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), distance)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_str", types.String},
				{"distance", types.Float},
				{"quad_segs", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				gStr := *args[0].(*tree.DString)
				distance := *args[1].(*tree.DFloat)
				quadSegs := *args[2].(*tree.DInt)

				g, err := geo.ParseGeometry(string(gStr))
				if err != nil {
					return nil, err
				}

				ret, err := geomfn.Buffer(
					g,
					geomfn.MakeDefaultBufferParams().WithQuadrantSegments(int(quadSegs)),
					float64(distance),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferWithQuadSegInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_str", types.String},
				// This should be float, but for this to work equivalently to the psql type system,
				// we have to make a decimal definition.
				{"distance", types.Decimal},
				{"quad_segs", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				gStr := *args[0].(*tree.DString)
				distance, err := args[1].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				quadSegs := *args[2].(*tree.DInt)

				g, err := geo.ParseGeometry(string(gStr))
				if err != nil {
					return nil, err
				}

				ret, err := geomfn.Buffer(
					g,
					geomfn.MakeDefaultBufferParams().WithQuadrantSegments(int(quadSegs)),
					distance,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferWithQuadSegInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_str", types.String},
				{"distance", types.Float},
				{"buffer_style_params", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				gStr := *args[0].(*tree.DString)
				distance := *args[1].(*tree.DFloat)
				paramsString := *args[2].(*tree.DString)

				g, err := geo.ParseGeometry(string(gStr))
				if err != nil {
					return nil, err
				}

				params, modifiedDistance, err := geomfn.ParseBufferParams(string(paramsString), float64(distance))
				if err != nil {
					return nil, err
				}

				ret, err := geomfn.Buffer(
					g,
					params,
					modifiedDistance,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferWithParamsInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_str", types.String},
				// This should be float, but for this to work equivalently to the psql type system,
				// we have to make a decimal definition.
				{"distance", types.Decimal},
				{"buffer_style_params", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				gStr := *args[0].(*tree.DString)
				distance, err := args[1].(*tree.DDecimal).Float64()
				if err != nil {
					return nil, err
				}
				paramsString := *args[2].(*tree.DString)

				g, err := geo.ParseGeometry(string(gStr))
				if err != nil {
					return nil, err
				}

				params, modifiedDistance, err := geomfn.ParseBufferParams(string(paramsString), distance)
				if err != nil {
					return nil, err
				}

				ret, err := geomfn.Buffer(
					g,
					params,
					modifiedDistance,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info:       stBufferWithParamsInfoBuilder.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Schema changes
	//
	"addgeometrycolumn": makeBuiltin(
		tree.FunctionProperties{
			Class:    tree.SQLClass,
			Category: categoryGeospatial,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"table_name", types.String},
				{"column_name", types.String},
				{"srid", types.Int},
				{"type", types.String},
				{"dimension", types.Int},
				{"use_typmod", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			SQLFn: func(ctx *tree.EvalContext, args tree.Datums) (string, error) {
				return addGeometryColumnSQL(
					ctx,
					"", /* catalogName */
					"", /* schemaName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					int(tree.MustBeDInt(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					bool(tree.MustBeDBool(args[5])),
				)
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return addGeometryColumnSummary(
					ctx,
					"", /* catalogName */
					"", /* schemaName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					int(tree.MustBeDInt(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					bool(tree.MustBeDBool(args[5])),
				)
			},
			Info: infoBuilder{
				info: `Adds a new geometry column to an existing table and returns metadata about the column created.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"schema_name", types.String},
				{"table_name", types.String},
				{"column_name", types.String},
				{"srid", types.Int},
				{"type", types.String},
				{"dimension", types.Int},
				{"use_typmod", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			SQLFn: func(ctx *tree.EvalContext, args tree.Datums) (string, error) {
				return addGeometryColumnSQL(
					ctx,
					"", /* catalogName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					int(tree.MustBeDInt(args[3])),
					string(tree.MustBeDString(args[4])),
					int(tree.MustBeDInt(args[5])),
					bool(tree.MustBeDBool(args[6])),
				)
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return addGeometryColumnSummary(
					ctx,
					"", /* catalogName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					int(tree.MustBeDInt(args[3])),
					string(tree.MustBeDString(args[4])),
					int(tree.MustBeDInt(args[5])),
					bool(tree.MustBeDBool(args[6])),
				)
			},
			Info: infoBuilder{
				info: `Adds a new geometry column to an existing table and returns metadata about the column created.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"catalog_name", types.String},
				{"schema_name", types.String},
				{"table_name", types.String},
				{"column_name", types.String},
				{"srid", types.Int},
				{"type", types.String},
				{"dimension", types.Int},
				{"use_typmod", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			SQLFn: func(ctx *tree.EvalContext, args tree.Datums) (string, error) {
				return addGeometryColumnSQL(
					ctx,
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					string(tree.MustBeDString(args[5])),
					int(tree.MustBeDInt(args[6])),
					bool(tree.MustBeDBool(args[7])),
				)
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return addGeometryColumnSummary(
					ctx,
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					string(tree.MustBeDString(args[5])),
					int(tree.MustBeDInt(args[6])),
					bool(tree.MustBeDBool(args[7])),
				)
			},
			Info: infoBuilder{
				info: `Adds a new geometry column to an existing table and returns metadata about the column created.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"table_name", types.String},
				{"column_name", types.String},
				{"srid", types.Int},
				{"type", types.String},
				{"dimension", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			SQLFn: func(ctx *tree.EvalContext, args tree.Datums) (string, error) {
				return addGeometryColumnSQL(
					ctx,
					"", /* catalogName */
					"", /* schemaName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					int(tree.MustBeDInt(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					true, /* useTypmod */
				)
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return addGeometryColumnSummary(
					ctx,
					"", /* catalogName */
					"", /* schemaName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					int(tree.MustBeDInt(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					true, /* useTypmod */
				)
			},
			Info: infoBuilder{
				info: `Adds a new geometry column to an existing table and returns metadata about the column created.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"schema_name", types.String},
				{"table_name", types.String},
				{"column_name", types.String},
				{"srid", types.Int},
				{"type", types.String},
				{"dimension", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			SQLFn: func(ctx *tree.EvalContext, args tree.Datums) (string, error) {
				return addGeometryColumnSQL(
					ctx,
					"", /* catalogName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					int(tree.MustBeDInt(args[3])),
					string(tree.MustBeDString(args[4])),
					int(tree.MustBeDInt(args[5])),
					true, /* useTypmod */
				)
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return addGeometryColumnSummary(
					ctx,
					"", /* catalogName */
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					int(tree.MustBeDInt(args[3])),
					string(tree.MustBeDString(args[4])),
					int(tree.MustBeDInt(args[5])),
					true, /* useTypmod */
				)
			},
			Info: infoBuilder{
				info: `Adds a new geometry column to an existing table and returns metadata about the column created.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"catalog_name", types.String},
				{"schema_name", types.String},
				{"table_name", types.String},
				{"column_name", types.String},
				{"srid", types.Int},
				{"type", types.String},
				{"dimension", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			SQLFn: func(ctx *tree.EvalContext, args tree.Datums) (string, error) {
				return addGeometryColumnSQL(
					ctx,
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					string(tree.MustBeDString(args[5])),
					int(tree.MustBeDInt(args[6])),
					true, /* useTypmod */
				)
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return addGeometryColumnSummary(
					ctx,
					string(tree.MustBeDString(args[0])),
					string(tree.MustBeDString(args[1])),
					string(tree.MustBeDString(args[2])),
					string(tree.MustBeDString(args[3])),
					int(tree.MustBeDInt(args[4])),
					string(tree.MustBeDString(args[5])),
					int(tree.MustBeDInt(args[6])),
					true, /* useTypmod */
				)
			},
			Info: infoBuilder{
				info: `Adds a new geometry column to an existing table and returns metadata about the column created.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
	),
}

// returnCompatibilityFixedStringBuiltin is an overload that takes in 0 arguments
// and returns the given fixed string.
// It is assumed to be fully immutable.
func returnCompatibilityFixedStringBuiltin(ret string) builtinDefinition {
	return makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDString(ret), nil
			},
			Info: infoBuilder{
				info: fmt.Sprintf("Compatibility placeholder function with PostGIS. Returns a fixed string based on PostGIS 3.0.1, with minor edits."),
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	)
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

// addGeometryColumnSQL returns the SQL statement that should be executed to
// add a geometry column.
func addGeometryColumnSQL(
	ctx *tree.EvalContext,
	catalogName string,
	schemaName string,
	tableName string,
	columnName string,
	srid int,
	shape string,
	dimension int,
	useTypmod bool,
) (string, error) {
	if dimension != 2 {
		return "", pgerror.Newf(
			pgcode.FeatureNotSupported,
			"only dimension=2 is currently supported",
		)
	}
	if !useTypmod {
		return "", unimplemented.NewWithIssue(
			49402,
			"useTypmod=false is currently not supported with AddGeometryColumn",
		)
	}

	tn := makeTableName(catalogName, schemaName, tableName)
	stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s GEOMETRY(%s,%d)",
		tn.String(),
		columnName,
		shape,
		srid,
	)
	return stmt, nil
}

// addGeometryColumnSummary returns metadata about the geometry column that
// was added.
func addGeometryColumnSummary(
	ctx *tree.EvalContext,
	catalogName string,
	schemaName string,
	tableName string,
	columnName string,
	srid int,
	shape string,
	dimension int,
	useTypmod bool,
) (tree.Datum, error) {
	tn := makeTableName(catalogName, schemaName, tableName)
	summary := fmt.Sprintf("%s.%s SRID:%d TYPE:%s DIMS:%d",
		tn.String(),
		columnName,
		srid,
		strings.ToUpper(shape),
		dimension,
	)
	return tree.NewDString(summary), nil
}

func makeTableName(catalogName string, schemaName string, tableName string) tree.UnresolvedName {
	if catalogName != "" {
		return tree.MakeUnresolvedName(catalogName, schemaName, tableName)
	} else if schemaName != "" {
		return tree.MakeUnresolvedName(schemaName, tableName)
	}
	return tree.MakeUnresolvedName(tableName)
}

func lineInterpolatePointForRepeatOverload(repeat bool, builtinInfo string) tree.Overload {
	return tree.Overload{
		Types: tree.ArgTypes{
			{"geometry", types.Geometry},
			{"fraction", types.Float},
		},
		ReturnType: tree.FixedReturnType(types.Geometry),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			g := args[0].(*tree.DGeometry)
			fraction := float64(*args[1].(*tree.DFloat))
			interpolatedPoints, err := geomfn.LineInterpolatePoints(g.Geometry, fraction, repeat)
			if err != nil {
				return nil, err
			}
			return tree.NewDGeometry(interpolatedPoints), nil
		},
		Info: infoBuilder{
			info:         builtinInfo,
			libraryUsage: usesGEOS,
		}.String(),
		Volatility: tree.VolatilityImmutable,
	}
}
