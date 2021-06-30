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
	"context"
	gojson "encoding/json"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geogfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/geo/geomfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/geo/geotransform"
	"github.com/cockroachdb/cockroach/pkg/geo/twkb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
	`the closest two points. The spheroid distance between these two points is calculated using GeographicLib. ` +
	`This follows observed PostGIS behavior.`

const (
	defaultWKTDecimalDigits = 15
)

// infoBuilder is used to build a detailed info string that is consistent between
// geospatial data types.
type infoBuilder struct {
	info         string
	libraryUsage libraryUsage
	precision    string
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
	return sb.String()
}

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
				if g.ShapeType2D() != shapeType {
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
				if g.ShapeType2D() != shapeType {
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
				if g.ShapeType2D() != shapeType {
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
				if g.ShapeType2D() != shapeType {
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

var lengthOverloadGeometry1 = geometryOverload1(
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
)

var perimeterOverloadGeometry1 = geometryOverload1(
	func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
		ret, err := geomfn.Perimeter(g.Geometry)
		if err != nil {
			return nil, err
		}
		return tree.NewDFloat(tree.DFloat(ret)), nil
	},
	types.Float,
	infoBuilder{
		info: `Returns the perimeter of the given geometry.

Note ST_Perimeter is only valid for Polygon - use ST_Length for LineString.`,
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

var usingBestGeomProjectionWarning = `

This operation is done by transforming the object into a Geometry. This occurs by translating
the Geography objects into Geometry objects before applying an LAEA, UTM or Web Mercator
based projection based on the bounding boxes of the given Geography objects. When the result is
calculated, the result is transformed back into a Geography with SRID 4326.`

// performGeographyOperationUsingBestGeomProjection performs an operation on a
// Geography by transforming it to a relevant Geometry SRID and applying the closure,
// before retransforming it back into a geopb.DefaultGeographySRID geometry.
func performGeographyOperationUsingBestGeomProjection(
	g geo.Geography, f func(geo.Geometry) (geo.Geometry, error),
) (geo.Geography, error) {
	bestProj, err := geogfn.BestGeomProjection(g.BoundingRect())
	if err != nil {
		return geo.Geography{}, err
	}
	geogDefaultProj, err := geoprojbase.Projection(geopb.DefaultGeographySRID)
	if err != nil {
		return geo.Geography{}, err
	}
	gProj, err := geoprojbase.Projection(g.SRID())
	if err != nil {
		return geo.Geography{}, err
	}

	inLatLonGeom, err := g.AsGeometry()
	if err != nil {
		return geo.Geography{}, err
	}

	inProjectedGeom, err := geotransform.Transform(
		inLatLonGeom,
		gProj.Proj4Text,
		bestProj,
		g.SRID(),
	)
	if err != nil {
		return geo.Geography{}, err
	}

	outProjectedGeom, err := f(inProjectedGeom)
	if err != nil {
		return geo.Geography{}, err
	}

	outGeom, err := geotransform.Transform(
		outProjectedGeom,
		bestProj,
		geogDefaultProj.Proj4Text,
		geopb.DefaultGeographySRID,
	)
	if err != nil {
		return geo.Geography{}, err
	}
	return outGeom.AsGeography()
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

func makeMinimumBoundGenerator(
	ctx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	geometry := tree.MustBeDGeometry(args[0])

	_, center, radius, err := geomfn.MinimumBoundingCircle(geometry.Geometry)
	if err != nil {
		return nil, err
	}
	return &minimumBoundRadiusGen{
		center: center,
		radius: radius,
		next:   true,
	}, nil
}

var minimumBoundingRadiusReturnType = types.MakeLabeledTuple(
	[]*types.T{types.Geometry, types.Float},
	[]string{"center", "radius"},
)

type minimumBoundRadiusGen struct {
	center geo.Geometry
	radius float64
	next   bool
}

func (m *minimumBoundRadiusGen) ResolvedType() *types.T {
	return minimumBoundingRadiusReturnType
}

func (m *minimumBoundRadiusGen) Start(ctx context.Context, txn *kv.Txn) error {
	return nil
}

func (m *minimumBoundRadiusGen) Next(ctx context.Context) (bool, error) {
	if m.next {
		m.next = false
		return true, nil
	}
	return false, nil
}

func (m *minimumBoundRadiusGen) Values() (tree.Datums, error) {
	return []tree.Datum{tree.NewDGeometry(m.center),
		tree.NewDFloat(tree.DFloat(m.radius))}, nil
}

func (m *minimumBoundRadiusGen) Close(_ context.Context) {}

func makeSubdividedGeometriesGeneratorFactory(expectMaxVerticesArg bool) tree.GeneratorFactory {
	return func(
		ctx *tree.EvalContext, args tree.Datums,
	) (tree.ValueGenerator, error) {
		geometry := tree.MustBeDGeometry(args[0])
		var maxVertices int
		if expectMaxVerticesArg {
			maxVertices = int(tree.MustBeDInt(args[1]))
		} else {
			maxVertices = 256
		}
		results, err := geomfn.Subdivide(geometry.Geometry, maxVertices)
		if err != nil {
			return nil, err
		}
		return &subdividedGeometriesGen{
			geometries: results,
			curr:       -1,
		}, nil
	}
}

// subdividedGeometriesGen implements the tree.ValueGenerator interface
type subdividedGeometriesGen struct {
	geometries []geo.Geometry
	curr       int
}

func (s *subdividedGeometriesGen) ResolvedType() *types.T { return types.Geometry }

func (s *subdividedGeometriesGen) Close(_ context.Context) {}

func (s *subdividedGeometriesGen) Start(_ context.Context, _ *kv.Txn) error {
	s.curr = -1
	return nil
}

func (s *subdividedGeometriesGen) Values() (tree.Datums, error) {
	return tree.Datums{tree.NewDGeometry(s.geometries[s.curr])}, nil
}

func (s *subdividedGeometriesGen) Next(_ context.Context) (bool, error) {
	s.curr++
	return s.curr < len(s.geometries), nil
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
				if g.Geometry.ShapeType2D() == geopb.ShapeType_Point {
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
	"postgis_getbbox": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				bbox := g.CartesianBoundingBox()
				if bbox == nil {
					return tree.DNull, nil
				}
				return tree.NewDBox2D(*bbox), nil
			},
			types.Box2D,
			infoBuilder{
				info: "Returns a box2d encapsulating the given Geometry.",
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
	// Indexing
	//

	"st_s2covering": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(evalCtx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				cfg, err := geoindex.GeometryIndexConfigForSRID(g.SRID())
				if err != nil {
					return nil, err
				}
				ret, err := geoindex.NewS2GeometryIndex(*cfg.S2Geometry).CoveringGeometry(evalCtx.Context, g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a geometry which represents the S2 covering used by the index using the default index configuration.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"settings", types.String}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				params := tree.MustBeDString(args[1])

				startCfg, err := geoindex.GeometryIndexConfigForSRID(g.SRID())
				if err != nil {
					return nil, err
				}
				cfg, err := applyGeoindexConfigStorageParams(evalCtx, *startCfg, string(params))
				if err != nil {
					return nil, err
				}
				ret, err := geoindex.NewS2GeometryIndex(*cfg.S2Geometry).CoveringGeometry(evalCtx.Context, g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `
Returns a geometry which represents the S2 covering used by the index using the index configuration specified
by the settings parameter.

The settings parameter uses the same format as the parameters inside the WITH in CREATE INDEX ... WITH (...),
e.g. CREATE INDEX t_idx ON t USING GIST(geom) WITH (s2_max_level=15, s2_level_mod=3) can be tried using
SELECT ST_S2Covering(geometry, 's2_max_level=15,s2_level_mod=3').
`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(evalCtx *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				cfg := geoindex.DefaultGeographyIndexConfig().S2Geography
				ret, err := geoindex.NewS2GeographyIndex(*cfg).CoveringGeography(evalCtx.Context, g.Geography)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(ret), nil
			},
			types.Geography,
			infoBuilder{
				info: "Returns a geography which represents the S2 covering used by the index using the default index configuration.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"geography", types.Geography}, {"settings", types.String}},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeography(args[0])
				params := tree.MustBeDString(args[1])

				startCfg := geoindex.DefaultGeographyIndexConfig()
				cfg, err := applyGeoindexConfigStorageParams(evalCtx, *startCfg, string(params))
				if err != nil {
					return nil, err
				}
				ret, err := geoindex.NewS2GeographyIndex(*cfg.S2Geography).CoveringGeography(evalCtx.Context, g.Geography)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(ret), nil
			},
			Info: infoBuilder{
				info: `
Returns a geography which represents the S2 covering used by the index using the index configuration specified
by the settings parameter.

The settings parameter uses the same format as the parameters inside the WITH in CREATE INDEX ... WITH (...),
e.g. CREATE INDEX t_idx ON t USING GIST(geom) WITH (s2_max_level=15, s2_level_mod=3) can be tried using
SELECT ST_S2Covering(geography, 's2_max_level=15,s2_level_mod=3').
`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Input (Geometry)
	//

	"st_geometryfromtext": makeBuiltin(
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
	),
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
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.String}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromGeoJSON([]byte(tree.MustBeDString(args[0])))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			// Simulate PostgreSQL's ambiguity type resolving check that prefers
			// strings over JSON.
			PreferredOverload: true,
			Info:              infoBuilder{info: "Returns the Geometry from an GeoJSON representation."}.String(),
			Volatility:        tree.VolatilityImmutable,
		},
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
	"st_makepoint": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Float}, {"y", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := float64(tree.MustBeDFloat(args[0]))
				y := float64(tree.MustBeDFloat(args[1]))
				g, err := geo.MakeGeometryFromLayoutAndPointCoords(geom.XY, []float64{x, y})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info:       infoBuilder{info: `Returns a new Point with the given X and Y coordinates.`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Float}, {"y", types.Float}, {"z", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := float64(tree.MustBeDFloat(args[0]))
				y := float64(tree.MustBeDFloat(args[1]))
				z := float64(tree.MustBeDFloat(args[2]))
				g, err := geo.MakeGeometryFromLayoutAndPointCoords(geom.XYZ, []float64{x, y, z})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info:       infoBuilder{info: `Returns a new Point with the given X, Y, and Z coordinates.`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Float}, {"y", types.Float}, {"z", types.Float}, {"m", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := float64(tree.MustBeDFloat(args[0]))
				y := float64(tree.MustBeDFloat(args[1]))
				z := float64(tree.MustBeDFloat(args[2]))
				m := float64(tree.MustBeDFloat(args[3]))
				g, err := geo.MakeGeometryFromLayoutAndPointCoords(geom.XYZM, []float64{x, y, z, m})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info:       infoBuilder{info: `Returns a new Point with the given X, Y, Z, and M coordinates.`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_makepointm": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Float}, {"y", types.Float}, {"m", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := float64(tree.MustBeDFloat(args[0]))
				y := float64(tree.MustBeDFloat(args[1]))
				m := float64(tree.MustBeDFloat(args[2]))
				g, err := geo.MakeGeometryFromLayoutAndPointCoords(geom.XYM, []float64{x, y, m})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info:       infoBuilder{info: `Returns a new Point with the given X, Y, and M coordinates.`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_makepolygon": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, outer *tree.DGeometry) (tree.Datum, error) {
				g, err := geomfn.MakePolygon(outer.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns a new Polygon with the given outer LineString.`,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"outer", types.Geometry},
				{"interior", types.AnyArray},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				outer := tree.MustBeDGeometry(args[0])
				interiorArr := tree.MustBeDArray(args[1])
				interior := make([]geo.Geometry, len(interiorArr.Array))
				for i, v := range interiorArr.Array {
					g, ok := v.(*tree.DGeometry)
					if !ok {
						return nil, errors.Newf("argument must be LINESTRING geometries")
					}
					interior[i] = g.Geometry
				}
				g, err := geomfn.MakePolygon(outer.Geometry, interior...)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: `Returns a new Polygon with the given outer LineString and interior (hole) LineString(s).`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_polygon": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"srid", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				srid := tree.MustBeDInt(args[1])
				polygon, err := geomfn.MakePolygonWithSRID(g.Geometry, int(srid))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(polygon), nil
			},
			Info: infoBuilder{
				info: `Returns a new Polygon from the given LineString and sets its SRID. It is equivalent ` +
					`to ST_MakePolygon with a single argument followed by ST_SetSRID.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
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

	"st_geographyfromtext": makeBuiltin(
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
	),

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
	"st_point": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"x", types.Float}, {"y", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := float64(tree.MustBeDFloat(args[0]))
				y := float64(tree.MustBeDFloat(args[1]))
				g, err := geo.MakeGeometryFromPointCoords(x, y)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info:       infoBuilder{info: `Returns a new Point with the given X and Y coordinates.`}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_pointfromgeohash": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geohash", types.String},
				{"precision", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDString(args[0])
				p := tree.MustBeDInt(args[1])
				ret, err := geo.ParseGeometryPointFromGeoHash(string(g), int(p))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Return a POINT Geometry from a GeoHash string with supplied precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geohash", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDString(args[0])
				p := len(string(g))
				ret, err := geo.ParseGeometryPointFromGeoHash(string(g), p)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Return a POINT Geometry from a GeoHash string with max precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_geomfromgeohash": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geohash", types.String},
				{"precision", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDString(args[0])
				p := tree.MustBeDInt(args[1])
				bbox, err := geo.ParseCartesianBoundingBoxFromGeoHash(string(g), int(p))
				if err != nil {
					return nil, err
				}
				ret, err := geo.MakeGeometryFromGeomT(bbox.ToGeomT(geopb.DefaultGeometrySRID))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Return a POLYGON Geometry from a GeoHash string with supplied precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geohash", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDString(args[0])
				p := len(string(g))
				bbox, err := geo.ParseCartesianBoundingBoxFromGeoHash(string(g), p)
				if err != nil {
					return nil, err
				}
				ret, err := geo.MakeGeometryFromGeomT(bbox.ToGeomT(geopb.DefaultGeometrySRID))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Return a POLYGON Geometry from a GeoHash string with max precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_box2dfromgeohash": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geohash", types.String},
				{"precision", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDString(args[0])
				p := tree.MustBeDInt(args[1])
				bbox, err := geo.ParseCartesianBoundingBoxFromGeoHash(string(g), int(p))
				if err != nil {
					return nil, err
				}
				return tree.NewDBox2D(bbox), nil
			},
			Info: infoBuilder{
				info: "Return a Box2D from a GeoHash string with supplied precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geohash", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDString(args[0])
				p := len(string(g))
				bbox, err := geo.ParseCartesianBoundingBoxFromGeoHash(string(g), p)
				if err != nil {
					return nil, err
				}
				return tree.NewDBox2D(bbox), nil
			},
			Info: infoBuilder{
				info: "Return a Box2D from a GeoHash string with max precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Output
	//

	"st_astext": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				wkt, err := geo.SpatialObjectToWKT(g.Geometry.SpatialObject(), defaultWKTDecimalDigits)
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
				g := tree.MustBeDGeometry(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				wkt, err := geo.SpatialObjectToWKT(g.Geometry.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
				return tree.NewDString(string(wkt)), err
			},
			Info: infoBuilder{
				info: "Returns the WKT representation of a given Geometry. The max_decimal_digits parameter controls the maximum decimal digits to print after the `.`. Use -1 to print as many digits as required to rebuild the same number.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkt, err := geo.SpatialObjectToWKT(g.Geography.SpatialObject(), defaultWKTDecimalDigits)
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
				g := tree.MustBeDGeography(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				wkt, err := geo.SpatialObjectToWKT(g.Geography.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
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
				ewkt, err := geo.SpatialObjectToEWKT(g.Geometry.SpatialObject(), defaultWKTDecimalDigits)
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
				g := tree.MustBeDGeometry(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				ewkt, err := geo.SpatialObjectToEWKT(g.Geometry.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
				return tree.NewDString(string(ewkt)), err
			},
			Info: infoBuilder{
				info: "Returns the WKT representation of a given Geometry. The max_decimal_digits parameter controls the maximum decimal digits to print after the `.`. Use -1 to print as many digits as required to rebuild the same number.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				ewkt, err := geo.SpatialObjectToEWKT(g.Geography.SpatialObject(), defaultWKTDecimalDigits)
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
				g := tree.MustBeDGeography(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				ewkt, err := geo.SpatialObjectToEWKT(g.Geography.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits))
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
				wkb, err := geo.SpatialObjectToWKB(g.Geometry.SpatialObject(), geo.DefaultEWKBEncodingFormat)
				return tree.NewDBytes(tree.DBytes(wkb)), err
			},
			types.Bytes,
			infoBuilder{info: "Returns the WKB representation of a given Geometry."},
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				wkb, err := geo.SpatialObjectToWKB(g.Geography.SpatialObject(), geo.DefaultEWKBEncodingFormat)
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
				g := tree.MustBeDGeometry(args[0])
				text := string(tree.MustBeDString(args[1]))

				wkb, err := geo.SpatialObjectToWKB(g.Geometry.SpatialObject(), geo.StringToByteOrder(text))
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
				g := tree.MustBeDGeography(args[0])
				text := string(tree.MustBeDString(args[1]))

				wkb, err := geo.SpatialObjectToWKB(g.Geography.SpatialObject(), geo.StringToByteOrder(text))
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
				hexwkb, err := geo.SpatialObjectToWKBHex(g.Geometry.SpatialObject())
				return tree.NewDString(hexwkb), err
			},
			types.String,
			infoBuilder{info: "Returns the WKB representation in hex of a given Geometry."},
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				hexwkb, err := geo.SpatialObjectToWKBHex(g.Geography.SpatialObject())
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
				g := tree.MustBeDGeometry(args[0])
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
				g := tree.MustBeDGeography(args[0])
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
	"st_astwkb": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"precision_xy", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				t, err := tree.MustBeDGeometry(args[0]).AsGeomT()
				if err != nil {
					return nil, err
				}
				ret, err := twkb.Marshal(
					t,
					twkb.MarshalOptionPrecisionXY(int64(tree.MustBeDInt(args[1]))),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns the TWKB representation of a given geometry.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"precision_xy", types.Int},
				{"precision_z", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				t, err := tree.MustBeDGeometry(args[0]).AsGeomT()
				if err != nil {
					return nil, err
				}
				ret, err := twkb.Marshal(
					t,
					twkb.MarshalOptionPrecisionXY(int64(tree.MustBeDInt(args[1]))),
					twkb.MarshalOptionPrecisionZ(int64(tree.MustBeDInt(args[2]))),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns the TWKB representation of a given geometry.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"precision_xy", types.Int},
				{"precision_z", types.Int},
				{"precision_m", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				t, err := tree.MustBeDGeometry(args[0]).AsGeomT()
				if err != nil {
					return nil, err
				}
				ret, err := twkb.Marshal(
					t,
					twkb.MarshalOptionPrecisionXY(int64(tree.MustBeDInt(args[1]))),
					twkb.MarshalOptionPrecisionZ(int64(tree.MustBeDInt(args[2]))),
					twkb.MarshalOptionPrecisionM(int64(tree.MustBeDInt(args[3]))),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns the TWKB representation of a given geometry.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_askml": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				kml, err := geo.SpatialObjectToKML(g.Geometry.SpatialObject())
				return tree.NewDString(kml), err
			},
			types.String,
			infoBuilder{info: "Returns the KML representation of a given Geometry."},
			tree.VolatilityImmutable,
		),
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				kml, err := geo.SpatialObjectToKML(g.Geography.SpatialObject())
				return tree.NewDString(kml), err
			},
			types.String,
			infoBuilder{info: "Returns the KML representation of a given Geography."},
			tree.VolatilityImmutable,
		),
	),
	"st_geohash": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geo.SpatialObjectToGeoHash(g.Geometry.SpatialObject(), geo.GeoHashAutoPrecision)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			types.String,
			infoBuilder{
				info: "Returns a GeoHash representation of the geometry with full precision if a point is provided, or with variable precision based on the size of the feature. This will error any coordinates are outside the bounds of longitude/latitude.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"precision", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				p := tree.MustBeDInt(args[1])
				ret, err := geo.SpatialObjectToGeoHash(g.Geometry.SpatialObject(), int(p))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: infoBuilder{
				info: "Returns a GeoHash representation of the geometry with the supplied precision. This will error any coordinates are outside the bounds of longitude/latitude.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		geographyOverload1(
			func(_ *tree.EvalContext, g *tree.DGeography) (tree.Datum, error) {
				ret, err := geo.SpatialObjectToGeoHash(g.Geography.SpatialObject(), geo.GeoHashAutoPrecision)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			types.String,
			infoBuilder{
				info: "Returns a GeoHash representation of the geeographywith full precision if a point is provided, or with variable precision based on the size of the feature.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"precision", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeography(args[0])
				p := tree.MustBeDInt(args[1])
				ret, err := geo.SpatialObjectToGeoHash(g.Geography.SpatialObject(), int(p))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: infoBuilder{
				info: "Returns a GeoHash representation of the geography with the supplied precision.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_asgeojson": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"row", types.AnyTuple}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tuple := tree.MustBeDTuple(args[0])
				return stAsGeoJSONFromTuple(
					ctx,
					tuple,
					"", /* geoColumn */
					geo.DefaultGeoJSONDecimalDigits,
					false, /* pretty */
				)
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geometry. Coordinates have a maximum of %d decimal digits.",
					geo.DefaultGeoJSONDecimalDigits,
				),
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"row", types.AnyTuple}, {"geo_column", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tuple := tree.MustBeDTuple(args[0])
				return stAsGeoJSONFromTuple(
					ctx,
					tuple,
					string(tree.MustBeDString(args[1])),
					geo.DefaultGeoJSONDecimalDigits,
					false, /* pretty */
				)
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geometry, using geo_column as the geometry for the given Feature. Coordinates have a maximum of %d decimal digits.",
					geo.DefaultGeoJSONDecimalDigits,
				),
			}.String(),
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"row", types.AnyTuple},
				{"geo_column", types.String},
				{"max_decimal_digits", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tuple := tree.MustBeDTuple(args[0])
				return stAsGeoJSONFromTuple(
					ctx,
					tuple,
					string(tree.MustBeDString(args[1])),
					fitMaxDecimalDigitsToBounds(int(tree.MustBeDInt(args[2]))),
					false, /* pretty */
				)
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geometry, using geo_column as the geometry for the given Feature. " +
						"max_decimal_digits will be output for each coordinate value.",
				),
			}.String(),
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"row", types.AnyTuple},
				{"geo_column", types.String},
				{"max_decimal_digits", types.Int},
				{"pretty", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tuple := tree.MustBeDTuple(args[0])
				return stAsGeoJSONFromTuple(
					ctx,
					tuple,
					string(tree.MustBeDString(args[1])),
					fitMaxDecimalDigitsToBounds(int(tree.MustBeDInt(args[2]))),
					bool(tree.MustBeDBool(args[3])),
				)
			},
			Info: infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geometry, using geo_column as the geometry for the given Feature. " +
						"max_decimal_digits will be output for each coordinate value. Output will be pretty printed in JSON if pretty is true.",
				),
			}.String(),
			Volatility: tree.VolatilityStable,
		},
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				geojson, err := geo.SpatialObjectToGeoJSON(g.Geometry.SpatialObject(), geo.DefaultGeoJSONDecimalDigits, geo.SpatialObjectToGeoJSONFlagShortCRSIfNot4326)
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geometry. Coordinates have a maximum of %d decimal digits.",
					geo.DefaultGeoJSONDecimalDigits,
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
				g := tree.MustBeDGeometry(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				geojson, err := geo.SpatialObjectToGeoJSON(g.Geometry.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), geo.SpatialObjectToGeoJSONFlagShortCRSIfNot4326)
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
				g := tree.MustBeDGeometry(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				options := geo.SpatialObjectToGeoJSONFlag(tree.MustBeDInt(args[2]))
				geojson, err := geo.SpatialObjectToGeoJSON(g.Geometry.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), options)
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
				geojson, err := geo.SpatialObjectToGeoJSON(g.Geography.SpatialObject(), geo.DefaultGeoJSONDecimalDigits, geo.SpatialObjectToGeoJSONFlagZero)
				return tree.NewDString(string(geojson)), err
			},
			types.String,
			infoBuilder{
				info: fmt.Sprintf(
					"Returns the GeoJSON representation of a given Geography. Coordinates have a maximum of %d decimal digits.",
					geo.DefaultGeoJSONDecimalDigits,
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
				g := tree.MustBeDGeography(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				geojson, err := geo.SpatialObjectToGeoJSON(g.Geography.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), geo.SpatialObjectToGeoJSONFlagZero)
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
				g := tree.MustBeDGeography(args[0])
				maxDecimalDigits := int(tree.MustBeDInt(args[1]))
				options := geo.SpatialObjectToGeoJSONFlag(tree.MustBeDInt(args[2]))
				geojson, err := geo.SpatialObjectToGeoJSON(g.Geography.SpatialObject(), fitMaxDecimalDigitsToBounds(maxDecimalDigits), options)
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
				g := tree.MustBeDGeography(args[0])
				distance := float64(tree.MustBeDFloat(args[1]))
				azimuth := float64(tree.MustBeDFloat(args[2]))

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
				switch t.Layout() {
				case geom.NoLayout:
					if gc, ok := t.(*geom.GeometryCollection); ok && gc.Empty() {
						return tree.NewDInt(tree.DInt(geom.XY.Stride())), nil
					}
					return nil, errors.AssertionFailedf("no layout found on object")
				default:
					return tree.NewDInt(tree.DInt(t.Stride())), nil
				}
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of coordinate dimensions of a given Geometry.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_dimension": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				dim, err := geomfn.Dimension(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(dim)), nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of topological dimensions of a given Geometry.",
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
					retG, err := geo.MakeGeometryFromGeomT(
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

				summary, err := geo.Summary(t, g.SpatialObject().BoundingBox != nil, g.ShapeType2D(), false)
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

				summary, err := geo.Summary(t, g.SpatialObject().BoundingBox != nil, g.ShapeType2D(), true)
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
					retG, err := geo.MakeGeometryFromGeomT(
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
	"st_generatepoints": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"npoints", types.Int4}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				geometry := tree.MustBeDGeometry(args[0]).Geometry
				npoints := int(tree.MustBeDInt(args[1]))
				seed := timeutil.Now().Unix()
				generatedPoints, err := geomfn.GenerateRandomPoints(geometry, npoints, rand.New(rand.NewSource(seed)))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(generatedPoints), nil
			},
			Info: infoBuilder{
				info: `Generates pseudo-random points until the requested number are found within the input area. Uses system time as a seed.
The requested number of points must be not larger than 65336.`,
			}.String(),
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"npoints", types.Int4}, {"seed", types.Int4}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				geometry := tree.MustBeDGeometry(args[0]).Geometry
				npoints := int(tree.MustBeDInt(args[1]))
				seed := int64(tree.MustBeDInt(args[2]))
				if seed < 1 {
					return nil, errors.New("seed must be greater than zero")
				}
				generatedPoints, err := geomfn.GenerateRandomPoints(geometry, npoints, rand.New(rand.NewSource(seed)))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(generatedPoints), nil
			},
			Info: infoBuilder{
				info: `Generates pseudo-random points until the requested number are found within the input area.
The requested number of points must be not larger than 65336.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
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
	"st_hasarc": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				// We don't support CIRCULARSTRINGs, so always return false.
				return tree.DBoolFalse, nil
			},
			types.Bool,
			infoBuilder{
				info: "Returns whether there is a CIRCULARSTRING in the geometry.",
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
				return tree.NewDInt(tree.DInt(geomfn.CountVertices(t))), nil
			},
			types.Int,
			infoBuilder{
				info: "Returns the number of points in a given Geometry. Works for any shape type.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_points": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				points, err := geomfn.Points(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(points), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns all coordinates in the given Geometry as a MultiPoint, including duplicates.",
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
					ret, err := geo.MakeGeometryFromGeomT(lineString.SetSRID(t.SRID()))
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
				g := tree.MustBeDGeometry(args[0])
				n := int(tree.MustBeDInt(args[1]))
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
					ret, err := geo.MakeGeometryFromGeomT(lineString)
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
				g := tree.MustBeDGeometry(args[0])
				n := int(tree.MustBeDInt(args[1])) - 1
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
					g, err := geo.MakeGeometryFromGeomT(geom.NewPointFlat(t.Layout(), t.Coord(n)).SetSRID(t.SRID()))
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
				g := tree.MustBeDGeometry(args[0])
				n := int(tree.MustBeDInt(args[1])) - 1
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
					g, err := geo.MakeGeometryFromGeomT(t.Point(n).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				case *geom.MultiLineString:
					if n >= t.NumLineStrings() {
						return tree.DNull, nil
					}
					g, err := geo.MakeGeometryFromGeomT(t.LineString(n).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				case *geom.MultiPolygon:
					if n >= t.NumPolygons() {
						return tree.DNull, nil
					}
					g, err := geo.MakeGeometryFromGeomT(t.Polygon(n).SetSRID(t.SRID()))
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(g), nil
				case *geom.GeometryCollection:
					if n >= t.NumGeoms() {
						return tree.DNull, nil
					}
					g, err := geo.MakeGeometryFromGeomT(t.Geom(n))
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
	"st_minimumclearance": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.MinimumClearance(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(ret)), nil
			},
			types.Float,
			infoBuilder{
				info: `Returns the minimum distance a vertex can move before producing an invalid geometry. ` +
					`Returns Infinity if no minimum clearance can be found (e.g. for a single point).`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_minimumclearanceline": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.MinimumClearanceLine(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns a LINESTRING spanning the minimum distance a vertex can move before producing ` +
					`an invalid geometry. If no minimum clearance can be found (e.g. for a single point), an ` +
					`empty LINESTRING is returned.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_numinteriorrings": makeBuiltin(
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
	),
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
	"st_force2d": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForceLayout(g.Geometry, geom.XY)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a Geometry that is forced into XY layout with any Z or M dimensions discarded.",
			},
			tree.VolatilityImmutable,
		),
	),
	// TODO(ayang): see if it's possible to refactor default args
	"st_force3dz": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForceLayout(g.Geometry, geom.XYZ)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a Geometry that is forced into XYZ layout. " +
					"If a Z coordinate doesn't exist, it will be set to 0. " +
					"If a M coordinate is present, it will be discarded.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"defaultZ", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				defaultZ := tree.MustBeDFloat(args[1])

				ret, err := geomfn.ForceLayoutWithDefaultZ(g.Geometry, geom.XYZ, float64(defaultZ))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{info: "Returns a Geometry that is forced into XYZ layout. " +
				"If a Z coordinate doesn't exist, it will be set to the specified default Z value. " +
				"If a M coordinate is present, it will be discarded."}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_force3dm": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForceLayout(g.Geometry, geom.XYM)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a Geometry that is forced into XYM layout. " +
					"If a M coordinate doesn't exist, it will be set to 0. " +
					"If a Z coordinate is present, it will be discarded.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"defaultM", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				defaultM := tree.MustBeDFloat(args[1])

				ret, err := geomfn.ForceLayoutWithDefaultM(g.Geometry, geom.XYM, float64(defaultM))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{info: "Returns a Geometry that is forced into XYM layout. " +
				"If a M coordinate doesn't exist, it will be set to the specified default M value. " +
				"If a Z coordinate is present, it will be discarded."}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_force4d": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForceLayout(g.Geometry, geom.XYZM)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a Geometry that is forced into XYZM layout. " +
					"If a Z coordinate doesn't exist, it will be set to 0. " +
					"If a M coordinate doesn't exist, it will be set to 0.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"defaultZ", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				defaultZ := tree.MustBeDFloat(args[1])

				ret, err := geomfn.ForceLayoutWithDefaultZ(g.Geometry, geom.XYZM, float64(defaultZ))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{info: "Returns a Geometry that is forced into XYZ layout. " +
				"If a Z coordinate doesn't exist, it will be set to the specified default Z value. " +
				"If a M coordinate doesn't exist, it will be set to 0."}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"defaultZ", types.Float}, {"defaultM", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				defaultZ := tree.MustBeDFloat(args[1])
				defaultM := tree.MustBeDFloat(args[2])

				ret, err := geomfn.ForceLayoutWithDefaultZM(g.Geometry, geom.XYZM, float64(defaultZ), float64(defaultM))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{info: "Returns a Geometry that is forced into XYZ layout. " +
				"If a Z coordinate doesn't exist, it will be set to the specified Z value. " +
				"If a M coordinate doesn't exist, it will be set to the specified M value."}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_forcepolygoncw": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForcePolygonOrientation(g.Geometry, geomfn.OrientationCW)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a Geometry where all Polygon objects have exterior rings in the clockwise orientation and interior rings in the counter-clockwise orientation. Non-Polygon objects are unchanged.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_forcepolygonccw": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForcePolygonOrientation(g.Geometry, geomfn.OrientationCCW)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a Geometry where all Polygon objects have exterior rings in the counter-clockwise orientation and interior rings in the clockwise orientation. Non-Polygon objects are unchanged.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_ispolygoncw": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			func(g geo.Geometry) (bool, error) {
				return geomfn.HasPolygonOrientation(g, geomfn.OrientationCW)
			},
			infoBuilder{
				info: "Returns whether the Polygon objects inside the Geometry have exterior rings in the clockwise orientation and interior rings in the counter-clockwise orientation. Non-Polygon objects are considered clockwise.",
			},
		),
	),
	"st_ispolygonccw": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			func(g geo.Geometry) (bool, error) {
				return geomfn.HasPolygonOrientation(g, geomfn.OrientationCCW)
			},
			infoBuilder{
				info: "Returns whether the Polygon objects inside the Geometry have exterior rings in the counter-clockwise orientation and interior rings in the clockwise orientation. Non-Polygon objects are considered counter-clockwise.",
			},
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
				return nil, errors.Newf("argument to st_x() must have shape POINT")
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
				return nil, errors.Newf("argument to st_y() must have shape POINT")
			},
			types.Float,
			infoBuilder{
				info: "Returns the Y coordinate of a geometry if it is a Point.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_z": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(evalContext *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Point:
					if t.Empty() || t.Layout().ZIndex() == -1 {
						return tree.DNull, nil
					}
					return tree.NewDFloat(tree.DFloat(t.Z())), nil
				}
				// Ideally we should return NULL here, but following PostGIS on this.
				return nil, errors.Newf("argument to st_z() must have shape POINT")
			},
			types.Float,
			infoBuilder{
				info: "Returns the Z coordinate of a geometry if it is a Point.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_m": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(evalContext *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch t := t.(type) {
				case *geom.Point:
					if t.Empty() || t.Layout().MIndex() == -1 {
						return tree.DNull, nil
					}
					return tree.NewDFloat(tree.DFloat(t.M())), nil
				}
				// Ideally we should return NULL here, but following PostGIS on this.
				return nil, errors.Newf("argument to st_m() must have shape POINT")
			},
			types.Float,
			infoBuilder{
				info: "Returns the M coordinate of a geometry if it is a Point.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_zmflag": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(evalContext *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				t, err := g.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch layout := t.Layout(); layout {
				case geom.XY:
					return tree.NewDInt(tree.DInt(0)), nil
				case geom.XYM:
					return tree.NewDInt(tree.DInt(1)), nil
				case geom.XYZ:
					return tree.NewDInt(tree.DInt(2)), nil
				case geom.XYZM:
					return tree.NewDInt(tree.DInt(3)), nil
				default:
					return nil, errors.Newf("unknown geom.Layout %d", layout)
				}
			},
			types.Int2,
			infoBuilder{
				info: "Returns a code based on the ZM coordinate dimension of a geometry (XY = 0, XYM = 1, XYZ = 2, XYZM = 3).",
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
			lengthOverloadGeometry1,
		)...,
	),
	"st_length2d": makeBuiltin(
		defProps(),
		lengthOverloadGeometry1,
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
			perimeterOverloadGeometry1,
		)...,
	),
	"st_perimeter2d": makeBuiltin(
		defProps(),
		perimeterOverloadGeometry1,
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
				return tree.NewDString(g.ShapeType2D().String()), nil
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
				return tree.NewDString(fmt.Sprintf("ST_%s", g.ShapeType2D().String())), nil
			},
			types.String,
			infoBuilder{
				info:         "Returns the type of geometry as a string prefixed with `ST_`.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_addmeasure": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"start", types.Float}, {"end", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				start := tree.MustBeDFloat(args[1])
				end := tree.MustBeDFloat(args[2])

				ret, err := geomfn.AddMeasure(g.Geometry, float64(start), float64(end))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{info: "Returns a copy of a LineString or MultiLineString with measure coordinates " +
				"linearly interpolated between the specified start and end values. " +
				"Any existing M coordinates will be overwritten."}.String(),
			Volatility: tree.VolatilityImmutable,
		},
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
				g := tree.MustBeDGeometry(args[0])
				fraction := float64(tree.MustBeDFloat(args[1]))
				repeat := bool(tree.MustBeDBool(args[2]))
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
	"st_multi": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				multi, err := geomfn.Multi(g.Geometry)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: multi}, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns the geometry as a new multi-geometry, e.g converts a POINT to a MULTIPOINT. If the input ` +
					`is already a multitype or collection, it is returned as is.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_collectionextract": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"type", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				shapeType := tree.MustBeDInt(args[1])
				res, err := geomfn.CollectionExtract(g.Geometry, geopb.ShapeType(shapeType))
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: res}, nil
			},
			Info: infoBuilder{
				info: `Given a collection, returns a multitype consisting only of elements of the specified type. ` +
					`If there are no elements of the given type, an EMPTY geometry is returned. Types are specified as ` +
					`1=POINT, 2=LINESTRING, 3=POLYGON - other types are not supported.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_collectionhomogenize": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.CollectionHomogenize(g.Geometry)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: ret}, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns the "simplest" representation of a collection's contents. Collections of a single ` +
					`type will be returned as an appopriate multitype, or a singleton if it only contains a ` +
					`single geometry.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_forcecollection": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ForceCollection(g.Geometry)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: ret}, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Converts the geometry into a GeometryCollection.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_linefrommultipoint": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				line, err := geomfn.LineStringFromMultiPoint(g.Geometry)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: line}, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Creates a LineString from a MultiPoint geometry.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_linemerge": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				line, err := geomfn.LineMerge(g.Geometry)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: line}, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns a LineString or MultiLineString by joining together constituents of a ` +
					`MultiLineString with matching endpoints. If the input is not a MultiLineString or LineString, ` +
					`an empty GeometryCollection is returned.`,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_shiftlongitude": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ShiftLongitude(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns a modified version of a geometry in which the longitude (X coordinate) of each point is ` +
					`incremented by 360 if it is <0 and decremented by 360 if it is >180. The result is only meaningful ` +
					`if the coordinates are in longitude/latitude.`,
			},
			tree.VolatilityImmutable,
		),
	),

	//
	// Unary predicates
	//
	"st_isclosed": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			geomfn.IsClosed,
			infoBuilder{
				info: `Returns whether the geometry is closed as defined by whether the start and end points are coincident. ` +
					`Points are considered closed, empty geometries are not. For collections and multi-types, all members must be closed, ` +
					`as must all polygon rings.`,
			},
		),
	),
	"st_iscollection": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			geomfn.IsCollection,
			infoBuilder{
				info: "Returns whether the geometry is of a collection type (including multi-types).",
			},
		),
	),
	"st_isempty": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			geomfn.IsEmpty,
			infoBuilder{
				info: "Returns whether the geometry is empty.",
			},
		),
	),
	"st_isring": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			geomfn.IsRing,
			infoBuilder{
				info: `Returns whether the geometry is a single linestring that is closed and simple, as defined by ` +
					`ST_IsClosed and ST_IsSimple.`,
				libraryUsage: usesGEOS,
			},
		),
	),
	"st_issimple": makeBuiltin(
		defProps(),
		geometryOverload1UnaryPredicate(
			geomfn.IsSimple,
			infoBuilder{
				info: `Returns true if the geometry has no anomalous geometric points, e.g. that it intersects with ` +
					`or lies tangent to itself.`,
				libraryUsage: usesGEOS,
			},
		),
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
				a := tree.MustBeDGeography(args[0])
				b := tree.MustBeDGeography(args[1])
				useSpheroid := tree.MustBeDBool(args[2])

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
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_distancesphere": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				aGeog, err := a.Geometry.AsGeography()
				if err != nil {
					return nil, err
				}
				bGeog, err := b.Geometry.AsGeography()
				if err != nil {
					return nil, err
				}
				ret, err := geogfn.Distance(aGeog, bGeog, geogfn.UseSphere)
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
				info: "Returns the distance in meters between geometry_a and geometry_b assuming the coordinates " +
					"represent lng/lat points on a sphere.",
				libraryUsage: usesS2,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_distancespheroid": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				aGeog, err := a.Geometry.AsGeography()
				if err != nil {
					return nil, err
				}
				bGeog, err := b.Geometry.AsGeography()
				if err != nil {
					return nil, err
				}
				ret, err := geogfn.Distance(aGeog, bGeog, geogfn.UseSpheroid)
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
				info: "Returns the distance in meters between geometry_a and geometry_b assuming the coordinates " +
					"represent lng/lat points on a spheroid." + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib | usesS2,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_frechetdistance": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.FrechetDistance(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				if ret == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*ret)), nil
			},
			types.Float,
			infoBuilder{
				info:         `Returns the Frechet distance between the given geometries.`,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"densify_frac", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				densifyFrac := tree.MustBeDFloat(args[2])

				ret, err := geomfn.FrechetDistanceDensify(a.Geometry, b.Geometry, float64(densifyFrac))
				if err != nil {
					return nil, err
				}
				if ret == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*ret)), nil
			},
			Info: infoBuilder{
				info: "Returns the Frechet distance between the given geometries, with the given " +
					"segment densification (range 0.0-1.0, -1 to disable).\n\n" +
					"Smaller densify_frac gives a more accurate Fréchet distance. However, the computation " +
					"time and memory usage increases with the square of the number of subsegments.",
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_hausdorffdistance": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.HausdorffDistance(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				if ret == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*ret)), nil
			},
			types.Float,
			infoBuilder{
				info:         `Returns the Hausdorff distance between the given geometries.`,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"densify_frac", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				densifyFrac := tree.MustBeDFloat(args[2])

				ret, err := geomfn.HausdorffDistanceDensify(a.Geometry, b.Geometry, float64(densifyFrac))
				if err != nil {
					return nil, err
				}
				if ret == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*ret)), nil
			},
			Info: infoBuilder{
				info: `Returns the Hausdorff distance between the given geometries, with the given ` +
					`segment densification (range 0.0-1.0).`,
				libraryUsage: usesGEOS,
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
			},
		),
		geographyOverload2BinaryPredicate(
			geogfn.Covers,
			infoBuilder{
				info:         `Returns true if no point in geography_b is outside geography_a.`,
				libraryUsage: usesS2,
			},
		),
	),
	"st_coveredby": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.CoveredBy,
			infoBuilder{
				info:         `Returns true if no point in geometry_a is outside geometry_b.`,
				libraryUsage: usesGEOS,
			},
		),
		geographyOverload2BinaryPredicate(
			geogfn.CoveredBy,
			infoBuilder{
				info:         `Returns true if no point in geography_a is outside geography_b.`,
				libraryUsage: usesS2,
				precision:    "1cm",
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
			},
		),
	),
	"st_disjoint": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Disjoint,
			infoBuilder{
				info:         "Returns true if geometry_a does not overlap, touch or is within geometry_b.",
				libraryUsage: usesGEOS,
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
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				dist := tree.MustBeDFloat(args[2])
				ret, err := geomfn.DFullyWithin(a.Geometry, b.Geometry, float64(dist), geo.FnInclusive)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if every pair of points comprising geometry_a and geometry_b are within distance units, inclusive. " +
					"In other words, the ST_MaxDistance between geometry_a and geometry_b is less than or equal to distance units.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_dwithin": makeSTDWithinBuiltin(geo.FnInclusive),
	"st_equals": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.Equals,
			infoBuilder{
				info: "Returns true if geometry_a is spatially equal to geometry_b, " +
					"i.e. ST_Within(geometry_a, geometry_b) = ST_Within(geometry_b, geometry_a) = true.",
				libraryUsage: usesGEOS,
			},
		),
	),
	"st_orderingequals": makeBuiltin(
		defProps(),
		geometryOverload2BinaryPredicate(
			geomfn.OrderingEquals,
			infoBuilder{
				info: "Returns true if geometry_a is exactly equal to geometry_b, having all coordinates " +
					"in the same order, as well as the same type, SRID, bounding box, and so on.",
			},
		),
	),
	"st_normalize": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.Normalize(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns the geometry in its normalized form.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
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
			},
		),
		geographyOverload2BinaryPredicate(
			geogfn.Intersects,
			infoBuilder{
				info:         `Returns true if geography_a shares any portion of space with geography_b.`,
				libraryUsage: usesS2,
				precision:    "1cm",
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
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				pattern := tree.MustBeDString(args[2])
				ret, err := geomfn.RelatePattern(a.Geometry, b.Geometry, string(pattern))
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
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"bnr", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				bnr := tree.MustBeDInt(args[2])
				ret, err := geomfn.RelateBoundaryNodeRule(a.Geometry, b.Geometry, int(bnr))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: infoBuilder{
				info: `Returns the DE-9IM spatial relation between geometry_a and geometry_b using the given ` +
					`boundary node rule (1:OGC/MOD2, 2:Endpoint, 3:MultivalentEndpoint, 4:MonovalentEndpoint).`,
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_relatematch": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"intersection_matrix", types.String},
				{"pattern", types.String},
			},
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				matrix := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))

				matches, err := geomfn.MatchesDE9IM(matrix, pattern)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(matches)), nil
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Info: infoBuilder{
				info: "Returns whether the given DE-9IM intersection matrix satisfies the given pattern.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Validity checks
	//

	"st_isvalid": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.IsValid(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			types.Bool,
			infoBuilder{
				info:         `Returns whether the geometry is valid as defined by the OGC spec.`,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"flags", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				flags := int(tree.MustBeDInt(args[1]))
				validDetail, err := geomfn.IsValidDetail(g.Geometry, flags)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(validDetail.IsValid)), nil
			},
			Info: infoBuilder{
				info: `Returns whether the geometry is valid.

For flags=0, validity is defined by the OGC spec.

For flags=1, validity considers self-intersecting rings forming holes as valid as per ESRI. This is not valid under OGC and CRDB spatial operations may not operate correctly.`,
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_isvalidreason": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.IsValidReason(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			types.String,
			infoBuilder{
				info:         `Returns a string containing the reason the geometry is invalid along with the point of interest, or "Valid Geometry" if it is valid. Validity is defined by the OGC spec.`,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"flags", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				flags := int(tree.MustBeDInt(args[1]))
				validDetail, err := geomfn.IsValidDetail(g.Geometry, flags)
				if err != nil {
					return nil, err
				}
				if validDetail.IsValid {
					return tree.NewDString("Valid Geometry"), nil
				}
				return tree.NewDString(validDetail.Reason), nil
			},
			Info: infoBuilder{
				info: `Returns the reason the geometry is invalid or "Valid Geometry" if it is valid.

For flags=0, validity is defined by the OGC spec.

For flags=1, validity considers self-intersecting rings forming holes as valid as per ESRI. This is not valid under OGC and CRDB spatial operations may not operate correctly.`,
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_isvalidtrajectory": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.IsValidTrajectory(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			types.Bool,
			infoBuilder{
				info: `Returns whether the geometry encodes a valid trajectory.

Note the geometry must be a LineString with M coordinates.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_makevalid": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				validGeom, err := geomfn.MakeValid(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(validGeom), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns a valid form of the given geometry according to the OGC spec.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),

	//
	// Topology operations
	//

	"st_boundary": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				centroid, err := geomfn.Boundary(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(centroid), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns the closure of the combinatorial boundary of this Geometry.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
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
		)...,
	),
	"st_clipbybox2d": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"box2d", types.Box2D}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				bbox := tree.MustBeDBox2D(args[1])
				ret, err := geomfn.ClipByRect(g.Geometry, bbox.CartesianBoundingBox)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Clips the geometry to conform to the bounding box specified by box2d.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_convexhull": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				convexHull, err := geomfn.ConvexHull(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(convexHull), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns a geometry that represents the Convex Hull of the given geometry.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_difference": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				diff, err := geomfn.Difference(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(diff), err
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns the difference of two Geometries.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
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
		geographyOverload2(
			func(ctx *tree.EvalContext, a *tree.DGeography, b *tree.DGeography) (tree.Datum, error) {
				proj, err := geogfn.BestGeomProjection(a.Geography.BoundingRect().Union(b.Geography.BoundingRect()))
				if err != nil {
					return nil, err
				}
				aProj, err := geoprojbase.Projection(a.Geography.SRID())
				if err != nil {
					return nil, err
				}
				bProj, err := geoprojbase.Projection(b.Geography.SRID())
				if err != nil {
					return nil, err
				}
				geogDefaultProj, err := geoprojbase.Projection(geopb.DefaultGeographySRID)
				if err != nil {
					return nil, err
				}

				aInGeom, err := a.Geography.AsGeometry()
				if err != nil {
					return nil, err
				}
				bInGeom, err := b.Geography.AsGeometry()
				if err != nil {
					return nil, err
				}

				aInProjected, err := geotransform.Transform(
					aInGeom,
					aProj.Proj4Text,
					proj,
					a.Geography.SRID(),
				)
				if err != nil {
					return nil, err
				}
				bInProjected, err := geotransform.Transform(
					bInGeom,
					bProj.Proj4Text,
					proj,
					b.Geography.SRID(),
				)
				if err != nil {
					return nil, err
				}

				projectedIntersection, err := geomfn.Intersection(aInProjected, bInProjected)
				if err != nil {
					return nil, err
				}

				outGeom, err := geotransform.Transform(
					projectedIntersection,
					proj,
					geogDefaultProj.Proj4Text,
					geopb.DefaultGeographySRID,
				)
				if err != nil {
					return nil, err
				}
				ret, err := outGeom.AsGeography()
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(ret), nil
			},
			types.Geography,
			infoBuilder{
				info:         "Returns the point intersections of the given geographies." + usingBestGeomProjectionWarning,
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_sharedpaths": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.SharedPaths(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				geom := tree.NewDGeometry(ret)
				return geom, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns a collection containing paths shared by the two input geometries.

Those going in the same direction are in the first element of the collection,
those going in the opposite direction are in the second element.
The paths themselves are given in the direction of the first geometry.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_closestpoint": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.ClosestPoint(a.Geometry, b.Geometry)
				if err != nil {
					if geo.IsEmptyGeometryError(err) {
						return tree.DNull, nil
					}
					return nil, err
				}
				geom := tree.NewDGeometry(ret)
				return geom, nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns the 2-dimensional point on geometry_a that is closest to geometry_b. This is the first point of the shortest line.`,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_symdifference": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a, b *tree.DGeometry) (tree.Datum, error) {
				ret, err := geomfn.SymDifference(a.Geometry, b.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			types.Geometry,
			infoBuilder{
				info:         "Returns the symmetric difference of both geometries.",
				libraryUsage: usesGEOS,
			},
			tree.VolatilityImmutable,
		),
	),
	"st_simplify": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := float64(tree.MustBeDFloat(args[1]))
				// TODO(#spatial): post v21.1, use the geomfn.Simplify we have implemented internally.
				// GEOS currently preserves collapsed for linestrings and not for polygons.
				ret, err := geomfn.SimplifyGEOS(g.Geometry, tolerance)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: ret}, nil
			},
			Info: infoBuilder{
				info:         `Simplifies the given geometry using the Douglas-Peucker algorithm.`,
				libraryUsage: usesGEOS,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
				{"preserve_collapsed", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := float64(tree.MustBeDFloat(args[1]))
				preserveCollapsed := bool(tree.MustBeDBool(args[2]))
				ret, collapsed, err := geomfn.Simplify(g.Geometry, tolerance, preserveCollapsed)
				if err != nil {
					return nil, err
				}
				if collapsed {
					return tree.DNull, nil
				}
				return &tree.DGeometry{Geometry: ret}, nil
			},
			Info: infoBuilder{
				info: `Simplifies the given geometry using the Douglas-Peucker algorithm, retaining objects that would be too small given the tolerance if preserve_collapsed is set to true.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_simplifypreservetopology": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := float64(tree.MustBeDFloat(args[1]))
				ret, err := geomfn.SimplifyPreserveTopology(g.Geometry, tolerance)
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: ret}, nil
			},
			Info: infoBuilder{
				info:         `Simplifies the given geometry using the Douglas-Peucker algorithm, avoiding the creation of invalid geometries.`,
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
				g := tree.MustBeDGeometry(args[0])
				srid := tree.MustBeDInt(args[1])
				newGeom, err := g.Geometry.CloneWithSRID(geopb.SRID(srid))
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
				g := tree.MustBeDGeography(args[0])
				srid := tree.MustBeDInt(args[1])
				newGeom, err := g.Geography.CloneWithSRID(geopb.SRID(srid))
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
				g := tree.MustBeDGeometry(args[0])
				srid := geopb.SRID(tree.MustBeDInt(args[1]))

				fromProj, err := geoprojbase.Projection(g.SRID())
				if err != nil {
					return nil, err
				}
				toProj, err := geoprojbase.Projection(srid)
				if err != nil {
					return nil, err
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
				g := tree.MustBeDGeometry(args[0])
				toProj := string(tree.MustBeDString(args[1]))

				fromProj, err := geoprojbase.Projection(g.SRID())
				if err != nil {
					return nil, err
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
				g := tree.MustBeDGeometry(args[0])
				fromProj := string(tree.MustBeDString(args[1]))
				toProj := string(tree.MustBeDString(args[2]))

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
				g := tree.MustBeDGeometry(args[0])
				fromProj := string(tree.MustBeDString(args[1]))
				srid := geopb.SRID(tree.MustBeDInt(args[2]))

				toProj, err := geoprojbase.Projection(srid)
				if err != nil {
					return nil, err
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
	"st_translate": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"delta_x", types.Float},
				{"delta_y", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				deltaX := float64(tree.MustBeDFloat(args[1]))
				deltaY := float64(tree.MustBeDFloat(args[2]))

				ret, err := geomfn.Translate(g.Geometry, []float64{deltaX, deltaY})
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry translated by the given deltas.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_affine": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"a", types.Float},
				{"b", types.Float},
				{"d", types.Float},
				{"e", types.Float},
				{"x_off", types.Float},
				{"y_off", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				ret, err := geomfn.Affine(
					g.Geometry,
					geomfn.AffineMatrix([][]float64{
						{float64(tree.MustBeDFloat(args[1])), float64(tree.MustBeDFloat(args[2])), 0, float64(tree.MustBeDFloat(args[5]))},
						{float64(tree.MustBeDFloat(args[3])), float64(tree.MustBeDFloat(args[4])), 0, float64(tree.MustBeDFloat(args[6]))},
						{0, 0, 1, 0},
						{0, 0, 0, 1},
					}),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Applies a 2D affine transformation to the given geometry.

The matrix transformation will be applied as follows for each coordinate:
/ a  b  x_off \  / x \
| d  e  y_off |  | y |
\ 0  0      1 /  \ 0 /
				`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"a", types.Float},
				{"b", types.Float},
				{"c", types.Float},
				{"d", types.Float},
				{"e", types.Float},
				{"f", types.Float},
				{"g", types.Float},
				{"h", types.Float},
				{"i", types.Float},
				{"x_off", types.Float},
				{"y_off", types.Float},
				{"z_off", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				ret, err := geomfn.Affine(
					g.Geometry,
					geomfn.AffineMatrix([][]float64{
						{float64(tree.MustBeDFloat(args[1])), float64(tree.MustBeDFloat(args[2])), float64(tree.MustBeDFloat(args[3])), float64(tree.MustBeDFloat(args[10]))},
						{float64(tree.MustBeDFloat(args[4])), float64(tree.MustBeDFloat(args[5])), float64(tree.MustBeDFloat(args[6])), float64(tree.MustBeDFloat(args[11]))},
						{float64(tree.MustBeDFloat(args[7])), float64(tree.MustBeDFloat(args[8])), float64(tree.MustBeDFloat(args[9])), float64(tree.MustBeDFloat(args[12]))},
						{0, 0, 0, 1},
					}),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Applies a 3D affine transformation to the given geometry.

The matrix transformation will be applied as follows for each coordinate:
/ a  b  c x_off \  / x \
| d  e  f y_off |  | y |
| g  h  i z_off |  | z |
\ 0  0  0     1 /  \ 0 /
				`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_scale": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"x_factor", types.Float},
				{"y_factor", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				xFactor := float64(tree.MustBeDFloat(args[1]))
				yFactor := float64(tree.MustBeDFloat(args[2]))

				ret, err := geomfn.Scale(g.Geometry, []float64{xFactor, yFactor})
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry scaled by the given factors.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"factor", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				factor, err := tree.MustBeDGeometry(args[1]).AsGeomT()
				if err != nil {
					return nil, err
				}

				pointFactor, ok := factor.(*geom.Point)
				if !ok {
					return nil, errors.Newf("a Point must be used as the scaling factor")
				}

				factors := pointFactor.FlatCoords()
				if len(factors) < 2 {
					// Scale by 0, and leave Z untouched (this matches the behavior of
					// ScaleRelativeToOrigin).
					factors = []float64{0, 0, 1}
				}
				ret, err := geomfn.Scale(g.Geometry, factors)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry scaled by taking in a Geometry as the factor.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"factor", types.Geometry},
				{"origin", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				factor := tree.MustBeDGeometry(args[1])
				origin := tree.MustBeDGeometry(args[2])

				ret, err := geomfn.ScaleRelativeToOrigin(g.Geometry, factor.Geometry, origin.Geometry)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry scaled by the Geometry factor relative to a false origin.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_rotate": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"angle_radians", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				rotRadians := float64(tree.MustBeDFloat(args[1]))

				ret, err := geomfn.Rotate(g.Geometry, rotRadians)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry whose coordinates are rotated around the origin by a rotation angle.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"angle_radians", types.Float},
				{"origin_x", types.Float},
				{"origin_y", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				rotRadians := float64(tree.MustBeDFloat(args[1]))
				x := float64(tree.MustBeDFloat(args[2]))
				y := float64(tree.MustBeDFloat(args[3]))
				geometry, err := geomfn.RotateWithXY(g.Geometry, rotRadians, x, y)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geometry), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry whose coordinates are rotated around the provided origin by a rotation angle.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"angle_radians", types.Float},
				{"origin_point", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				rotRadians := float64(tree.MustBeDFloat(args[1]))
				originPoint := tree.MustBeDGeometry(args[2])

				ret, err := geomfn.RotateWithPointOrigin(g.Geometry, rotRadians, originPoint.Geometry)
				if errors.Is(err, geomfn.ErrPointOriginEmpty) {
					return tree.DNull, nil
				}

				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry whose coordinates are rotated around the provided origin by a rotation angle.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_rotatex": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"angle_radians", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				rotRadians := float64(tree.MustBeDFloat(args[1]))

				ret, err := geomfn.RotateX(g.Geometry, rotRadians)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry whose coordinates are rotated about the x axis by a rotation angle.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_rotatey": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"angle_radians", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				rotRadians := float64(tree.MustBeDFloat(args[1]))

				ret, err := geomfn.RotateY(g.Geometry, rotRadians)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry whose coordinates are rotated about the y axis by a rotation angle.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_rotatez": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"g", types.Geometry},
				{"angle_radians", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				rotRadians := float64(tree.MustBeDFloat(args[1]))

				ret, err := geomfn.RotateZ(g.Geometry, rotRadians)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified Geometry whose coordinates are rotated about the z axis by a rotation angle.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_addpoint": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"line_string", types.Geometry},
				{"point", types.Geometry},
				{"index", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lineString := tree.MustBeDGeometry(args[0])
				point := tree.MustBeDGeometry(args[1])
				index := int(tree.MustBeDInt(args[2]))

				ret, err := geomfn.AddPoint(lineString.Geometry, index, point.Geometry)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Adds a Point to a LineString at the given 0-based index (-1 to append).`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"line_string", types.Geometry},
				{"point", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lineString := tree.MustBeDGeometry(args[0])
				point := tree.MustBeDGeometry(args[1])

				ret, err := geomfn.AddPoint(lineString.Geometry, -1, point.Geometry)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Adds a Point to the end of a LineString.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_setpoint": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"line_string", types.Geometry},
				{"index", types.Int},
				{"point", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lineString := tree.MustBeDGeometry(args[0])
				index := int(tree.MustBeDInt(args[1]))
				point := tree.MustBeDGeometry(args[2])

				ret, err := geomfn.SetPoint(lineString.Geometry, index, point.Geometry)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Sets the Point at the given 0-based index and returns the modified LineString geometry.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_removepoint": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"line_string", types.Geometry},
				{"index", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				lineString := tree.MustBeDGeometry(args[0])
				index := int(tree.MustBeDInt(args[1]))

				ret, err := geomfn.RemovePoint(lineString.Geometry, index)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Removes the Point at the given 0-based index and returns the modified LineString geometry.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_removerepeatedpoints": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := float64(tree.MustBeDFloat(args[1]))

				ret, err := geomfn.RemoveRepeatedPoints(g.Geometry, tolerance)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a geometry with repeated points removed, within the given distance tolerance.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				ret, err := geomfn.RemoveRepeatedPoints(g.Geometry, 0)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a geometry with repeated points removed.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_reverse": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				geometry := tree.MustBeDGeometry(args[0])

				ret, err := geomfn.Reverse(geometry.Geometry)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a modified geometry by reversing the order of its vertices.`,
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
				g := tree.MustBeDGeography(args[0])
				segmentMaxLength := float64(tree.MustBeDFloat(args[1]))
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
				g := tree.MustBeDGeometry(args[0])
				segmentMaxLength := float64(tree.MustBeDFloat(args[1]))
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
	"st_snaptogrid": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"size", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				size := float64(tree.MustBeDFloat(args[1]))
				ret, err := geomfn.SnapToGrid(g.Geometry, geom.Coord{0, 0, 0, 0}, geom.Coord{size, size, 0, 0})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Snap a geometry to a grid of the given size. " +
					"The specified size is only used to snap X and Y coordinates.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"size_x", types.Float},
				{"size_y", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				sizeX := float64(tree.MustBeDFloat(args[1]))
				sizeY := float64(tree.MustBeDFloat(args[2]))
				ret, err := geomfn.SnapToGrid(g.Geometry, geom.Coord{0, 0, 0, 0}, geom.Coord{sizeX, sizeY, 0, 0})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Snap a geometry to a grid of with X coordinates snapped to size_x and Y coordinates snapped to size_y.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"origin_x", types.Float},
				{"origin_y", types.Float},
				{"size_x", types.Float},
				{"size_y", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				originX := float64(tree.MustBeDFloat(args[1]))
				originY := float64(tree.MustBeDFloat(args[2]))
				sizeX := float64(tree.MustBeDFloat(args[3]))
				sizeY := float64(tree.MustBeDFloat(args[4]))
				ret, err := geomfn.SnapToGrid(g.Geometry, geom.Coord{originX, originY, 0, 0}, geom.Coord{sizeX, sizeY, 0, 0})
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Snap a geometry to a grid of with X coordinates snapped to size_x and Y coordinates snapped to size_y based on an origin of (origin_x, origin_y).",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"origin", types.Geometry},
				{"size_x", types.Float},
				{"size_y", types.Float},
				{"size_z", types.Float},
				{"size_m", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				origin := tree.MustBeDGeometry(args[1])
				sizeX := float64(tree.MustBeDFloat(args[2]))
				sizeY := float64(tree.MustBeDFloat(args[3]))
				sizeZ := float64(tree.MustBeDFloat(args[4]))
				sizeM := float64(tree.MustBeDFloat(args[5]))
				originT, err := origin.Geometry.AsGeomT()
				if err != nil {
					return nil, err
				}
				switch originT := originT.(type) {
				case *geom.Point:
					// Prevent nil dereference if origin is an empty point.
					if originT.Empty() {
						originT = geom.NewPoint(originT.Layout())
					}
					ret, err := geomfn.SnapToGrid(
						g.Geometry,
						geom.Coord{originT.X(), originT.Y(), originT.Z(), originT.M()},
						geom.Coord{sizeX, sizeY, sizeZ, sizeM})
					if err != nil {
						return nil, err
					}
					return tree.NewDGeometry(ret), nil
				default:
					return nil, errors.Newf("origin must be a POINT")
				}
			},
			Info: infoBuilder{
				info: "Snap a geometry to a grid defined by the given origin and X, Y, Z, and M cell sizes. " +
					"Any dimension with a 0 cell size will not be snapped.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_snap": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"input", types.Geometry},
				{"target", types.Geometry},
				{"tolerance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g1 := tree.MustBeDGeometry(args[0])
				g2 := tree.MustBeDGeometry(args[1])
				tolerance := tree.MustBeDFloat(args[2])
				ret, err := geomfn.Snap(g1.Geometry, g2.Geometry, float64(tolerance))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Snaps the vertices and segments of input geometry the target geometry's vertices.
Tolerance is used to control where snapping is performed. The result geometry is the input geometry with the vertices snapped. 
If no snapping occurs then the input geometry is returned unchanged.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_buffer": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"distance", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				distance := tree.MustBeDInt(args[1])

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
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				distance := tree.MustBeDFloat(args[1])

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
				{"distance", types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				distanceDec := tree.MustBeDDecimal(args[1])

				distance, err := distanceDec.Float64()
				if err != nil {
					return nil, err
				}

				ret, err := geomfn.Buffer(g.Geometry, geomfn.MakeDefaultBufferParams(), distance)
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
				g := tree.MustBeDGeometry(args[0])
				distance := tree.MustBeDFloat(args[1])
				quadSegs := tree.MustBeDInt(args[2])

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
				g := tree.MustBeDGeometry(args[0])
				distance := tree.MustBeDFloat(args[1])
				paramsString := tree.MustBeDString(args[2])

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
				{"geography", types.Geography},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeography(args[0])
				distance := tree.MustBeDFloat(args[1])

				ret, err := performGeographyOperationUsingBestGeomProjection(
					g.Geography,
					func(g geo.Geometry) (geo.Geometry, error) {
						return geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), float64(distance))
					},
				)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeography(ret), nil
			},
			Info:       stBufferInfoBuilder.String() + usingBestGeomProjectionWarning,
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"distance", types.Float},
				{"quad_segs", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeography(args[0])
				distance := tree.MustBeDFloat(args[1])
				quadSegs := tree.MustBeDInt(args[2])

				ret, err := performGeographyOperationUsingBestGeomProjection(
					g.Geography,
					func(g geo.Geometry) (geo.Geometry, error) {
						return geomfn.Buffer(
							g,
							geomfn.MakeDefaultBufferParams().WithQuadrantSegments(int(quadSegs)),
							float64(distance),
						)
					},
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(ret), nil
			},
			Info:       stBufferWithQuadSegInfoBuilder.String() + usingBestGeomProjectionWarning,
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geography", types.Geography},
				{"distance", types.Float},
				{"buffer_style_params", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geography),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeography(args[0])
				distance := tree.MustBeDFloat(args[1])
				paramsString := tree.MustBeDString(args[2])

				params, modifiedDistance, err := geomfn.ParseBufferParams(string(paramsString), float64(distance))
				if err != nil {
					return nil, err
				}

				ret, err := performGeographyOperationUsingBestGeomProjection(
					g.Geography,
					func(g geo.Geometry) (geo.Geometry, error) {
						return geomfn.Buffer(
							g,
							params,
							modifiedDistance,
						)
					},
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeography(ret), nil
			},
			Info:       stBufferWithParamsInfoBuilder.String() + usingBestGeomProjectionWarning,
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_envelope": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(ctx *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				envelope, err := geomfn.Envelope(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(envelope), nil
			},
			types.Geometry,
			infoBuilder{
				info: `Returns a bounding envelope for the given geometry.

For geometries which have a POINT or LINESTRING bounding box (i.e. is a single point
or a horizontal or vertical line), a POINT or LINESTRING is returned. Otherwise, the
returned POLYGON will be ordered Bottom Left, Top Left, Top Right, Bottom Right,
Bottom Left.`,
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types: tree.ArgTypes{
				{"box2d", types.Box2D},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bbox := tree.MustBeDBox2D(args[0]).CartesianBoundingBox
				ret, err := geo.MakeGeometryFromGeomT(bbox.ToGeomT(0 /* SRID */))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Returns a bounding geometry for the given box.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_flipcoordinates": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])

				ret, err := geomfn.FlipCoordinates(g.Geometry)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Returns a new geometry with the X and Y axes flipped.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_swapordinates": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{
					Name: "geometry",
					Typ:  types.Geometry,
				},
				{
					Name: "swap_ordinate_string",
					Typ:  types.String,
				},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				cString := tree.MustBeDString(args[1])

				ret, err := geomfn.SwapOrdinates(g.Geometry, string(cString))
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a version of the given geometry with given ordinates swapped.
The swap_ordinate_string parameter is a 2-character string naming the ordinates to swap. Valid names are: x, y, z and m.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	"st_angle": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"point1", types.Geometry},
				{"point2", types.Geometry},
				{"point3", types.Geometry},
				{"point4", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g1 := tree.MustBeDGeometry(args[0]).Geometry
				g2 := tree.MustBeDGeometry(args[1]).Geometry
				g3 := tree.MustBeDGeometry(args[2]).Geometry
				g4 := tree.MustBeDGeometry(args[3]).Geometry
				angle, err := geomfn.Angle(g1, g2, g3, g4)
				if err != nil {
					return nil, err
				}
				if angle == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*angle)), nil
			},
			Info: infoBuilder{
				info: `Returns the clockwise angle between the vectors formed by point1,point2 and point3,point4. ` +
					`The arguments must be POINT geometries. Returns NULL if any vectors have 0 length.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"point1", types.Geometry},
				{"point2", types.Geometry},
				{"point3", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g1 := tree.MustBeDGeometry(args[0]).Geometry
				g2 := tree.MustBeDGeometry(args[1]).Geometry
				g3 := tree.MustBeDGeometry(args[2]).Geometry
				g4, err := geo.MakeGeometryFromGeomT(geom.NewPointEmpty(geom.XY))
				if err != nil {
					return nil, err
				}
				angle, err := geomfn.Angle(g1, g2, g3, g4)
				if err != nil {
					return nil, err
				}
				if angle == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*angle)), nil
			},
			Info: infoBuilder{
				info: `Returns the clockwise angle between the vectors formed by point2,point1 and point2,point3. ` +
					`The arguments must be POINT geometries. Returns NULL if any vectors have 0 length.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"line1", types.Geometry},
				{"line2", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g1 := tree.MustBeDGeometry(args[0]).Geometry
				g2 := tree.MustBeDGeometry(args[1]).Geometry
				angle, err := geomfn.AngleLineString(g1, g2)
				if err != nil {
					return nil, err
				}
				if angle == nil {
					return tree.DNull, nil
				}
				return tree.NewDFloat(tree.DFloat(*angle)), nil
			},
			Info: infoBuilder{
				info: `Returns the clockwise angle between two LINESTRING geometries, treating them as vectors ` +
					`between their start- and endpoints. Returns NULL if any vectors have 0 length.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	"st_asencodedpolyline": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0]).Geometry
				s, err := geo.GeometryToEncodedPolyline(g, 5)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(s), nil
			},
			Info: infoBuilder{
				info: `Returns the geometry as an Encoded Polyline.
This format is used by Google Maps with precision=5 and by Open Source Routing Machine with precision=5 and 6.
Preserves 5 decimal places.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"precision", types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0]).Geometry
				p := int(tree.MustBeDInt(args[1]))
				s, err := geo.GeometryToEncodedPolyline(g, p)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(s), nil
			},
			Info: infoBuilder{
				info: `Returns the geometry as an Encoded Polyline.
This format is used by Google Maps with precision=5 and by Open Source Routing Machine with precision=5 and 6.
Precision specifies how many decimal places will be preserved in Encoded Polyline. Value should be the same on encoding and decoding, or coordinates will be incorrect.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_linefromencodedpolyline": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"encoded_polyline", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				p := 5
				g, err := geo.ParseEncodedPolyline(s, p)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: `Creates a LineString from an Encoded Polyline string.

Returns valid results only if the polyline was encoded with 5 decimal places.

See http://developers.google.com/maps/documentation/utilities/polylinealgorithm`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"encoded_polyline", types.String},
				{"precision", types.Int4},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				p := int(tree.MustBeDInt(args[1]))
				g, err := geo.ParseEncodedPolyline(s, p)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(g), nil
			},
			Info: infoBuilder{
				info: `Creates a LineString from an Encoded Polyline string.

Precision specifies how many decimal places will be preserved in Encoded Polyline. Value should be the same on encoding and decoding, or coordinates will be incorrect.

See http://developers.google.com/maps/documentation/utilities/polylinealgorithm`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_unaryunion": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				res, err := geomfn.UnaryUnion(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(res), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns a union of the components for any geometry or geometry collection provided. Dissolves boundaries of a multipolygon.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_node": makeBuiltin(
		defProps(),
		geometryOverload1(
			func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				res, err := geomfn.Node(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(res), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Adds a node on a geometry for each intersection. Resulting geometry is always a MultiLineString.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_subdivide": makeBuiltin(
		genProps(),
		makeGeneratorOverload(
			tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			types.Geometry,
			makeSubdividedGeometriesGeneratorFactory(false /* expectMaxVerticesArg */),
			"Returns a geometry divided into parts, where each part contains no more than 256 vertices.",
			tree.VolatilityImmutable,
		),
		makeGeneratorOverload(
			tree.ArgTypes{
				{"geometry", types.Geometry},
				{"max_vertices", types.Int4},
			},
			types.Geometry,
			makeSubdividedGeometriesGeneratorFactory(true /* expectMaxVerticesArg */),
			"Returns a geometry divided into parts, where each part contains no more than the number of vertices provided.",
			tree.VolatilityImmutable,
		),
	),

	//
	// BoundingBox
	//

	"st_makebox2d": makeBuiltin(
		defProps(),
		geometryOverload2(
			func(ctx *tree.EvalContext, a *tree.DGeometry, b *tree.DGeometry) (tree.Datum, error) {
				if a.Geometry.SRID() != b.Geometry.SRID() {
					return nil, geo.NewMismatchingSRIDsError(a.Geometry.SpatialObject(), b.Geometry.SpatialObject())
				}
				aGeomT, err := a.AsGeomT()
				if err != nil {
					return nil, err
				}
				bGeomT, err := b.AsGeomT()
				if err != nil {
					return nil, err
				}

				switch aGeomT := aGeomT.(type) {
				case *geom.Point:
					switch bGeomT := bGeomT.(type) {
					case *geom.Point:
						if aGeomT.Empty() || bGeomT.Empty() {
							return nil, errors.Newf("cannot use POINT EMPTY")
						}
						bbox := a.CartesianBoundingBox().Combine(b.CartesianBoundingBox())
						return tree.NewDBox2D(*bbox), nil
					default:
						return nil, errors.Newf("second argument is not a POINT")
					}
				default:
					return nil, errors.Newf("first argument is not a POINT")
				}
			},
			types.Box2D,
			infoBuilder{
				info: "Creates a box2d from two points. Errors if arguments are not two non-empty points.",
			},
			tree.VolatilityImmutable,
		),
	),
	"st_combinebbox": makeBuiltin(
		tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.ArgTypes{{"box2d", types.Box2D}, {"geometry", types.Geometry}},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[1] == tree.DNull {
					return args[0], nil
				}
				if args[0] == tree.DNull {
					bbox := tree.MustBeDGeometry(args[1]).CartesianBoundingBox()
					if bbox == nil {
						return tree.DNull, nil
					}
					return tree.NewDBox2D(*bbox), nil
				}
				bbox := &tree.MustBeDBox2D(args[0]).CartesianBoundingBox
				g := tree.MustBeDGeometry(args[1])
				bbox = bbox.Combine(g.CartesianBoundingBox())
				if bbox == nil {
					return tree.DNull, nil
				}
				return tree.NewDBox2D(*bbox), nil
			},
			Info: infoBuilder{
				info: "Combines the current bounding box with the bounding box of the Geometry.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_expand": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"box2d", types.Box2D}, {"delta", types.Float}},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bbox := tree.MustBeDBox2D(args[0])
				delta := float64(tree.MustBeDFloat(args[1]))
				bboxBuffered := bbox.Buffer(delta, delta)
				if bboxBuffered == nil {
					return tree.DNull, nil
				}
				return tree.NewDBox2D(*bboxBuffered), nil
			},
			Info: infoBuilder{
				info: "Extends the box2d by delta units across all dimensions.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"box2d", types.Box2D},
				{"delta_x", types.Float},
				{"delta_y", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bbox := tree.MustBeDBox2D(args[0])
				deltaX := float64(tree.MustBeDFloat(args[1]))
				deltaY := float64(tree.MustBeDFloat(args[2]))
				bboxBuffered := bbox.Buffer(deltaX, deltaY)
				if bboxBuffered == nil {
					return tree.DNull, nil
				}
				return tree.NewDBox2D(*bboxBuffered), nil
			},
			Info: infoBuilder{
				info: "Extends the box2d by delta_x units in the x dimension and delta_y units in the y dimension.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {"delta", types.Float}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				delta := float64(tree.MustBeDFloat(args[1]))
				if g.Empty() {
					return g, nil
				}
				bbox := g.CartesianBoundingBox().Buffer(delta, delta)
				ret, err := geo.MakeGeometryFromGeomT(bbox.ToGeomT(g.SRID()))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Extends the bounding box represented by the geometry by delta units across all dimensions, returning a Polygon representing the new bounding box.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"delta_x", types.Float},
				{"delta_y", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				deltaX := float64(tree.MustBeDFloat(args[1]))
				deltaY := float64(tree.MustBeDFloat(args[2]))
				if g.Empty() {
					return g, nil
				}
				bbox := g.CartesianBoundingBox().Buffer(deltaX, deltaY)
				ret, err := geo.MakeGeometryFromGeomT(bbox.ToGeomT(g.SRID()))
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: "Extends the bounding box represented by the geometry by delta_x units in the x dimension and delta_y units in the y dimension, returning a Polygon representing the new bounding box.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Table metadata
	//

	"st_estimatedextent": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySpatial,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"schema_name", types.String},
				{"table_name", types.String},
				{"geocolumn_name", types.String},
				{"parent_only", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(#64257): implement by looking at statistics.
				return tree.DNull, nil
			},
			Info: infoBuilder{
				info: `Returns the estimated extent of the geometries in the column of the given table. This currently always returns NULL.

The parent_only boolean is always ignored.`,
			}.String(),
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"schema_name", types.String},
				{"table_name", types.String},
				{"geocolumn_name", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(#64257): implement by looking at statistics.
				return tree.DNull, nil
			},
			Info: infoBuilder{
				info: `Returns the estimated extent of the geometries in the column of the given table. This currently always returns NULL.`,
			}.String(),
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"table_name", types.String},
				{"geocolumn_name", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Box2D),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(#64257): implement by looking at statistics.
				return tree.DNull, nil
			},
			Info: infoBuilder{
				info: `Returns the estimated extent of the geometries in the column of the given table. This currently always returns NULL.`,
			}.String(),
			Volatility: tree.VolatilityStable,
		},
	),

	//
	// Schema changes
	//

	"addgeometrycolumn": makeBuiltin(
		tree.FunctionProperties{
			Class:    tree.SQLClass,
			Category: categorySpatial,
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
	"st_dwithinexclusive": makeSTDWithinBuiltin(geo.FnExclusive),
	"st_dfullywithinexclusive": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				dist := tree.MustBeDFloat(args[2])
				ret, err := geomfn.DFullyWithin(a.Geometry, b.Geometry, float64(dist), geo.FnExclusive)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if every pair of points comprising geometry_a and geometry_b are within distance units, exclusive. " +
					"In other words, the ST_MaxDistance between geometry_a and geometry_b is less than distance units.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_pointinsidecircle": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"x_coord", types.Float},
				{"y_coord", types.Float},
				{"radius", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				point := tree.MustBeDGeometry(args[0])

				geomT, err := point.AsGeomT()
				if err != nil {
					return nil, err
				}

				if _, ok := geomT.(*geom.Point); !ok {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"first parameter has to be of type Point",
					)
				}

				x := float64(tree.MustBeDFloat(args[1]))
				y := float64(tree.MustBeDFloat(args[2]))
				radius := float64(tree.MustBeDFloat(args[3]))
				if radius <= 0 {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"radius of the circle has to be positive",
					)
				}
				center, err := geo.MakeGeometryFromPointCoords(x, y)
				if err != nil {
					return nil, err
				}
				dist, err := geomfn.MinDistance(point.Geometry, center)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(dist <= radius), nil
			},
			Info: infoBuilder{
				info: "Returns the true if the geometry is a point and is inside the circle. Returns false otherwise.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	"st_memsize": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{
					Name: "geometry",
					Typ:  types.Geometry,
				},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				geo := tree.MustBeDGeometry(args[0])
				return tree.NewDInt(tree.DInt(geo.Size())), nil
			},
			Info:       "Returns the amount of memory space (in bytes) the geometry takes.",
			Volatility: tree.VolatilityImmutable,
		}),

	"st_linelocatepoint": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{
					Name: "line",
					Typ:  types.Geometry,
				},
				{
					Name: "point",
					Typ:  types.Geometry,
				},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				line := tree.MustBeDGeometry(args[0])
				p := tree.MustBeDGeometry(args[1])

				// compute fraction of new line segment compared to total line length
				fraction, err := geomfn.LineLocatePoint(line.Geometry, p.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(fraction)), nil
			},
			Info: "Returns a float between 0 and 1 representing the location of the closest point " +
				"on LineString to the given Point, as a fraction of total 2d line length.",
			Volatility: tree.VolatilityImmutable,
		}),

	"st_minimumboundingradius": makeBuiltin(genProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}},
			ReturnType: tree.FixedReturnType(minimumBoundingRadiusReturnType),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return nil, newUnsuitableUseOfGeneratorError()
			},
			Generator:  makeMinimumBoundGenerator,
			Info:       "Returns a record containing the center point and radius of the smallest circle that can fully contains the given geometry.",
			Volatility: tree.VolatilityImmutable,
		}),

	"st_minimumboundingcircle": makeBuiltin(defProps(),
		geometryOverload1(
			func(evalContext *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
				polygon, _, _, err := geomfn.MinimumBoundingCircle(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(polygon), nil
			},
			types.Geometry,
			infoBuilder{
				info: "Returns the smallest circle polygon that can fully contain a geometry.",
			},
			tree.VolatilityImmutable,
		),
		tree.Overload{
			Types:      tree.ArgTypes{{"geometry", types.Geometry}, {" num_segs", types.Int}},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Info: infoBuilder{
				info: "Returns the smallest circle polygon that can fully contain a geometry.",
			}.String(),
			Volatility: tree.VolatilityImmutable,
			Fn: func(evalContext *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				numOfSeg := tree.MustBeDInt(args[1])
				_, centroid, radius, err := geomfn.MinimumBoundingCircle(g.Geometry)
				if err != nil {
					return nil, err
				}

				polygon, err := geomfn.Buffer(
					centroid,
					geomfn.MakeDefaultBufferParams().WithQuadrantSegments(int(numOfSeg)),
					radius,
				)
				if err != nil {
					return nil, err
				}

				return tree.NewDGeometry(polygon), nil
			},
		},
	),
	"st_transscale": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"delta_x", types.Float},
				{"delta_y", types.Float},
				{"x_factor", types.Float},
				{"y_factor", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Volatility: tree.VolatilityImmutable,
			Info: infoBuilder{
				info: "Translates the geometry using the deltaX and deltaY args, then scales it using the XFactor, YFactor args, working in 2D only.",
			}.String(),
			Fn: func(_ *tree.EvalContext, datums tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(datums[0])
				deltaX := float64(tree.MustBeDFloat(datums[1]))
				deltaY := float64(tree.MustBeDFloat(datums[2]))
				xFactor := float64(tree.MustBeDFloat(datums[3]))
				yFactor := float64(tree.MustBeDFloat(datums[4]))
				geometry, err := geomfn.TransScale(g.Geometry, deltaX, deltaY, xFactor, yFactor)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geometry), nil
			},
		}),
	"st_voronoipolygons": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				var env *geo.Geometry
				ret, err := geomfn.VoronoiDiagram(g.Geometry, env, 0.0, false /* onlyEdges */)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := tree.MustBeDFloat(args[1])
				var env *geo.Geometry
				ret, err := geomfn.VoronoiDiagram(g.Geometry, env, float64(tolerance), false /* onlyEdges */)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
				{"extend_to", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := tree.MustBeDFloat(args[1])
				env := tree.MustBeDGeometry(args[2])
				ret, err := geomfn.VoronoiDiagram(g.Geometry, &env.Geometry, float64(tolerance), false /* onlyEdges */)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_voronoilines": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				var env *geo.Geometry
				ret, err := geomfn.VoronoiDiagram(g.Geometry, env, 0.0, true /* onlyEdges */)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry as` +
					`the boundaries between cells in that diagram as a MultiLineString.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := tree.MustBeDFloat(args[1])
				var env *geo.Geometry
				ret, err := geomfn.VoronoiDiagram(g.Geometry, env, float64(tolerance), true /* onlyEdges */)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry as` +
					`the boundaries between cells in that diagram as a MultiLineString.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
				{"tolerance", types.Float},
				{"extend_to", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				tolerance := tree.MustBeDFloat(args[1])
				env := tree.MustBeDGeometry(args[2])
				ret, err := geomfn.VoronoiDiagram(g.Geometry, &env.Geometry, float64(tolerance), true /* onlyEdges */)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a two-dimensional Voronoi diagram from the vertices of the supplied geometry as` +
					`the boundaries between cells in that diagram as a MultiLineString.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_orientedenvelope": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(args[0])
				ret, err := geomfn.MinimumRotatedRectangle(g.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(ret), nil
			},
			Info: infoBuilder{
				info: `Returns a minimum rotated rectangle enclosing a geometry.
Note that more than one minimum rotated rectangle may exist.
May return a Point or LineString in the case of degenerate inputs.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),
	"st_linesubstring": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"linestring", types.Geometry},
				{"start_fraction", types.Float},
				{"end_fraction", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Volatility: tree.VolatilityImmutable,
			Info: infoBuilder{
				info: "Return a linestring being a substring of the input one starting and ending at the given fractions of total 2D length. Second and third arguments are float8 values between 0 and 1.",
			}.String(),
			Fn: func(_ *tree.EvalContext, datums tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(datums[0])
				startFraction := float64(tree.MustBeDFloat(datums[1]))
				endFraction := float64(tree.MustBeDFloat(datums[2]))
				geometry, err := geomfn.LineSubstring(g.Geometry, startFraction, endFraction)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geometry), nil
			},
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"linestring", types.Geometry},
				{"start_fraction", types.Decimal},
				{"end_fraction", types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Geometry),
			Volatility: tree.VolatilityImmutable,
			Info: infoBuilder{
				info: "Return a linestring being a substring of the input one starting and ending at the given fractions of total 2D length. Second and third arguments are float8 values between 0 and 1.",
			}.String(),
			Fn: func(_ *tree.EvalContext, datums tree.Datums) (tree.Datum, error) {
				g := tree.MustBeDGeometry(datums[0])
				startFraction := tree.MustBeDDecimal(datums[1])
				startFractionFloat, err := startFraction.Float64()
				if err != nil {
					return nil, err
				}
				endFraction := tree.MustBeDDecimal(datums[2])
				endFractionFloat, err := endFraction.Float64()
				if err != nil {
					return nil, err
				}
				// PostGIS returns nil for empty linestrings.
				if g.Empty() && g.ShapeType2D() == geopb.ShapeType_LineString {
					return tree.DNull, nil
				}
				geometry, err := geomfn.LineSubstring(g.Geometry, startFractionFloat, endFractionFloat)
				if err != nil {
					return nil, err
				}
				return tree.NewDGeometry(geometry), nil
			},
		}),

	"st_linecrossingdirection": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"linestring_a", types.Geometry},
				{"linestring_b", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				linestringA := tree.MustBeDGeometry(args[0])
				linestringB := tree.MustBeDGeometry(args[1])

				ret, err := geomfn.LineCrossingDirection(linestringA.Geometry, linestringB.Geometry)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(ret)), nil
			},
			Info: infoBuilder{
				info: `Returns an interger value defining behavior of crossing of lines: 
0: lines do not cross,
-1: linestring_b crosses linestring_a from right to left,
1: linestring_b crosses linestring_a from left to right,
-2: linestring_b crosses linestring_a multiple times from right to left,
2: linestring_b crosses linestring_a multiple times from left to right,
-3: linestring_b crosses linestring_a multiple times from left to left,
3: linestring_b crosses linestring_a multiple times from right to right.

Note that the top vertex of the segment touching another line does not count as a crossing, but the bottom vertex of segment touching another line is considered a crossing.`,
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	),

	//
	// Unimplemented.
	//

	"st_asgml":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48877}),
	"st_aslatlontext":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48882}),
	"st_assvg":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48883}),
	"st_boundingdiagonal":    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48889}),
	"st_buildarea":           makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48892}),
	"st_chaikinsmoothing":    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48894}),
	"st_cleangeometry":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48895}),
	"st_clusterdbscan":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48898}),
	"st_clusterintersecting": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48899}),
	"st_clusterkmeans":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48900}),
	"st_clusterwithin":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48901}),
	"st_concavehull":         makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48906}),
	"st_delaunaytriangles":   makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48915}),
	"st_dump":                makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49785}),
	"st_dumppoints":          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49786}),
	"st_dumprings":           makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49787}),
	"st_geometricmedian":     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48944}),
	"st_interpolatepoint":    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48950}),
	"st_isvaliddetail":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48962}),
	"st_length2dspheroid":    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48967}),
	"st_lengthspheroid":      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48968}),
	"st_polygonize":          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49011}),
	"st_quantizecoordinates": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49012}),
	"st_seteffectivearea":    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49030}),
	"st_simplifyvw":          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49039}),
	"st_split":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49045}),
	"st_tileenvelope":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49053}),
	"st_wrapx":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 49068}),
	"st_bdpolyfromtext":      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48801}),
	"st_geomfromgml":         makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48807}),
	"st_geomfromtwkb":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48809}),
	"st_gmltosql":            makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 48810}),
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
				info: "Compatibility placeholder function with PostGIS. Returns a fixed string based on PostGIS 3.0.1, with minor edits.",
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
			a := tree.MustBeDGeometry(args[0])
			return f(ctx, a)
		},
		Info:       ib.String(),
		Volatility: volatility,
	}
}

// geometryOverload1UnaryPredicate hides the boilerplate for builtins
// operating on one geometry wrapping a unary predicate.
func geometryOverload1UnaryPredicate(
	f func(geo.Geometry) (bool, error), ib infoBuilder,
) tree.Overload {
	return geometryOverload1(
		func(_ *tree.EvalContext, g *tree.DGeometry) (tree.Datum, error) {
			ret, err := f(g.Geometry)
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
			a := tree.MustBeDGeometry(args[0])
			b := tree.MustBeDGeometry(args[1])
			return f(ctx, a, b)
		},
		Info:       ib.String(),
		Volatility: volatility,
	}
}

// geometryOverload2BinaryPredicate hides the boilerplate for builtins
// operating on two geometries and the overlap wraps a binary predicate.
func geometryOverload2BinaryPredicate(
	f func(geo.Geometry, geo.Geometry) (bool, error), ib infoBuilder,
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
			a := tree.MustBeDGeography(args[0])
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
				a := tree.MustBeDGeography(args[0])
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
				a := tree.MustBeDGeography(args[0])
				b := tree.MustBeDBool(args[1])
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
			a := tree.MustBeDGeography(args[0])
			b := tree.MustBeDGeography(args[1])
			return f(ctx, a, b)
		},
		Info:       ib.String(),
		Volatility: volatility,
	}
}

// geographyOverload2 hides the boilerplate for builtins operating on two geographys
// and the overlap wraps a binary predicate.
func geographyOverload2BinaryPredicate(
	f func(geo.Geography, geo.Geography) (bool, error), ib infoBuilder,
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
func toUseSphereOrSpheroid(useSpheroid tree.DBool) geogfn.UseSphereOrSpheroid {
	if useSpheroid {
		return geogfn.UseSpheroid
	}
	return geogfn.UseSphere
}

func initGeoBuiltins() {
	// Some functions have exactly the same definition - in which case,
	// we can just copy them.
	for _, alias := range []struct {
		alias       string
		builtinName string
	}{
		{"geomfromewkt", "st_geomfromewkt"},
		{"geomfromewkb", "st_geomfromewkb"},
		{"st_coorddim", "st_ndims"},
		{"st_geogfromtext", "st_geographyfromtext"},
		{"st_geomfromtext", "st_geometryfromtext"},
		{"st_numinteriorring", "st_numinteriorrings"},
		{"st_symmetricdifference", "st_symdifference"},
		{"st_force3d", "st_force3dz"},
	} {
		if _, ok := geoBuiltins[alias.builtinName]; !ok {
			panic("expected builtin definition for alias: " + alias.builtinName)
		}
		geoBuiltins[alias.alias] = geoBuiltins[alias.builtinName]
	}

	// Indexed functions have an alternative version with an underscore prepended
	// to the name, which tells the optimizer to not utilize the index.
	for indexBuiltinName := range geoindex.RelationshipMap {
		builtin, exists := geoBuiltins[indexBuiltinName]
		if !exists {
			panic("expected builtin: " + indexBuiltinName)
		}
		// Copy the builtin and add an underscore on the name.
		overloads := make([]tree.Overload, len(builtin.overloads))
		for i, ovCopy := range builtin.overloads {
			builtin.overloads[i].Info += "\n\nThis function variant will attempt to utilize any available spatial index."

			ovCopy.Info += "\n\nThis function variant does not utilize any spatial index."
			overloads[i] = ovCopy
		}
		underscoreBuiltin := makeBuiltin(
			builtin.props,
			overloads...,
		)
		geoBuiltins["_"+indexBuiltinName] = underscoreBuiltin
	}

	// These builtins have both a GEOMETRY and GEOGRAPHY overload.
	// For these cases, having an unknown argument for the function will
	// automatically resolve it to a string type and be implicitly cast as
	// GEOMETRY.
	for _, builtinName := range []string{
		"st_area",
		"st_asewkt",
		"st_asgeojson",
		// TODO(#48877): uncomment
		// "st_asgml",
		"st_askml",
		// TODO(#48883): uncomment
		// "st_assvg",
		// TODO(#48886): uncomment
		// "st_astwkb",
		"st_astext",
		"st_buffer",
		"st_centroid",
		// TODO(#48899): uncomment
		// "st_clusterintersecting",
		// TODO(#48901): uncomment
		// "st_clusterwithin",
		"st_coveredby",
		"st_covers",
		"st_distance",
		"st_dwithin",
		"st_intersects",
		"st_intersection",
		"st_length",
		"st_dwithinexclusive",
	} {
		builtin, exists := geoBuiltins[builtinName]
		if !exists {
			panic("expected builtin: " + builtinName)
		}
		geoBuiltins[builtinName] = appendStrArgOverloadForGeometryArgOverloads(builtin)
	}

	for k, v := range geoBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		v.props.Category = categorySpatial
		v.props.AvailableOnPublicSchema = true
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
			g := tree.MustBeDGeometry(args[0])
			fraction := float64(tree.MustBeDFloat(args[1]))
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

// appendStrArgOverloadForGeometryArgOverloads appends overloads by casting string
// types into geometry types if any geometry arguments are taken in as input.
// These are useful for functions which take GEOMETRY and GEOGRAPHY unknown args,
// which the type system will be unable to resolve without a cast. If a string
// argument is given in the same position, it is always favored.
func appendStrArgOverloadForGeometryArgOverloads(def builtinDefinition) builtinDefinition {
	newOverloads := make([]tree.Overload, len(def.overloads))
	copy(newOverloads, def.overloads)

	for i := range def.overloads {
		// Define independntly as it is used by a closure below.
		ov := def.overloads[i]

		argTypes, ok := ov.Types.(tree.ArgTypes)
		if !ok {
			continue
		}

		// Find all argument indexes that have the Geometry type.
		var argsToCast util.FastIntSet
		for i, argType := range argTypes {
			if argType.Typ.Equal(types.Geometry) {
				argsToCast.Add(i)
			}
		}
		if argsToCast.Len() == 0 {
			continue
		}

		newOverload := ov

		// Replace them with strings ArgType.
		newArgTypes := make(tree.ArgTypes, len(argTypes))
		for i := range argTypes {
			newArgTypes[i] = argTypes[i]
			if argsToCast.Contains(i) {
				newArgTypes[i].Name += "_str"
				newArgTypes[i].Typ = types.String
			}
		}

		// Wrap the overloads to cast to Geometry.
		newOverload.Types = newArgTypes
		newOverload.Fn = func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			for i, ok := argsToCast.Next(0); ok; i, ok = argsToCast.Next(i + 1) {
				arg := string(tree.MustBeDString(args[i]))
				g, err := geo.ParseGeometry(arg)
				if err != nil {
					return nil, err
				}
				args[i] = tree.NewDGeometry(g)
			}
			return ov.Fn(ctx, args)
		}

		newOverload.Info += `

This variant will cast all geometry_str arguments into Geometry types.
`

		newOverloads = append(newOverloads, newOverload)
	}

	def.overloads = newOverloads
	return def
}

// stAsGeoJSONFromTuple returns a *tree.DString representing JSON output
// for ST_AsGeoJSON.
func stAsGeoJSONFromTuple(
	ctx *tree.EvalContext, tuple *tree.DTuple, geoColumn string, numDecimalDigits int, pretty bool,
) (*tree.DString, error) {
	typ := tuple.ResolvedType()
	labels := typ.TupleLabels()

	var geometry json.JSON
	properties := json.NewObjectBuilder(len(tuple.D))

	foundGeoColumn := false
	for i, d := range tuple.D {
		var label string
		if labels != nil {
			label = labels[i]
		}
		// If label is not specified, append `f + index` as the name, in line with
		// row_to_json.
		if label == "" {
			label = fmt.Sprintf("f%d", i+1)
		}
		// If the geoColumn is not specified and the geometry is not found,
		// take a look.
		// Also take a look if the column label is the column we desire.
		if (geoColumn == "" && geometry == nil) || geoColumn == label {
			// If we can parse the data type as Geometry or Geography,
			// we've found it.
			if g, ok := d.(*tree.DGeometry); ok {
				foundGeoColumn = true
				var err error
				geometry, err = json.FromSpatialObject(g.SpatialObject(), numDecimalDigits)
				if err != nil {
					return nil, err
				}
				continue
			}
			if g, ok := d.(*tree.DGeography); ok {
				foundGeoColumn = true
				var err error
				geometry, err = json.FromSpatialObject(g.SpatialObject(), numDecimalDigits)
				if err != nil {
					return nil, err
				}
				continue
			}

			if geoColumn == label {
				foundGeoColumn = true
				// If the entry is NULL, skip the current entry.
				// Otherwise, we found the column but it was the wrong type, in which case,
				// we error out.
				if d == tree.DNull {
					continue
				}
				return nil, errors.Newf(
					"expected column %s to be a geo type, but it is of type %s",
					geoColumn,
					d.ResolvedType().SQLString(),
				)
			}
		}
		tupleJSON, err := tree.AsJSON(
			d,
			ctx.SessionData.DataConversionConfig,
			ctx.GetLocation(),
		)
		if err != nil {
			return nil, err
		}
		properties.Add(label, tupleJSON)
	}
	// If we have not found a column with the matching name, error out.
	if !foundGeoColumn && geoColumn != "" {
		return nil, errors.Newf("%q column not found", geoColumn)
	}
	// If geometry is still NULL, we either found no geometry columns
	// or the found geometry column is NULL. Return a NULL type, a la
	// PostGIS.
	if geometry == nil {
		geometryBuilder := json.NewObjectBuilder(1)
		geometryBuilder.Add("type", json.NullJSONValue)
		geometry = geometryBuilder.Build()
	}
	retJSON := json.NewObjectBuilder(3)
	retJSON.Add("type", json.FromString("Feature"))
	retJSON.Add("geometry", geometry)
	retJSON.Add("properties", properties.Build())
	retString := retJSON.Build().String()
	if !pretty {
		return tree.NewDString(retString), nil
	}
	var reserializedJSON map[string]interface{}
	err := gojson.Unmarshal([]byte(retString), &reserializedJSON)
	if err != nil {
		return nil, err
	}
	marshalledIndent, err := gojson.MarshalIndent(reserializedJSON, "", "\t")
	if err != nil {
		return nil, err
	}
	return tree.NewDString(string(marshalledIndent)), nil
}

func makeSTDWithinBuiltin(exclusivity geo.FnExclusivity) builtinDefinition {
	exclusivityStr := ", inclusive."
	if exclusivity == geo.FnExclusive {
		exclusivityStr = ", exclusive."
	}
	return makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"geometry_a", types.Geometry},
				{"geometry_b", types.Geometry},
				{"distance", types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				a := tree.MustBeDGeometry(args[0])
				b := tree.MustBeDGeometry(args[1])
				dist := tree.MustBeDFloat(args[2])
				ret, err := geomfn.DWithin(a.Geometry, b.Geometry, float64(dist), exclusivity)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if any of geometry_a is within distance units of geometry_b" +
					exclusivityStr,
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
				a := tree.MustBeDGeography(args[0])
				b := tree.MustBeDGeography(args[1])
				dist := tree.MustBeDFloat(args[2])
				ret, err := geogfn.DWithin(a.Geography, b.Geography, float64(dist), geogfn.UseSpheroid, exclusivity)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if any of geography_a is within distance meters of geography_b" +
					exclusivityStr + usesSpheroidMessage + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib,
				precision:    "1cm",
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
				a := tree.MustBeDGeography(args[0])
				b := tree.MustBeDGeography(args[1])
				dist := tree.MustBeDFloat(args[2])
				useSpheroid := tree.MustBeDBool(args[3])

				ret, err := geogfn.DWithin(a.Geography, b.Geography, float64(dist), toUseSphereOrSpheroid(useSpheroid), exclusivity)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ret)), nil
			},
			Info: infoBuilder{
				info: "Returns true if any of geography_a is within distance meters of geography_b" +
					exclusivityStr + spheroidDistanceMessage,
				libraryUsage: usesGeographicLib | usesS2,
				precision:    "1cm",
			}.String(),
			Volatility: tree.VolatilityImmutable,
		},
	)
}

func applyGeoindexConfigStorageParams(
	evalCtx *tree.EvalContext, cfg geoindex.Config, params string,
) (geoindex.Config, error) {
	indexDesc := &descpb.IndexDescriptor{GeoConfig: cfg}
	stmt, err := parser.ParseOne(
		fmt.Sprintf("CREATE INDEX t_idx ON t USING GIST(geom) WITH (%s)", params),
	)
	if err != nil {
		return geoindex.Config{}, errors.Newf("invalid storage parameters specified: %s", params)
	}
	semaCtx := tree.MakeSemaContext()
	if err := paramparse.ApplyStorageParameters(
		evalCtx.Context,
		&semaCtx,
		evalCtx,
		stmt.AST.(*tree.CreateIndex).StorageParams,
		&paramparse.IndexStorageParamObserver{IndexDesc: indexDesc},
	); err != nil {
		return geoindex.Config{}, err
	}
	return indexDesc.GeoConfig, nil
}
