// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geo contains the base types for spatial data type operations.
package geo

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/r1"
	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// DefaultEWKBEncodingFormat is the default encoding format for EWKB.
var DefaultEWKBEncodingFormat = binary.LittleEndian

// EmptyBehavior is the behavior to adopt when an empty Geometry is encountered.
type EmptyBehavior uint8

const (
	// EmptyBehaviorError will error with EmptyGeometryError when an empty geometry
	// is encountered.
	EmptyBehaviorError EmptyBehavior = 0
	// EmptyBehaviorOmit will omit an entry when an empty geometry is encountered.
	EmptyBehaviorOmit EmptyBehavior = 1
)

// FnExclusivity is used to indicate whether a geo function should have
// inclusive or exclusive semantics. For example, DWithin == (Distance <= x),
// while DWithinExclusive == (Distance < x).
type FnExclusivity bool

// MaxAllowedSplitPoints is the maximum number of points any spatial function may split to.
const MaxAllowedSplitPoints = 65336

const (
	// FnExclusive indicates that the corresponding geo function should have
	// exclusive semantics.
	FnExclusive FnExclusivity = true
	// FnInclusive indicates that the corresponding geo function should have
	// inclusive semantics.
	FnInclusive FnExclusivity = false
)

// SpatialObjectFitsColumnMetadata determines whether a GeospatialType is compatible with the
// given SRID and Shape.
// Returns an error if the types does not fit.
func SpatialObjectFitsColumnMetadata(
	so geopb.SpatialObject, srid geopb.SRID, shapeType geopb.ShapeType,
) error {
	// SRID 0 can take in any SRID. Otherwise SRIDs must match.
	if srid != 0 && so.SRID != srid {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"object SRID %d does not match column SRID %d",
			so.SRID,
			srid,
		)
	}
	// Shape_Unset can take in any kind of shape.
	// Shape_Geometry[ZM] must match dimensions.
	// Otherwise, shapes must match.
	switch shapeType {
	case geopb.ShapeType_Unset:
		break
	case geopb.ShapeType_Geometry, geopb.ShapeType_GeometryM, geopb.ShapeType_GeometryZ, geopb.ShapeType_GeometryZM:
		if ShapeTypeToLayout(shapeType) != ShapeTypeToLayout(so.ShapeType) {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"object type %s does not match column dimensionality %s",
				so.ShapeType,
				shapeType,
			)
		}
	default:
		if shapeType != so.ShapeType {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"object type %s does not match column type %s",
				so.ShapeType,
				shapeType,
			)
		}
	}
	return nil
}

// ShapeTypeToLayout returns the geom.Layout of the given ShapeType.
// Note this is not a definition on ShapeType to prevent geopb from importing twpayne/go-geom.
func ShapeTypeToLayout(s geopb.ShapeType) geom.Layout {
	switch {
	case (s&geopb.MShapeTypeFlag > 0) && (s&geopb.ZShapeTypeFlag > 0):
		return geom.XYZM
	case s&geopb.ZShapeTypeFlag > 0:
		return geom.XYZ
	case s&geopb.MShapeTypeFlag > 0:
		return geom.XYM
	default:
		return geom.XY
	}
}

//
// Geometry
//

// Geometry is planar spatial object.
type Geometry struct {
	spatialObject geopb.SpatialObject
}

// MakeGeometry returns a new Geometry. Assumes the input EWKB is validated and in little endian.
func MakeGeometry(spatialObject geopb.SpatialObject) (Geometry, error) {
	if spatialObject.SRID != 0 {
		if _, err := geoprojbase.Projection(spatialObject.SRID); err != nil {
			return Geometry{}, err
		}
	}
	if spatialObject.Type != geopb.SpatialObjectType_GeometryType {
		return Geometry{}, pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"expected geometry type, found %s",
			spatialObject.Type,
		)
	}
	return Geometry{spatialObject: spatialObject}, nil
}

// MakeGeometryUnsafe creates a geometry object that assumes spatialObject is from the DB.
// It assumes the spatialObject underneath is safe.
func MakeGeometryUnsafe(spatialObject geopb.SpatialObject) Geometry {
	return Geometry{spatialObject: spatialObject}
}

// MakeGeometryFromPointCoords makes a point from x, y coordinates.
func MakeGeometryFromPointCoords(x, y float64) (Geometry, error) {
	return MakeGeometryFromLayoutAndPointCoords(geom.XY, []float64{x, y})
}

// MakeGeometryFromLayoutAndPointCoords makes a point with a given layout and ordered slice of coordinates.
func MakeGeometryFromLayoutAndPointCoords(
	layout geom.Layout, flatCoords []float64,
) (Geometry, error) {
	// Validate that the stride matches what is expected for the layout.
	switch {
	case layout == geom.XY && len(flatCoords) == 2:
	case layout == geom.XYM && len(flatCoords) == 3:
	case layout == geom.XYZ && len(flatCoords) == 3:
	case layout == geom.XYZM && len(flatCoords) == 4:
	default:
		return Geometry{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"mismatch between layout %d and stride %d",
			layout,
			len(flatCoords),
		)
	}
	s, err := spatialObjectFromGeomT(geom.NewPointFlat(layout, flatCoords), geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(s)
}

// MakeGeometryFromGeomT creates a new Geometry object from a geom.T object.
func MakeGeometryFromGeomT(g geom.T) (Geometry, error) {
	spatialObject, err := spatialObjectFromGeomT(g, geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(spatialObject)
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str string) (Geometry, error) {
	spatialObject, err := parseAmbiguousText(geopb.SpatialObjectType_GeometryType, str, geopb.DefaultGeometrySRID)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(spatialObject)
}

// MustParseGeometry behaves as ParseGeometry, but panics if there is an error.
func MustParseGeometry(str string) Geometry {
	g, err := ParseGeometry(str)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseGeometryFromEWKT parses the EWKT into a Geometry.
func ParseGeometryFromEWKT(
	ewkt geopb.EWKT, srid geopb.SRID, defaultSRIDOverwriteSetting defaultSRIDOverwriteSetting,
) (Geometry, error) {
	g, err := parseEWKT(geopb.SpatialObjectType_GeometryType, ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(g)
}

// ParseGeometryFromEWKB parses the EWKB into a Geometry.
func ParseGeometryFromEWKB(ewkb geopb.EWKB) (Geometry, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeometryType, ewkb, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(g)
}

// ParseGeometryFromEWKBAndSRID parses the EWKB into a given Geometry with the given
// SRID set.
func ParseGeometryFromEWKBAndSRID(ewkb geopb.EWKB, srid geopb.SRID) (Geometry, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeometryType, ewkb, srid, DefaultSRIDShouldOverwrite)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(g)
}

// MustParseGeometryFromEWKB behaves as ParseGeometryFromEWKB, but panics if an error occurs.
func MustParseGeometryFromEWKB(ewkb geopb.EWKB) Geometry {
	ret, err := ParseGeometryFromEWKB(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeometryFromGeoJSON parses the GeoJSON into a given Geometry.
func ParseGeometryFromGeoJSON(json []byte) (Geometry, error) {
	// Note we set SRID to 4326 from here, to match PostGIS's behavior as per
	// RFC7946 (https://tools.ietf.org/html/rfc7946#section-4).
	g, err := parseGeoJSON(geopb.SpatialObjectType_GeometryType, json, geopb.DefaultGeographySRID)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(g)
}

// ParseGeometryFromEWKBUnsafe returns a new Geometry from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You most likely want geo.ParseGeometryFromEWKB instead.
func ParseGeometryFromEWKBUnsafe(ewkb geopb.EWKB) (Geometry, error) {
	base, err := parseEWKBRaw(geopb.SpatialObjectType_GeometryType, ewkb)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometryUnsafe(base), nil
}

// AsGeography converts a given Geometry to its Geography form.
func (g *Geometry) AsGeography() (Geography, error) {
	srid := g.SRID()
	if srid == 0 {
		// Set a geography SRID if one is not already set.
		srid = geopb.DefaultGeographySRID
	}
	spatialObject, err := adjustSpatialObject(g.spatialObject, srid, geopb.SpatialObjectType_GeographyType)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(spatialObject)
}

// CloneWithSRID sets a given Geometry's SRID to another, without any transformations.
// Returns a new Geometry object.
func (g *Geometry) CloneWithSRID(srid geopb.SRID) (Geometry, error) {
	spatialObject, err := adjustSpatialObject(g.spatialObject, srid, geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(spatialObject)
}

// adjustSpatialObject returns the SpatialObject with new parameters.
func adjustSpatialObject(
	so geopb.SpatialObject, srid geopb.SRID, soType geopb.SpatialObjectType,
) (geopb.SpatialObject, error) {
	t, err := ewkb.Unmarshal(so.EWKB)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	AdjustGeomTSRID(t, srid)
	return spatialObjectFromGeomT(t, soType)
}

// AsGeomT returns the geometry as a geom.T object.
func (g *Geometry) AsGeomT() (geom.T, error) {
	return ewkb.Unmarshal(g.spatialObject.EWKB)
}

// Empty returns whether the given Geometry is empty.
func (g *Geometry) Empty() bool {
	return g.spatialObject.BoundingBox == nil
}

// EWKB returns the EWKB representation of the Geometry.
func (g *Geometry) EWKB() geopb.EWKB {
	return g.spatialObject.EWKB
}

// SpatialObject returns the SpatialObject representation of the Geometry.
func (g *Geometry) SpatialObject() geopb.SpatialObject {
	return g.spatialObject
}

// SpatialObjectRef return a pointer to the SpatialObject representation of the
// Geometry.
func (g *Geometry) SpatialObjectRef() *geopb.SpatialObject {
	return &g.spatialObject
}

// EWKBHex returns the EWKBHex representation of the Geometry.
func (g *Geometry) EWKBHex() string {
	return g.spatialObject.EWKBHex()
}

// SRID returns the SRID representation of the Geometry.
func (g *Geometry) SRID() geopb.SRID {
	return g.spatialObject.SRID
}

// ShapeType returns the shape type of the Geometry.
func (g *Geometry) ShapeType() geopb.ShapeType {
	return g.spatialObject.ShapeType
}

// ShapeType2D returns the 2D shape type of the Geometry.
func (g *Geometry) ShapeType2D() geopb.ShapeType {
	return g.ShapeType().To2D()
}

// CartesianBoundingBox returns a Cartesian bounding box.
func (g *Geometry) CartesianBoundingBox() *CartesianBoundingBox {
	if g.spatialObject.BoundingBox == nil {
		return nil
	}
	return &CartesianBoundingBox{BoundingBox: *g.spatialObject.BoundingBox}
}

// BoundingBoxRef returns a pointer to the BoundingBox, if any.
func (g *Geometry) BoundingBoxRef() *geopb.BoundingBox {
	return g.spatialObject.BoundingBox
}

// SpaceCurveIndex returns an uint64 index to use representing an index into a space-filling curve.
// This will return 0 for empty spatial objects, and math.MaxUint64 for any object outside
// the defined bounds of the given SRID projection.
func (g *Geometry) SpaceCurveIndex() (uint64, error) {
	bbox := g.CartesianBoundingBox()
	if bbox == nil {
		return 0, nil
	}
	centerX := (bbox.BoundingBox.LoX + bbox.BoundingBox.HiX) / 2
	centerY := (bbox.BoundingBox.LoY + bbox.BoundingBox.HiY) / 2
	// By default, bound by MaxInt32 (we have not typically seen bounds greater than 1B).
	bounds := geoprojbase.Bounds{
		MinX: math.MinInt32,
		MaxX: math.MaxInt32,
		MinY: math.MinInt32,
		MaxY: math.MaxInt32,
	}
	if g.SRID() != 0 {
		proj, err := geoprojbase.Projection(g.SRID())
		if err != nil {
			return 0, err
		}
		bounds = proj.Bounds
	}
	// If we're out of bounds, give up and return a large number.
	if centerX > bounds.MaxX || centerY > bounds.MaxY || centerX < bounds.MinX || centerY < bounds.MinY {
		return math.MaxUint64, nil
	}

	const boxLength = 1 << 32
	// Add 1 to each bound so that we normalize the coordinates to [0, 1) before
	// multiplying by boxLength to give coordinates that are integers in the interval [0, boxLength-1].
	xBounds := (bounds.MaxX - bounds.MinX) + 1
	yBounds := (bounds.MaxY - bounds.MinY) + 1
	// hilbertInverse returns values in the interval [0, boxLength^2-1], so return [0, 2^64-1].
	xPos := uint64(((centerX - bounds.MinX) / xBounds) * boxLength)
	yPos := uint64(((centerY - bounds.MinY) / yBounds) * boxLength)
	return hilbertInverse(boxLength, xPos, yPos), nil
}

// Compare compares a Geometry against another.
// It compares using SpaceCurveIndex, followed by the byte representation of the Geometry.
// This must produce the same ordering as the index mechanism.
func (g *Geometry) Compare(o Geometry) int {
	lhs, err := g.SpaceCurveIndex()
	if err != nil {
		// We should always be able to compare a valid geometry.
		panic(err)
	}
	rhs, err := o.SpaceCurveIndex()
	if err != nil {
		// We should always be able to compare a valid geometry.
		panic(err)
	}
	if lhs > rhs {
		return 1
	}
	if lhs < rhs {
		return -1
	}
	return compareSpatialObjectBytes(g.SpatialObjectRef(), o.SpatialObjectRef())
}

//
// Geography
//

// Geography is a spherical spatial object.
type Geography struct {
	spatialObject geopb.SpatialObject
}

// MakeGeography returns a new Geography. Assumes the input EWKB is validated and in little endian.
func MakeGeography(spatialObject geopb.SpatialObject) (Geography, error) {
	projection, err := geoprojbase.Projection(spatialObject.SRID)
	if err != nil {
		return Geography{}, err
	}
	if !projection.IsLatLng {
		return Geography{}, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"SRID %d cannot be used for geography as it is not in a lon/lat coordinate system",
			spatialObject.SRID,
		)
	}
	if spatialObject.Type != geopb.SpatialObjectType_GeographyType {
		return Geography{}, pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"expected geography type, found %s",
			spatialObject.Type,
		)
	}
	return Geography{spatialObject: spatialObject}, nil
}

// MakeGeographyUnsafe creates a geometry object that assumes spatialObject is from the DB.
// It assumes the spatialObject underneath is safe.
func MakeGeographyUnsafe(spatialObject geopb.SpatialObject) Geography {
	return Geography{spatialObject: spatialObject}
}

// MakeGeographyFromGeomT creates a new Geography from a geom.T object.
func MakeGeographyFromGeomT(g geom.T) (Geography, error) {
	spatialObject, err := spatialObjectFromGeomT(g, geopb.SpatialObjectType_GeographyType)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(spatialObject)
}

// MustMakeGeographyFromGeomT enforces no error from MakeGeographyFromGeomT.
func MustMakeGeographyFromGeomT(g geom.T) Geography {
	ret, err := MakeGeographyFromGeomT(g)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeography parses a Geography from a given text.
func ParseGeography(str string) (Geography, error) {
	spatialObject, err := parseAmbiguousText(geopb.SpatialObjectType_GeographyType, str, geopb.DefaultGeographySRID)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(spatialObject)
}

// MustParseGeography behaves as ParseGeography, but panics if there is an error.
func MustParseGeography(str string) Geography {
	g, err := ParseGeography(str)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseGeographyFromEWKT parses the EWKT into a Geography.
func ParseGeographyFromEWKT(
	ewkt geopb.EWKT, srid geopb.SRID, defaultSRIDOverwriteSetting defaultSRIDOverwriteSetting,
) (Geography, error) {
	g, err := parseEWKT(geopb.SpatialObjectType_GeographyType, ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(g)
}

// ParseGeographyFromEWKB parses the EWKB into a Geography.
func ParseGeographyFromEWKB(ewkb geopb.EWKB) (Geography, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeographyType, ewkb, geopb.DefaultGeographySRID, DefaultSRIDIsHint)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(g)
}

// ParseGeographyFromEWKBAndSRID parses the EWKB into a given Geography with the
// given SRID set.
func ParseGeographyFromEWKBAndSRID(ewkb geopb.EWKB, srid geopb.SRID) (Geography, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeographyType, ewkb, srid, DefaultSRIDShouldOverwrite)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(g)
}

// MustParseGeographyFromEWKB behaves as ParseGeographyFromEWKB, but panics if an error occurs.
func MustParseGeographyFromEWKB(ewkb geopb.EWKB) Geography {
	ret, err := ParseGeographyFromEWKB(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeographyFromGeoJSON parses the GeoJSON into a given Geography.
func ParseGeographyFromGeoJSON(json []byte) (Geography, error) {
	g, err := parseGeoJSON(geopb.SpatialObjectType_GeographyType, json, geopb.DefaultGeographySRID)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(g)
}

// ParseGeographyFromEWKBUnsafe returns a new Geography from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You most likely want ParseGeographyFromEWKB instead.
func ParseGeographyFromEWKBUnsafe(ewkb geopb.EWKB) (Geography, error) {
	base, err := parseEWKBRaw(geopb.SpatialObjectType_GeographyType, ewkb)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeographyUnsafe(base), nil
}

// CloneWithSRID sets a given Geography's SRID to another, without any transformations.
// Returns a new Geography object.
func (g *Geography) CloneWithSRID(srid geopb.SRID) (Geography, error) {
	spatialObject, err := adjustSpatialObject(g.spatialObject, srid, geopb.SpatialObjectType_GeographyType)
	if err != nil {
		return Geography{}, err
	}
	return MakeGeography(spatialObject)
}

// AsGeometry converts a given Geography to its Geometry form.
func (g *Geography) AsGeometry() (Geometry, error) {
	spatialObject, err := adjustSpatialObject(g.spatialObject, g.SRID(), geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return Geometry{}, err
	}
	return MakeGeometry(spatialObject)
}

// AsGeomT returns the Geography as a geom.T object.
func (g *Geography) AsGeomT() (geom.T, error) {
	return ewkb.Unmarshal(g.spatialObject.EWKB)
}

// EWKB returns the EWKB representation of the Geography.
func (g *Geography) EWKB() geopb.EWKB {
	return g.spatialObject.EWKB
}

// SpatialObject returns the SpatialObject representation of the Geography.
func (g *Geography) SpatialObject() geopb.SpatialObject {
	return g.spatialObject
}

// SpatialObjectRef returns a pointer to the SpatialObject representation of the
// Geography.
func (g *Geography) SpatialObjectRef() *geopb.SpatialObject {
	return &g.spatialObject
}

// EWKBHex returns the EWKBHex representation of the Geography.
func (g *Geography) EWKBHex() string {
	return g.spatialObject.EWKBHex()
}

// SRID returns the SRID representation of the Geography.
func (g *Geography) SRID() geopb.SRID {
	return g.spatialObject.SRID
}

// ShapeType returns the shape type of the Geography.
func (g *Geography) ShapeType() geopb.ShapeType {
	return g.spatialObject.ShapeType
}

// ShapeType2D returns the 2D shape type of the Geography.
func (g *Geography) ShapeType2D() geopb.ShapeType {
	return g.ShapeType().To2D()
}

// Spheroid returns the spheroid represented by the given Geography.
func (g *Geography) Spheroid() (*geographiclib.Spheroid, error) {
	proj, err := geoprojbase.Projection(g.SRID())
	if err != nil {
		return nil, err
	}
	return proj.Spheroid, nil
}

// AsS2 converts a given Geography into it's S2 form.
func (g *Geography) AsS2(emptyBehavior EmptyBehavior) ([]s2.Region, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	// TODO(otan): convert by reading from EWKB to S2 directly.
	return S2RegionsFromGeomT(geomRepr, emptyBehavior)
}

// BoundingRect returns the bounding s2.Rect of the given Geography.
func (g *Geography) BoundingRect() s2.Rect {
	bbox := g.spatialObject.BoundingBox
	if bbox == nil {
		return s2.EmptyRect()
	}
	return s2.Rect{
		Lat: r1.Interval{Lo: bbox.LoY, Hi: bbox.HiY},
		Lng: s1.Interval{Lo: bbox.LoX, Hi: bbox.HiX},
	}
}

// BoundingCap returns the bounding s2.Cap of the given Geography.
func (g *Geography) BoundingCap() s2.Cap {
	return g.BoundingRect().CapBound()
}

// SpaceCurveIndex returns an uint64 index to use representing an index into a space-filling curve.
// This will return 0 for empty spatial objects.
func (g *Geography) SpaceCurveIndex() uint64 {
	rect := g.BoundingRect()
	if rect.IsEmpty() {
		return 0
	}
	return uint64(s2.CellIDFromLatLng(rect.Center()))
}

// Compare compares a Geography against another.
// It compares using SpaceCurveIndex, followed by the byte representation of the Geography.
// This must produce the same ordering as the index mechanism.
func (g *Geography) Compare(o Geography) int {
	lhs := g.SpaceCurveIndex()
	rhs := o.SpaceCurveIndex()
	if lhs > rhs {
		return 1
	}
	if lhs < rhs {
		return -1
	}
	return compareSpatialObjectBytes(g.SpatialObjectRef(), o.SpatialObjectRef())
}

//
// Common
//

// AdjustGeomTSRID adjusts the SRID of a given geom.T.
// Ideally SetSRID is an interface of geom.T, but that is not the case.
func AdjustGeomTSRID(t geom.T, srid geopb.SRID) {
	switch t := t.(type) {
	case *geom.Point:
		t.SetSRID(int(srid))
	case *geom.LineString:
		t.SetSRID(int(srid))
	case *geom.Polygon:
		t.SetSRID(int(srid))
	case *geom.GeometryCollection:
		t.SetSRID(int(srid))
	case *geom.MultiPoint:
		t.SetSRID(int(srid))
	case *geom.MultiLineString:
		t.SetSRID(int(srid))
	case *geom.MultiPolygon:
		t.SetSRID(int(srid))
	default:
		panic(errors.AssertionFailedf("geo: unknown geom type: %v", t))
	}
}

// IsLinearRingCCW returns whether a given linear ring is counter clock wise.
// See 2.07 of http://www.faqs.org/faqs/graphics/algorithms-faq/.
// "Find the lowest vertex (or, if  there is more than one vertex with the same lowest coordinate,
//  the rightmost of those vertices) and then take the cross product of the edges fore and aft of it."
func IsLinearRingCCW(linearRing *geom.LinearRing) bool {
	smallestIdx := 0
	smallest := linearRing.Coord(0)

	for pointIdx := 1; pointIdx < linearRing.NumCoords()-1; pointIdx++ {
		curr := linearRing.Coord(pointIdx)
		if curr.Y() < smallest.Y() || (curr.Y() == smallest.Y() && curr.X() > smallest.X()) {
			smallestIdx = pointIdx
			smallest = curr
		}
	}

	// Find the previous point in the ring that is not the same as smallest.
	prevIdx := smallestIdx - 1
	if prevIdx < 0 {
		prevIdx = linearRing.NumCoords() - 1
	}
	for prevIdx != smallestIdx {
		a := linearRing.Coord(prevIdx)
		if a.X() != smallest.X() || a.Y() != smallest.Y() {
			break
		}
		prevIdx--
		if prevIdx < 0 {
			prevIdx = linearRing.NumCoords() - 1
		}
	}
	// Find the next point in the ring that is not the same as smallest.
	nextIdx := smallestIdx + 1
	if nextIdx >= linearRing.NumCoords() {
		nextIdx = 0
	}
	for nextIdx != smallestIdx {
		c := linearRing.Coord(nextIdx)
		if c.X() != smallest.X() || c.Y() != smallest.Y() {
			break
		}
		nextIdx++
		if nextIdx >= linearRing.NumCoords() {
			nextIdx = 0
		}
	}

	// We could do the cross product, but we are only interested in the sign.
	// To find the sign, reorganize into the orientation matrix:
	//  1 x_a y_a
	//  1 x_b y_b
	//  1 x_c y_c
	// and find the determinant.
	// https://en.wikipedia.org/wiki/Curve_orientation#Orientation_of_a_simple_polygon
	a := linearRing.Coord(prevIdx)
	b := smallest
	c := linearRing.Coord(nextIdx)

	areaSign := a.X()*b.Y() - a.Y()*b.X() +
		a.Y()*c.X() - a.X()*c.Y() +
		b.X()*c.Y() - c.X()*b.Y()
	// Note having an area sign of 0 means it is a flat polygon, which is invalid.
	return areaSign > 0
}

// S2RegionsFromGeomT converts an geom representation of an object
// to s2 regions.
// As S2 does not really handle empty geometries well, we need to ingest emptyBehavior and
// react appropriately.
func S2RegionsFromGeomT(geomRepr geom.T, emptyBehavior EmptyBehavior) ([]s2.Region, error) {
	var regions []s2.Region
	if geomRepr.Empty() {
		switch emptyBehavior {
		case EmptyBehaviorOmit:
			return nil, nil
		case EmptyBehaviorError:
			return nil, NewEmptyGeometryError()
		default:
			return nil, errors.AssertionFailedf("programmer error: unknown behavior")
		}
	}
	switch repr := geomRepr.(type) {
	case *geom.Point:
		regions = []s2.Region{
			s2.PointFromLatLng(s2.LatLngFromDegrees(repr.Y(), repr.X())),
		}
	case *geom.LineString:
		latLngs := make([]s2.LatLng, repr.NumCoords())
		for i := 0; i < repr.NumCoords(); i++ {
			p := repr.Coord(i)
			latLngs[i] = s2.LatLngFromDegrees(p.Y(), p.X())
		}
		regions = []s2.Region{
			s2.PolylineFromLatLngs(latLngs),
		}
	case *geom.Polygon:
		loops := make([]*s2.Loop, repr.NumLinearRings())
		// All loops must be oriented CCW for S2.
		for ringIdx := 0; ringIdx < repr.NumLinearRings(); ringIdx++ {
			linearRing := repr.LinearRing(ringIdx)
			points := make([]s2.Point, linearRing.NumCoords())
			isCCW := IsLinearRingCCW(linearRing)
			for pointIdx := 0; pointIdx < linearRing.NumCoords(); pointIdx++ {
				p := linearRing.Coord(pointIdx)
				pt := s2.PointFromLatLng(s2.LatLngFromDegrees(p.Y(), p.X()))
				if isCCW {
					points[pointIdx] = pt
				} else {
					points[len(points)-pointIdx-1] = pt
				}
			}
			loops[ringIdx] = s2.LoopFromPoints(points)
		}
		regions = []s2.Region{
			s2.PolygonFromLoops(loops),
		}
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			subRegions, err := S2RegionsFromGeomT(geom, emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			subRegions, err := S2RegionsFromGeomT(repr.Point(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			subRegions, err := S2RegionsFromGeomT(repr.LineString(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			subRegions, err := S2RegionsFromGeomT(repr.Polygon(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	}
	return regions, nil
}

// normalizeLngLat normalizes geographical coordinates into a valid range.
func normalizeLngLat(lng float64, lat float64) (float64, float64) {
	if lat > 90 || lat < -90 {
		lat = NormalizeLatitudeDegrees(lat)
	}
	if lng > 180 || lng < -180 {
		lng = NormalizeLongitudeDegrees(lng)
	}
	return lng, lat
}

// normalizeGeographyGeomT limits geography coordinates to spherical coordinates
// by converting geom.T coordinates inplace
func normalizeGeographyGeomT(t geom.T) {
	switch repr := t.(type) {
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			normalizeGeographyGeomT(geom)
		}
	default:
		coords := repr.FlatCoords()
		for i := 0; i < len(coords); i += repr.Stride() {
			coords[i], coords[i+1] = normalizeLngLat(coords[i], coords[i+1])
		}
	}
}

// validateGeomT validates the geom.T object across valid geom.T objects,
// returning an error if it is invalid.
func validateGeomT(t geom.T) error {
	if t.Empty() {
		return nil
	}
	switch t := t.(type) {
	case *geom.Point:
	case *geom.LineString:
		if t.NumCoords() < 2 {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"LineString must have at least 2 coordinates",
			)
		}
	case *geom.Polygon:
		for i := 0; i < t.NumLinearRings(); i++ {
			linearRing := t.LinearRing(i)
			if linearRing.NumCoords() < 4 {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"Polygon LinearRing must have at least 4 points, found %d at position %d",
					linearRing.NumCoords(),
					i+1,
				)
			}
			if !linearRing.Coord(0).Equal(linearRing.Layout(), linearRing.Coord(linearRing.NumCoords()-1)) {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					"Polygon LinearRing at position %d is not closed",
					i+1,
				)
			}
		}
	case *geom.MultiPoint:
	case *geom.MultiLineString:
		for i := 0; i < t.NumLineStrings(); i++ {
			if err := validateGeomT(t.LineString(i)); err != nil {
				return errors.Wrapf(err, "invalid MultiLineString component at position %d", i+1)
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < t.NumPolygons(); i++ {
			if err := validateGeomT(t.Polygon(i)); err != nil {
				return errors.Wrapf(err, "invalid MultiPolygon component at position %d", i+1)
			}
		}
	case *geom.GeometryCollection:
		// TODO(ayang): verify that the geometries all have the same Layout
		for i := 0; i < t.NumGeoms(); i++ {
			if err := validateGeomT(t.Geom(i)); err != nil {
				return errors.Wrapf(err, "invalid GeometryCollection component at position %d", i+1)
			}
		}
	default:
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"unknown geom.T type: %T",
			t,
		)
	}
	return nil
}

// spatialObjectFromGeomT creates a geopb.SpatialObject from a geom.T.
func spatialObjectFromGeomT(t geom.T, soType geopb.SpatialObjectType) (geopb.SpatialObject, error) {
	if err := validateGeomT(t); err != nil {
		return geopb.SpatialObject{}, err
	}
	if soType == geopb.SpatialObjectType_GeographyType {
		normalizeGeographyGeomT(t)
	}
	ret, err := ewkb.Marshal(t, DefaultEWKBEncodingFormat)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	shapeType, err := shapeTypeFromGeomT(t)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	bbox, err := boundingBoxFromGeomT(t, soType)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	return geopb.SpatialObject{
		Type:        soType,
		EWKB:        geopb.EWKB(ret),
		SRID:        geopb.SRID(t.SRID()),
		ShapeType:   shapeType,
		BoundingBox: bbox,
	}, nil
}

func shapeTypeFromGeomT(t geom.T) (geopb.ShapeType, error) {
	var shapeType geopb.ShapeType
	switch t := t.(type) {
	case *geom.Point:
		shapeType = geopb.ShapeType_Point
	case *geom.LineString:
		shapeType = geopb.ShapeType_LineString
	case *geom.Polygon:
		shapeType = geopb.ShapeType_Polygon
	case *geom.MultiPoint:
		shapeType = geopb.ShapeType_MultiPoint
	case *geom.MultiLineString:
		shapeType = geopb.ShapeType_MultiLineString
	case *geom.MultiPolygon:
		shapeType = geopb.ShapeType_MultiPolygon
	case *geom.GeometryCollection:
		shapeType = geopb.ShapeType_GeometryCollection
	default:
		return geopb.ShapeType_Unset, pgerror.Newf(pgcode.InvalidParameterValue, "unknown shape: %T", t)
	}
	switch t.Layout() {
	case geom.NoLayout:
		if gc, ok := t.(*geom.GeometryCollection); !ok || !gc.Empty() {
			return geopb.ShapeType_Unset, pgerror.Newf(pgcode.InvalidParameterValue, "no layout found on object")
		}
	case geom.XY:
		break
	case geom.XYM:
		shapeType = shapeType | geopb.MShapeTypeFlag
	case geom.XYZ:
		shapeType = shapeType | geopb.ZShapeTypeFlag
	case geom.XYZM:
		shapeType = shapeType | geopb.ZShapeTypeFlag | geopb.MShapeTypeFlag
	default:
		return geopb.ShapeType_Unset, pgerror.Newf(pgcode.InvalidParameterValue, "unknown layout: %s", t.Layout())
	}
	return shapeType, nil
}

// GeomTContainsEmpty returns whether a geom.T contains any empty element.
func GeomTContainsEmpty(g geom.T) bool {
	if g.Empty() {
		return true
	}
	switch g := g.(type) {
	case *geom.MultiPoint:
		for i := 0; i < g.NumPoints(); i++ {
			if g.Point(i).Empty() {
				return true
			}
		}
	case *geom.MultiLineString:
		for i := 0; i < g.NumLineStrings(); i++ {
			if g.LineString(i).Empty() {
				return true
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < g.NumPolygons(); i++ {
			if g.Polygon(i).Empty() {
				return true
			}
		}
	case *geom.GeometryCollection:
		for i := 0; i < g.NumGeoms(); i++ {
			if GeomTContainsEmpty(g.Geom(i)) {
				return true
			}
		}
	}
	return false
}

// compareSpatialObjectBytes compares the SpatialObject if they were serialized.
// This is used for comparison operations, and must be kept consistent with the indexing
// encoding.
func compareSpatialObjectBytes(lhs *geopb.SpatialObject, rhs *geopb.SpatialObject) int {
	marshalledLHS, err := protoutil.Marshal(lhs)
	if err != nil {
		panic(err)
	}
	marshalledRHS, err := protoutil.Marshal(rhs)
	if err != nil {
		panic(err)
	}
	return bytes.Compare(marshalledLHS, marshalledRHS)
}
