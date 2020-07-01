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

	"github.com/cockroachdb/cockroach/pkg/geo/geographiclib"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geoprojbase"
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

//
// Geospatial Type
//

// GeospatialType are functions that are common between all Geospatial types.
type GeospatialType interface {
	// SRID returns the SRID of the given type.
	SRID() geopb.SRID
	// ShapeType returns the ShapeType of the given type.
	ShapeType() geopb.ShapeType
}

var _ GeospatialType = (*Geometry)(nil)
var _ GeospatialType = (*Geography)(nil)

// GeospatialTypeFitsColumnMetadata determines whether a GeospatialType is compatible with the
// given SRID and Shape.
// Returns an error if the types does not fit.
func GeospatialTypeFitsColumnMetadata(
	t GeospatialType, srid geopb.SRID, shapeType geopb.ShapeType,
) error {
	// SRID 0 can take in any SRID. Otherwise SRIDs must match.
	if srid != 0 && t.SRID() != srid {
		return errors.Newf("object SRID %d does not match column SRID %d", t.SRID(), srid)
	}
	// Shape_Geometry/Shape_Unset can take in any kind of shape.
	// Otherwise, shapes must match.
	if shapeType != geopb.ShapeType_Unset && shapeType != geopb.ShapeType_Geometry && shapeType != t.ShapeType() {
		return errors.Newf("object type %s does not match column type %s", t.ShapeType(), shapeType)
	}
	return nil
}

//
// Geometry
//

// Geometry is planar spatial object.
type Geometry struct {
	spatialObject geopb.SpatialObject
}

// NewGeometry returns a new Geometry. Assumes the input EWKB is validated and in little endian.
func NewGeometry(spatialObject geopb.SpatialObject) (*Geometry, error) {
	if spatialObject.SRID != 0 {
		if _, ok := geoprojbase.Projection(spatialObject.SRID); !ok {
			return nil, errors.Newf("unknown SRID for Geometry: %d", spatialObject.SRID)
		}
	}
	if spatialObject.Type != geopb.SpatialObjectType_GeometryType {
		return nil, errors.Newf("expected geometry type, found %s", spatialObject.Type)
	}
	return &Geometry{spatialObject: spatialObject}, nil
}

// NewGeometryUnsafe creates a geometry object that assumes spatialObject is from the DB.
// It assumes the spatialObject underneath is safe.
func NewGeometryUnsafe(spatialObject geopb.SpatialObject) *Geometry {
	return &Geometry{spatialObject: spatialObject}
}

// NewGeometryFromPointCoords makes a point from x, y coordinates.
func NewGeometryFromPointCoords(x, y float64) (*Geometry, error) {
	s, err := spatialObjectFromGeomT(geom.NewPointFlat(geom.XY, []float64{x, y}), geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return nil, err
	}
	return NewGeometry(s)
}

// NewGeometryFromGeom creates a new Geometry object from a geom.T object.
func NewGeometryFromGeom(g geom.T) (*Geometry, error) {
	spatialObject, err := spatialObjectFromGeomT(g, geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject)
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str string) (*Geometry, error) {
	spatialObject, err := parseAmbiguousText(geopb.SpatialObjectType_GeometryType, str, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject)
}

// MustParseGeometry behaves as ParseGeometry, but panics if there is an error.
func MustParseGeometry(str string) *Geometry {
	g, err := ParseGeometry(str)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseGeometryFromEWKT parses the EWKT into a Geometry.
func ParseGeometryFromEWKT(
	ewkt geopb.EWKT, srid geopb.SRID, defaultSRIDOverwriteSetting defaultSRIDOverwriteSetting,
) (*Geometry, error) {
	g, err := parseEWKT(geopb.SpatialObjectType_GeometryType, ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// ParseGeometryFromEWKB parses the EWKB into a Geometry.
func ParseGeometryFromEWKB(ewkb geopb.EWKB) (*Geometry, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeometryType, ewkb, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// ParseGeometryFromEWKBAndSRID parses the EWKB into a given Geometry with the given
// SRID set.
func ParseGeometryFromEWKBAndSRID(ewkb geopb.EWKB, srid geopb.SRID) (*Geometry, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeometryType, ewkb, srid, DefaultSRIDShouldOverwrite)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// MustParseGeometryFromEWKB behaves as ParseGeometryFromEWKB, but panics if an error occurs.
func MustParseGeometryFromEWKB(ewkb geopb.EWKB) *Geometry {
	ret, err := ParseGeometryFromEWKB(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeometryFromGeoJSON parses the GeoJSON into a given Geometry.
func ParseGeometryFromGeoJSON(json []byte) (*Geometry, error) {
	g, err := parseGeoJSON(geopb.SpatialObjectType_GeometryType, json, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g)
}

// ParseGeometryFromEWKBUnsafe returns a new Geometry from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You most likely want geo.ParseGeometryFromEWKB instead.
func ParseGeometryFromEWKBUnsafe(ewkb geopb.EWKB) (*Geometry, error) {
	base, err := parseEWKBRaw(geopb.SpatialObjectType_GeometryType, ewkb)
	if err != nil {
		return nil, err
	}
	return NewGeometryUnsafe(base), nil
}

// AsGeography converts a given Geometry to its Geography form.
func (g *Geometry) AsGeography() (*Geography, error) {
	srid := g.SRID()
	if srid == 0 {
		// Set a geography SRID if one is not already set.
		srid = geopb.DefaultGeographySRID
	}
	spatialObject, err := adjustSpatialObject(g.spatialObject, srid, geopb.SpatialObjectType_GeographyType)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject)
}

// CloneWithSRID sets a given Geometry's SRID to another, without any transformations.
// Returns a new Geometry object.
func (g *Geometry) CloneWithSRID(srid geopb.SRID) (*Geometry, error) {
	spatialObject, err := adjustSpatialObject(g.spatialObject, srid, geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject)
}

// adjustSpatialObject returns the SpatialObject with new parameters.
func adjustSpatialObject(
	so geopb.SpatialObject, srid geopb.SRID, soType geopb.SpatialObjectType,
) (geopb.SpatialObject, error) {
	t, err := ewkb.Unmarshal(so.EWKB)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	adjustGeomSRID(t, srid)
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

// CartesianBoundingBox returns a Cartesian bounding box.
func (g *Geometry) CartesianBoundingBox() *CartesianBoundingBox {
	if g.spatialObject.BoundingBox == nil {
		return nil
	}
	return &CartesianBoundingBox{BoundingBox: *g.spatialObject.BoundingBox}
}

//
// Geography
//

// Geography is a spherical spatial object.
type Geography struct {
	spatialObject geopb.SpatialObject
}

// NewGeography returns a new Geography. Assumes the input EWKB is validated and in little endian.
func NewGeography(spatialObject geopb.SpatialObject) (*Geography, error) {
	projection, ok := geoprojbase.Projection(spatialObject.SRID)
	if !ok {
		return nil, errors.Newf("unknown SRID for Geography: %d", spatialObject.SRID)
	}
	if !projection.IsLatLng {
		return nil, errors.Newf(
			"SRID %d cannot be used for geography as it is not in a lon/lat coordinate system",
			spatialObject.SRID,
		)
	}
	if spatialObject.Type != geopb.SpatialObjectType_GeographyType {
		return nil, errors.Newf("expected geography type, found %s", spatialObject.Type)
	}
	return &Geography{spatialObject: spatialObject}, nil
}

// NewGeographyUnsafe creates a geometry object that assumes spatialObject is from the DB.
// It assumes the spatialObject underneath is safe.
func NewGeographyUnsafe(spatialObject geopb.SpatialObject) *Geography {
	return &Geography{spatialObject: spatialObject}
}

// NewGeographyFromGeom creates a new Geography from a geom.T object.
func NewGeographyFromGeom(g geom.T) (*Geography, error) {
	spatialObject, err := spatialObjectFromGeomT(g, geopb.SpatialObjectType_GeographyType)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject)
}

// MustNewGeographyFromGeom enforces no error from NewGeographyFromGeom.
func MustNewGeographyFromGeom(g geom.T) *Geography {
	ret, err := NewGeographyFromGeom(g)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeography parses a Geography from a given text.
func ParseGeography(str string) (*Geography, error) {
	spatialObject, err := parseAmbiguousText(geopb.SpatialObjectType_GeographyType, str, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject)
}

// MustParseGeography behaves as ParseGeography, but panics if there is an error.
func MustParseGeography(str string) *Geography {
	g, err := ParseGeography(str)
	if err != nil {
		panic(err)
	}
	return g
}

// ParseGeographyFromEWKT parses the EWKT into a Geography.
func ParseGeographyFromEWKT(
	ewkt geopb.EWKT, srid geopb.SRID, defaultSRIDOverwriteSetting defaultSRIDOverwriteSetting,
) (*Geography, error) {
	g, err := parseEWKT(geopb.SpatialObjectType_GeographyType, ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// ParseGeographyFromEWKB parses the EWKB into a Geography.
func ParseGeographyFromEWKB(ewkb geopb.EWKB) (*Geography, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeographyType, ewkb, geopb.DefaultGeographySRID, DefaultSRIDIsHint)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// ParseGeographyFromEWKBAndSRID parses the EWKB into a given Geography with the
// given SRID set.
func ParseGeographyFromEWKBAndSRID(ewkb geopb.EWKB, srid geopb.SRID) (*Geography, error) {
	g, err := parseEWKB(geopb.SpatialObjectType_GeographyType, ewkb, srid, DefaultSRIDShouldOverwrite)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// MustParseGeographyFromEWKB behaves as ParseGeographyFromEWKB, but panics if an error occurs.
func MustParseGeographyFromEWKB(ewkb geopb.EWKB) *Geography {
	ret, err := ParseGeographyFromEWKB(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// ParseGeographyFromGeoJSON parses the GeoJSON into a given Geography.
func ParseGeographyFromGeoJSON(json []byte) (*Geography, error) {
	g, err := parseGeoJSON(geopb.SpatialObjectType_GeographyType, json, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(g)
}

// ParseGeographyFromEWKBUnsafe returns a new Geography from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You most likely want ParseGeographyFromEWKB instead.
func ParseGeographyFromEWKBUnsafe(ewkb geopb.EWKB) (*Geography, error) {
	base, err := parseEWKBRaw(geopb.SpatialObjectType_GeographyType, ewkb)
	if err != nil {
		return nil, err
	}
	return NewGeographyUnsafe(base), nil
}

// CloneWithSRID sets a given Geography's SRID to another, without any transformations.
// Returns a new Geography object.
func (g *Geography) CloneWithSRID(srid geopb.SRID) (*Geography, error) {
	spatialObject, err := adjustSpatialObject(g.spatialObject, srid, geopb.SpatialObjectType_GeographyType)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject)
}

// AsGeometry converts a given Geography to its Geometry form.
func (g *Geography) AsGeometry() (*Geometry, error) {
	spatialObject, err := adjustSpatialObject(g.spatialObject, g.SRID(), geopb.SpatialObjectType_GeometryType)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject)
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

// Spheroid returns the spheroid represented by the given Geography.
func (g *Geography) Spheroid() (*geographiclib.Spheroid, error) {
	proj, ok := geoprojbase.Projection(g.SRID())
	if !ok {
		return nil, errors.Newf("expected spheroid for SRID %d", g.SRID())
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
	return S2RegionsFromGeom(geomRepr, emptyBehavior)
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

	// prevIdx is the previous point. If we are at the 0th point, the last coordinate
	// is also the 0th point, so take the second last point.
	// Note we don't have to apply this for "nextIdx" as we cap the search above at the
	// second last vertex.
	prevIdx := smallestIdx - 1
	if smallestIdx == 0 {
		prevIdx = linearRing.NumCoords() - 2
	}
	a := linearRing.Coord(prevIdx)
	b := smallest
	c := linearRing.Coord(smallestIdx + 1)

	// We could do the cross product, but we are only interested in the sign.
	// To find the sign, reorganize into the orientation matrix:
	//  1 x_a y_a
	//  1 x_b y_b
	//  1 x_c y_c
	// and find the determinant.
	// https://en.wikipedia.org/wiki/Curve_orientation#Orientation_of_a_simple_polygon
	areaSign := a.X()*b.Y() - a.Y()*b.X() +
		a.Y()*c.X() - a.X()*c.Y() +
		b.X()*c.Y() - c.X()*b.Y()
	// Note having an area sign of 0 means it is a flat polygon, which is invalid.
	return areaSign > 0
}

// S2RegionsFromGeom converts an geom representation of an object
// to s2 regions.
// As S2 does not really handle empty geometries well, we need to ingest emptyBehavior and
// react appropriately.
func S2RegionsFromGeom(geomRepr geom.T, emptyBehavior EmptyBehavior) ([]s2.Region, error) {
	var regions []s2.Region
	if geomRepr.Empty() {
		switch emptyBehavior {
		case EmptyBehaviorOmit:
			return nil, nil
		case EmptyBehaviorError:
			return nil, NewEmptyGeometryError()
		default:
			return nil, errors.Newf("programmer error: unknown behavior")
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
			subRegions, err := S2RegionsFromGeom(geom, emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			subRegions, err := S2RegionsFromGeom(repr.Point(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			subRegions, err := S2RegionsFromGeom(repr.LineString(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			subRegions, err := S2RegionsFromGeom(repr.Polygon(i), emptyBehavior)
			if err != nil {
				return nil, err
			}
			regions = append(regions, subRegions...)
		}
	}
	return regions, nil
}

//
// Common
//

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
// by converting geom.T cordinates inplace
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

// spatialObjectFromGeomT creates a geopb.SpatialObject from a geom.T.
func spatialObjectFromGeomT(t geom.T, soType geopb.SpatialObjectType) (geopb.SpatialObject, error) {
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
	switch t.Layout() {
	case geom.XY:
	case geom.NoLayout:
		if gc, ok := t.(*geom.GeometryCollection); !ok || !gc.Empty() {
			return geopb.SpatialObject{}, errors.Newf("no layout found on object")
		}
	default:
		return geopb.SpatialObject{}, errors.Newf("only 2D objects are currently supported")
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
	switch t := t.(type) {
	case *geom.Point:
		return geopb.ShapeType_Point, nil
	case *geom.LineString:
		return geopb.ShapeType_LineString, nil
	case *geom.Polygon:
		return geopb.ShapeType_Polygon, nil
	case *geom.MultiPoint:
		return geopb.ShapeType_MultiPoint, nil
	case *geom.MultiLineString:
		return geopb.ShapeType_MultiLineString, nil
	case *geom.MultiPolygon:
		return geopb.ShapeType_MultiPolygon, nil
	case *geom.GeometryCollection:
		return geopb.ShapeType_GeometryCollection, nil
	default:
		return geopb.ShapeType_Unset, errors.Newf("unknown shape: %T", t)
	}
}

// CompareSpatialObject compares the SpatialObject.
// This must match the byte ordering that is be produced by encoding.EncodeGeoAscending.
func CompareSpatialObject(lhs geopb.SpatialObject, rhs geopb.SpatialObject) int {
	marshalledLHS, err := protoutil.Marshal(&lhs)
	if err != nil {
		panic(err)
	}
	marshalledRHS, err := protoutil.Marshal(&rhs)
	if err != nil {
		panic(err)
	}
	return bytes.Compare(marshalledLHS, marshalledRHS)
}
