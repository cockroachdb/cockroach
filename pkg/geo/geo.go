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
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

// EWKBEncodingFormat is the encoding format for EWKB.
var EWKBEncodingFormat = binary.LittleEndian

// GeospatialType are functions that are common between all Geospatial types.
type GeospatialType interface {
	// SRID returns the SRID of the given type.
	SRID() geopb.SRID
	// Shape returns the Shape of the given type.
	Shape() geopb.Shape
}

var _ GeospatialType = (*Geometry)(nil)
var _ GeospatialType = (*Geography)(nil)

//
// Geometry
//

// Geometry is planar spatial object.
type Geometry struct {
	geopb.SpatialObject
}

// NewGeometry returns a new Geometry. Assumes the input EWKB is validated and in little endian.
func NewGeometry(spatialObject geopb.SpatialObject) *Geometry {
	return &Geometry{SpatialObject: spatialObject}
}

// NewGeometryFromPointCoords makes a point from x, y coordinates.
func NewGeometryFromPointCoords(x, y float64) (*Geometry, error) {
	s, err := spatialObjectFromGeom(geom.NewPointFlat(geom.XY, []float64{x, y}))
	if err != nil {
		return nil, err
	}
	return &Geometry{SpatialObject: s}, nil
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str string) (*Geometry, error) {
	spatialObject, err := parseAmbiguousText(str, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(spatialObject), nil
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
	g, err := parseEWKT(ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g), nil
}

// ParseGeometryFromEWKB parses the EWKB into a Geometry.
func ParseGeometryFromEWKB(ewkb geopb.EWKB) (*Geometry, error) {
	g, err := parseEWKB(ewkb, geopb.DefaultGeometrySRID, DefaultSRIDIsHint)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g), nil
}

// ParseGeometryFromWKB parses the WKB into a given Geometry.
func ParseGeometryFromWKB(wkb geopb.WKB, srid geopb.SRID) (*Geometry, error) {
	g, err := parseWKB(wkb, srid)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g), nil
}

// ParseGeometryFromGeoJSON parses the GeoJSON into a given Geometry.
func ParseGeometryFromGeoJSON(json []byte) (*Geometry, error) {
	g, err := parseGeoJSON(json, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(g), nil
}

// ParseGeometryFromEWKBRaw returns a new Geometry from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You must likely want geo.ParseGeometryFromEWKB instead.
func ParseGeometryFromEWKBRaw(ewkb geopb.EWKB) (*Geometry, error) {
	base, err := parseEWKBRaw(ewkb)
	if err != nil {
		return nil, err
	}
	return &Geometry{SpatialObject: base}, nil
}

// MustParseGeometryFromEWKBRaw behaves as ParseGeometryFromEWKBRaw, but panics if an error occurs.
func MustParseGeometryFromEWKBRaw(ewkb geopb.EWKB) *Geometry {
	ret, err := ParseGeometryFromEWKBRaw(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// AsGeography converts a given Geometry to it's Geography form.
func (g *Geometry) AsGeography() (*Geography, error) {
	if g.SRID() != 0 {
		// TODO(otan): check SRID is latlng
		return NewGeography(g.SpatialObject), nil
	}

	// Set a default SRID if one is not already set.
	t, err := ewkb.Unmarshal(g.EWKB())
	if err != nil {
		return nil, err
	}
	adjustGeomSRID(t, geopb.DefaultGeographySRID)
	spatialObject, err := spatialObjectFromGeom(t)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject), nil
}

// AsGeomT returns the geometry as a geom.T object.
func (g *Geometry) AsGeomT() (geom.T, error) {
	return ewkb.Unmarshal(g.SpatialObject.EWKB)
}

// EWKB returns the EWKB representation of the Geometry.
func (g *Geometry) EWKB() geopb.EWKB {
	return g.SpatialObject.EWKB
}

// SRID returns the SRID representation of the Geometry.
func (g *Geometry) SRID() geopb.SRID {
	return g.SpatialObject.SRID
}

// Shape returns the shape of the Geometry.
func (g *Geometry) Shape() geopb.Shape {
	return g.SpatialObject.Shape
}

//
// Geography
//

// Geography is a spherical spatial object.
type Geography struct {
	geopb.SpatialObject
}

// NewGeography returns a new Geography. Assumes the input EWKB is validated and in little endian.
func NewGeography(spatialObject geopb.SpatialObject) *Geography {
	return &Geography{SpatialObject: spatialObject}
}

// NewGeographyFromGeom Geography Object from geom.T.
func NewGeographyFromGeom(g geom.T) (*Geography, error) {
	spatialObject, err := spatialObjectFromGeom(g)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject), nil
}

// ParseGeography parses a Geography from a given text.
func ParseGeography(str string) (*Geography, error) {
	spatialObject, err := parseAmbiguousText(str, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(spatialObject), nil
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
	g, err := parseEWKT(ewkt, srid, defaultSRIDOverwriteSetting)
	if err != nil {
		return nil, err
	}
	return NewGeography(g), nil
}

// ParseGeographyFromEWKB parses the EWKB into a Geography.
func ParseGeographyFromEWKB(ewkb geopb.EWKB) (*Geography, error) {
	g, err := parseEWKB(ewkb, geopb.DefaultGeographySRID, DefaultSRIDIsHint)
	if err != nil {
		return nil, err
	}
	return NewGeography(g), nil
}

// ParseGeographyFromWKB parses the WKB into a given Geography.
func ParseGeographyFromWKB(wkb geopb.WKB, srid geopb.SRID) (*Geography, error) {
	g, err := parseWKB(wkb, srid)
	if err != nil {
		return nil, err
	}
	return NewGeography(g), nil
}

// ParseGeographyFromGeoJSON parses the GeoJSON into a given Geography.
func ParseGeographyFromGeoJSON(json []byte) (*Geography, error) {
	g, err := parseGeoJSON(json, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(g), nil
}

// ParseGeographyFromEWKBRaw returns a new Geography from an EWKB, without any SRID checks.
// You should only do this if you trust the EWKB is setup correctly.
// You must likely want ParseGeographyFromEWKB instead.
func ParseGeographyFromEWKBRaw(ewkb geopb.EWKB) (*Geography, error) {
	base, err := parseEWKBRaw(ewkb)
	if err != nil {
		return nil, err
	}
	return &Geography{SpatialObject: base}, nil
}

// MustParseGeographyFromEWKBRaw behaves as ParseGeographyFromEWKBRaw, but panics if an error occurs.
func MustParseGeographyFromEWKBRaw(ewkb geopb.EWKB) *Geography {
	ret, err := ParseGeographyFromEWKBRaw(ewkb)
	if err != nil {
		panic(err)
	}
	return ret
}

// AsGeometry converts a given Geography to it's Geometry form.
func (g *Geography) AsGeometry() *Geometry {
	return NewGeometry(g.SpatialObject)
}

// AsGeomT returns the Geography as a geom.T object.
func (g *Geography) AsGeomT() (geom.T, error) {
	return ewkb.Unmarshal(g.SpatialObject.EWKB)
}

// EWKB returns the EWKB representation of the Geography.
func (g *Geography) EWKB() geopb.EWKB {
	return g.SpatialObject.EWKB
}

// SRID returns the SRID representation of the Geography.
func (g *Geography) SRID() geopb.SRID {
	return g.SpatialObject.SRID
}

// Shape returns the shape of the Geography.
func (g *Geography) Shape() geopb.Shape {
	return g.SpatialObject.Shape
}

// AsS2 converts a given Geography into it's S2 form.
func (g *Geography) AsS2() ([]s2.Region, error) {
	geomRepr, err := g.AsGeomT()
	if err != nil {
		return nil, err
	}
	// TODO(otan): convert by reading from EWKB to S2 directly.
	return S2RegionsFromGeom(geomRepr), nil
}

// isLinearRingCCW returns whether a given linear ring is counter clock wise.
// See 2.07 of http://www.faqs.org/faqs/graphics/algorithms-faq/.
// "Find the lowest vertex (or, if  there is more than one vertex with the same lowest coordinate,
//  the rightmost of those vertices) and then take the cross product of the edges fore and aft of it."
func isLinearRingCCW(linearRing *geom.LinearRing) bool {
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
func S2RegionsFromGeom(geomRepr geom.T) []s2.Region {
	var regions []s2.Region
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
			isCCW := isLinearRingCCW(linearRing)
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
			regions = append(regions, S2RegionsFromGeom(geom)...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			regions = append(regions, S2RegionsFromGeom(repr.Point(i))...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			regions = append(regions, S2RegionsFromGeom(repr.LineString(i))...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			regions = append(regions, S2RegionsFromGeom(repr.Polygon(i))...)
		}
	}
	return regions
}

//
// common
//

// spatialObjectFromGeom creates a geopb.SpatialObject from a geom.T.
func spatialObjectFromGeom(t geom.T) (geopb.SpatialObject, error) {
	ret, err := ewkb.Marshal(t, EWKBEncodingFormat)
	if err != nil {
		return geopb.SpatialObject{}, err
	}
	var shape geopb.Shape
	switch t := t.(type) {
	case *geom.Point:
		shape = geopb.Shape_Point
	case *geom.LineString:
		shape = geopb.Shape_LineString
	case *geom.Polygon:
		shape = geopb.Shape_Polygon
	case *geom.MultiPoint:
		shape = geopb.Shape_MultiPoint
	case *geom.MultiLineString:
		shape = geopb.Shape_MultiLineString
	case *geom.MultiPolygon:
		shape = geopb.Shape_MultiPolygon
	case *geom.GeometryCollection:
		shape = geopb.Shape_GeometryCollection
	default:
		return geopb.SpatialObject{}, errors.Newf("unknown shape: %T", t)
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
	return geopb.SpatialObject{
		EWKB:  geopb.EWKB(ret),
		SRID:  geopb.SRID(t.SRID()),
		Shape: shape,
	}, nil
}
