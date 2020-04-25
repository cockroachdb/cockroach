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
	"encoding/hex"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
)

var ewkbEncodingFormat = binary.LittleEndian

// spatialObjectBase is the base for spatial objects.
type spatialObjectBase struct {
	ewkb geopb.EWKB
}

// EWKB returns the EWKB form of the data type.
func (b *spatialObjectBase) EWKB() geopb.EWKB {
	return b.ewkb
}

// SRID returns the SRID of the given spatial object.
func (b *spatialObjectBase) SRID() geopb.SRID {
	// We always assume EWKB is little endian and valid.
	// Mask the 5th byte and check if it has the SRID bit set.
	if b.ewkb[4]&0x20 == 0 {
		return 0
	}
	// Read the next 4 bytes as little endian.
	return geopb.SRID(
		int32(b.ewkb[5]) +
			(int32(b.ewkb[6]) << 8) +
			(int32(b.ewkb[7]) << 16) +
			(int32(b.ewkb[8]) << 24),
	)
}

// EWKBHex returns the EWKB-hex version of this data type
func (b *spatialObjectBase) EWKBHex() string {
	return strings.ToUpper(hex.EncodeToString(b.ewkb))
}

// makeSpatialObjectBase creates a spatialObjectBase from an unvalidated EWKB.
func makeSpatialObjectBase(in geopb.UnvalidatedEWKB) (spatialObjectBase, error) {
	t, err := ewkb.Unmarshal(in)
	if err != nil {
		return spatialObjectBase{}, err
	}
	ret, err := ewkb.Marshal(t, ewkbEncodingFormat)
	if err != nil {
		return spatialObjectBase{}, err
	}
	return spatialObjectBase{ewkb: geopb.EWKB(ret)}, nil
}

// Geometry is planar spatial object.
type Geometry struct {
	spatialObjectBase
}

// NewGeometry returns a new Geometry. Assumes the input EWKB is validated and in little endian.
func NewGeometry(ewkb geopb.EWKB) *Geometry {
	return &Geometry{spatialObjectBase{ewkb: ewkb}}
}

// NewGeometryFromUnvalidatedEWKB returns a new Geometry from an unvalidated EWKB.
func NewGeometryFromUnvalidatedEWKB(ewkb geopb.UnvalidatedEWKB) (*Geometry, error) {
	base, err := makeSpatialObjectBase(ewkb)
	if err != nil {
		return nil, err
	}
	return &Geometry{base}, nil
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str string) (*Geometry, error) {
	ewkb, err := parseAmbiguousTextToEWKB(str, geopb.DefaultGeometrySRID)
	if err != nil {
		return nil, err
	}
	return NewGeometry(ewkb), nil
}

// MustParseGeometry behaves as ParseGeometry, but panics if there is an error.
func MustParseGeometry(str string) *Geometry {
	g, err := ParseGeometry(str)
	if err != nil {
		panic(err)
	}
	return g
}

// AsGeography converts a given Geometry to it's Geography form.
func (g *Geometry) AsGeography() (*Geography, error) {
	if g.SRID() != 0 {
		// TODO(otan): check SRID is latlng
		return NewGeography(g.ewkb), nil
	}

	// Set a default SRID if one is not already set.
	geom, err := ewkb.Unmarshal(g.ewkb)
	if err != nil {
		return nil, err
	}
	adjustGeomSRID(geom, geopb.DefaultGeographySRID)
	ret, err := ewkb.Marshal(geom, ewkbEncodingFormat)
	if err != nil {
		return nil, err
	}
	return NewGeography(geopb.EWKB(ret)), nil
}

// AsGeomT returns the geometry as a geom.T object.
func (g *Geometry) AsGeomT() (geom.T, error) {
	return ewkb.Unmarshal(g.ewkb)
}

// Geography is a spherical spatial object.
type Geography struct {
	spatialObjectBase
}

// NewGeography returns a new Geography. Assumes the input EWKB is validated and in little endian.
func NewGeography(ewkb geopb.EWKB) *Geography {
	return &Geography{spatialObjectBase{ewkb: ewkb}}
}

// NewGeographyFromUnvalidatedEWKB returns a new Geography from an unvalidated EWKB.
func NewGeographyFromUnvalidatedEWKB(ewkb geopb.UnvalidatedEWKB) (*Geography, error) {
	base, err := makeSpatialObjectBase(ewkb)
	if err != nil {
		return nil, err
	}
	return &Geography{base}, nil
}

// ParseGeography parses a Geography from a given text.
func ParseGeography(str string) (*Geography, error) {
	// TODO(otan): set SRID of EWKB to 4326.
	ewkb, err := parseAmbiguousTextToEWKB(str, geopb.DefaultGeographySRID)
	if err != nil {
		return nil, err
	}
	return NewGeography(ewkb), nil
}

// AsGeometry converts a given Geography to it's Geometry form.
func (g *Geography) AsGeometry() *Geometry {
	return NewGeometry(g.ewkb)
}

// AsS2 converts a given Geography into it's S2 form.
func (g *Geography) AsS2() ([]s2.Region, error) {
	// TODO(otan): parse EWKB ourselves.
	geomRepr, err := ewkb.Unmarshal(g.ewkb)
	if err != nil {
		return nil, err
	}
	// TODO(otan): convert by reading from S2 directly.
	return s2RegionsFromGeom(geomRepr), nil
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

// s2RegionsFromGeom converts an geom representation of an object
// to s2 regions.
func s2RegionsFromGeom(geomRepr geom.T) []s2.Region {
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
			regions = append(regions, s2RegionsFromGeom(geom)...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			regions = append(regions, s2RegionsFromGeom(repr.Point(i))...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			regions = append(regions, s2RegionsFromGeom(repr.LineString(i))...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			regions = append(regions, s2RegionsFromGeom(repr.Polygon(i))...)
		}
	}
	return regions
}
