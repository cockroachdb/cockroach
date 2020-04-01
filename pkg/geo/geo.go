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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	// Force these into vendor until they're used.
	_ "github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/geojson"
	_ "github.com/twpayne/go-geom/encoding/kml"
	_ "github.com/twpayne/go-geom/encoding/wkb"
	_ "github.com/twpayne/go-geom/encoding/wkbhex"
	_ "github.com/twpayne/go-geom/encoding/wkt"
)

// spatialObjectBase is the base for spatial objects.
type spatialObjectBase struct {
	ewkb geopb.EWKB
	// TODO: denormalize SRID from EWKB.
}

// Geometry is planar spatial object.
type Geometry struct {
	spatialObjectBase
}

// NewGeometry returns a new Geometry.
func NewGeometry(ewkb geopb.EWKB) *Geometry {
	return &Geometry{spatialObjectBase{ewkb: ewkb}}
}

// ParseGeometry parses a Geometry from a given text.
func ParseGeometry(str geopb.WKT) (*Geometry, error) {
	wkb, err := geos.WKTToWKB(str)
	if err != nil {
		return nil, err
	}
	return NewGeometry(geopb.EWKB(wkb)), nil
}

// Geography is a spherical spatial object.
type Geography struct {
	spatialObjectBase
}

// NewGeography returns a new Geography.
func NewGeography(ewkb geopb.EWKB) *Geography {
	return &Geography{spatialObjectBase{ewkb: ewkb}}
}

// ParseGeography parses a Geography from a given text.
// TODO(otan): when we have our own WKT parser, move this to geo.
func ParseGeography(str geopb.WKT) (*Geography, error) {
	// TODO(otan): set SRID of EWKB to 4326.
	wkb, err := geos.WKTToWKB(str)
	if err != nil {
		return nil, err
	}
	return NewGeography(geopb.EWKB(wkb)), nil
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
		// The first ring is a "shell", which is represented as CCW.
		// Following rings are "holes", which are CW. For S2, they are CCW and automatically figured out.
		for ringIdx := 0; ringIdx < repr.NumLinearRings(); ringIdx++ {
			linearRing := repr.LinearRing(ringIdx)
			points := make([]s2.Point, linearRing.NumCoords())
			for pointIdx := 0; pointIdx < linearRing.NumCoords(); pointIdx++ {
				p := linearRing.Coord(pointIdx)
				pt := s2.PointFromLatLng(s2.LatLngFromDegrees(p.Y(), p.X()))
				if ringIdx == 0 {
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
