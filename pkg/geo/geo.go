// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/golang/geo/s2"
	"github.com/otan-cockroach/gogeos/geos"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/ewkb"
	"github.com/twpayne/go-geom/encoding/ewkbhex"
	"github.com/twpayne/go-geom/encoding/wkt"
	"github.com/xeonx/geodesic"
	"github.com/xeonx/geographic"
)

type SRID int32

const (
	UnknownSRID = SRID(0)

	DefaultGeographySRID = SRID(4326)
	DefaultGeometrySRID  = UnknownSRID
)

var (
	ekbEndian = binary.LittleEndian
)

type Geometry struct {
	T geom.T
}

func (g *Geometry) String() string {
	ret, err := ewkbhex.Encode(g.T, ekbEndian)
	if err != nil {
		panic(err)
	}
	return strings.ToUpper(ret)
}

func (g *Geometry) WKT() string {
	// TODO(#geo): go-geom has extra spaces in the repr
	ret, err := wkt.Marshal(g.T)
	if err != nil {
		panic(err)
	}
	return ret
}

func (g *Geometry) Encode() []byte {
	b, err := ewkb.Marshal(g.T, ekbEndian)
	if err != nil {
		panic(err)
	}
	return b
}

func (g *Geometry) GEOS() *geos.Geometry {
	ret, err := geos.FromWKB(g.Encode())
	if err != nil {
		panic(err)
	}
	return ret
}

func (g *Geometry) SRID() SRID {
	return SRID(g.T.SRID())
}

func (g *Geometry) S2LatLng() s2.LatLng {
	switch t := g.T.(type) {
	case *geom.Point:
		switch g.T.Layout() {
		case geom.XY, geom.XYM:
			return s2.LatLngFromDegrees(t.Y(), t.X())
		}
	}
	panic("cannot convert shape")
}

func (g *Geometry) GeographicArea() float64 {
	// TODO: spheroids (supported by GeographicLib)
	switch g.T.Layout() {
	case geom.XY:
	case geom.XYM:
	default:
		panic("unexpected lat/lng")
	}

	switch t := g.T.(type) {
	case *geom.Point:
		return 0
	case *geom.MultiPoint:
		return 0
	case *geom.LineString:
		return 0
	case *geom.MultiLineString:
		return 0
	case *geom.Polygon:
		return S2PolygonFromGeom(t).Area()
	case *geom.MultiPolygon:
		var area float64
		for i := 0; i < t.NumPolygons(); i++ {
			area += (&Geometry{t.Polygon(i)}).GeographicArea()
		}
		return area
	case *geom.GeometryCollection:
		var area float64
		for _, subGeometry := range t.Geoms() {
			area += (&Geometry{subGeometry}).GeographicArea()
		}
		return area
	}
	panic("cannot calculate geographic area")
}

func S2PolygonFromGeom(t *geom.Polygon) *s2.Polygon {
	var loops []*s2.Loop
	prevEnd := 0
	for endIdx, end := range t.Ends() {
		coords := t.FlatCoords()[prevEnd:end]
		points := make([]s2.Point, 0, (end-prevEnd)/t.Stride())
		for i := 0; i < len(coords); i += t.Stride() {
			// WKT defines loops to be CW after idx=0 (and ALWAYS holes).
			if endIdx == 0 {
				x, y := coords[i], coords[i+1]
				points = append(points, s2.PointFromLatLng(s2.LatLngFromDegrees(y, x)))
			} else {
				x, y := coords[len(coords)-(i+t.Stride())], coords[len(coords)-(i+t.Stride()-1)]
				points = append(points, s2.PointFromLatLng(s2.LatLngFromDegrees(y, x)))
			}
		}
		loops = append(loops, s2.LoopFromPoints(points))
		prevEnd = end
	}
	return s2.PolygonFromLoops(loops)
}

func S2PolylineFromGeom(t *geom.LineString) *s2.Polyline {
	latLngs := []s2.LatLng{}
	for _, p := range t.Coords() {
		latLngs = append(latLngs, s2.LatLngFromDegrees(p.Y(), p.X()))
	}
	return s2.PolylineFromLatLngs(latLngs)
}

func S2CellFromGeom(t *geom.Point) s2.Cell {
	return s2.CellFromLatLng(s2.LatLngFromDegrees(t.Y(), t.X()))
}

func (g *Geometry) GeographicDistance(o *Geometry, useSpheroid bool) float64 {
	switch lhs := g.T.(type) {
	case *geom.Point:
		switch rhs := o.T.(type) {
		case *geom.Point:
			if useSpheroid {
				lhsLng, lhsLat := lhs.X(), lhs.Y()
				rhsLng, rhsLat := rhs.X(), rhs.Y()
				// TODO(otan): use spheroid defined by SRID.
				s12, _, _ := geodesic.WGS84.Inverse(
					geographic.Point{lhsLat, lhsLng},
					geographic.Point{rhsLat, rhsLng},
				)
				return s12
			}
			return float64(g.S2LatLng().Distance(o.S2LatLng())) * 6371010
		case *geom.LineString:
			// Game is hard.
			// DistanceFromSegment will work for spheres.
			// edge_distance_to_edge checks intersection first, THEN
			// checks point vs edge for both sides for all four points on the edges.
		}
	}
	panic("no")
}

func (g *Geometry) GeographicIntersect(o *Geometry) bool {
	switch lhs := g.T.(type) {
	case *geom.Point:
		lhsCell := S2CellFromGeom(lhs)
		switch rhs := o.T.(type) {
		case *geom.Point:
			return lhsCell.IntersectsCell(S2CellFromGeom(rhs))
		case *geom.LineString:
			return S2PolylineFromGeom(rhs).IntersectsCell(lhsCell)
		case *geom.Polygon:
			return S2PolygonFromGeom(rhs).IntersectsCell(lhsCell)
		case *geom.MultiPoint:
			for i := 0; i < rhs.NumPoints(); i++ {
				if lhsCell.IntersectsCell(S2CellFromGeom(rhs.Point(i))) {
					return true
				}
			}
			return false
		case *geom.MultiLineString:
			for i := 0; i < rhs.NumLineStrings(); i++ {
				if S2PolylineFromGeom(rhs.LineString(i)).IntersectsCell(lhsCell) {
					return true
				}
			}
			return false
		case *geom.MultiPolygon:
			for i := 0; i < rhs.NumPolygons(); i++ {
				if S2PolygonFromGeom(rhs.Polygon(i)).IntersectsCell(lhsCell) {
					return true
				}
			}
			return false
		case *geom.GeometryCollection:
			for _, subGeometry := range rhs.Geoms() {
				if g.GeographicIntersect(&Geometry{subGeometry}) {
					return true
				}
			}
			return false
		}
	case *geom.LineString:
		switch rhs := o.T.(type) {
		case *geom.Point:
			return S2PolylineFromGeom(lhs).IntersectsCell(S2CellFromGeom(rhs))
		case *geom.LineString:
			return S2PolylineFromGeom(lhs).Intersects(S2PolylineFromGeom(rhs))
		case *geom.Polygon:
			// missing `IntersectWithPolyline` in s2 library (polygon.go#1197).
		}
	case *geom.Polygon:
		switch rhs := o.T.(type) {
		case *geom.Polygon:
			return S2PolygonFromGeom(lhs).Intersects(S2PolygonFromGeom(rhs))
		}
	}
	// ... etc ...
	panic("unknown comparison")
}

func DecodeGeometry(b []byte) (Geometry, error) {
	t, err := ewkb.Unmarshal(b)
	if err != nil {
		return Geometry{}, err
	}
	return Geometry{T: t}, nil
}

func ParseGeometry(str string) (Geometry, error) {
	srid := SRID(0)
	if strings.HasPrefix(str, "SRID=") {
		end := strings.Index(str, ";")
		// TODO: get SRID.
		if end != -1 {
			sridInt64, err := strconv.ParseInt(str[len("SRID="):end], 10, 64)
			if err != nil {
				return Geometry{}, err
			}
			srid = SRID(sridInt64)
			str = str[end+1:]
		}
	}

	// Try parse it as wkbhex first, if we can do so.
	if '0' <= str[0] && str[0] <= '9' {
		t, err := ewkbhex.Decode(str)
		if err == nil {
			return Geometry{T: t}, nil
		}
	}

	// TODO: SRID= at the front... doesn't work in the geos library!
	g, err := geos.FromWKT(str)
	if err != nil {
		return Geometry{}, err
	}
	wkbBytes, err := g.WKB()
	if err != nil {
		return Geometry{}, err
	}
	t, err := ewkb.Read(bytes.NewReader(wkbBytes))
	if err != nil {
		return Geometry{}, err
	}
	if srid != 0 {
		switch o := t.(type) {
		case *geom.Point:
			o.SetSRID(int(srid))
		}
	}
	return Geometry{T: t}, nil
}
