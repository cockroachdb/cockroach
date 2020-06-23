// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geomfn"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/r3"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// s2GeometryIndex is an implementation of GeometryIndex that uses the S2 geometry
// library.
type s2GeometryIndex struct {
	rc                     *s2.RegionCoverer
	minX, maxX, minY, maxY float64
	deltaX, deltaY         float64
}

var _ GeometryIndex = (*s2GeometryIndex)(nil)

// NewS2GeometryIndex returns an index with the given configuration. All reads and
// writes on this index must use the same config. Writes must use the same
// config to correctly process deletions. Reads must use the same config since
// the bounds affect when a read needs to look at the exceedsBoundsCellID.
func NewS2GeometryIndex(cfg S2GeometryConfig) GeometryIndex {
	// We adjust the clipping bounds to be smaller by this fraction, since using
	// the endpoints of face 0 in S2 causes coverings to spill out of that face.
	const boundsDelta = 0.01

	// TODO(sumeer): Sanity check cfg.
	return &s2GeometryIndex{
		rc: &s2.RegionCoverer{
			MinLevel: int(cfg.S2Config.MinLevel),
			MaxLevel: int(cfg.S2Config.MaxLevel),
			LevelMod: int(cfg.S2Config.LevelMod),
			MaxCells: int(cfg.S2Config.MaxCells),
		},
		minX:   cfg.MinX,
		maxX:   cfg.MaxX,
		minY:   cfg.MinY,
		maxY:   cfg.MaxY,
		deltaX: boundsDelta * (cfg.MaxX - cfg.MinX),
		deltaY: boundsDelta * (cfg.MaxY - cfg.MinY),
	}
}

// DefaultGeometryIndexConfig returns a default config for a geometry index.
func DefaultGeometryIndexConfig() *Config {
	return &Config{
		S2Geometry: &S2GeometryConfig{
			// Arbitrary bounding box.
			// TODO(sumeer): replace with parameters specified by CREATE INDEX.
			MinX:     -10000,
			MaxX:     10000,
			MinY:     -10000,
			MaxY:     10000,
			S2Config: defaultS2Config()},
	}
}

// A cell id unused by S2. We use it to index geometries that exceed the
// configured bounds.
const exceedsBoundsCellID = s2.CellID(^uint64(0))

// TODO(sumeer): adjust code to handle precision issues with floating point
// arithmetic.

// InvertedIndexKeys implements the GeometryIndex interface.
func (s *s2GeometryIndex) InvertedIndexKeys(c context.Context, g *geo.Geometry) ([]Key, error) {
	// If the geometry exceeds the bounds, we index the clipped geometry in
	// addition to the special cell, so that queries for geometries that don't
	// exceed the bounds don't need to query the special cell (which would
	// become a hotspot in the key space).
	gt, clipped, err := s.convertToGeomTAndTryClip(g)
	if err != nil {
		return nil, err
	}
	var keys []Key
	if gt != nil {
		r := s.s2RegionsFromPlanarGeom(gt)
		keys = invertedIndexKeys(c, s.rc, r)
	}
	if clipped {
		keys = append(keys, Key(exceedsBoundsCellID))
	}
	return keys, nil
}

// Covers implements the GeometryIndex interface.
func (s *s2GeometryIndex) Covers(c context.Context, g *geo.Geometry) (UnionKeySpans, error) {
	return s.Intersects(c, g)
}

// CoveredBy implements the GeometryIndex interface.
func (s *s2GeometryIndex) CoveredBy(c context.Context, g *geo.Geometry) (RPKeyExpr, error) {
	// If the geometry exceeds the bounds, we use the clipped geometry to
	// restrict the search within the bounds.
	gt, clipped, err := s.convertToGeomTAndTryClip(g)
	if err != nil {
		return nil, err
	}
	var expr RPKeyExpr
	if gt != nil {
		r := s.s2RegionsFromPlanarGeom(gt)
		expr = coveredBy(c, s.rc, r)
	}
	if clipped {
		// Intersect with the shapes that exceed the bounds.
		expr = append(expr, Key(exceedsBoundsCellID))
		if len(expr) > 1 {
			expr = append(expr, RPSetIntersection)
		}
	}
	return expr, nil
}

// Intersects implements the GeometryIndex interface.
func (s *s2GeometryIndex) Intersects(c context.Context, g *geo.Geometry) (UnionKeySpans, error) {
	// If the geometry exceeds the bounds, we use the clipped geometry to
	// restrict the search within the bounds.
	gt, clipped, err := s.convertToGeomTAndTryClip(g)
	if err != nil {
		return nil, err
	}
	var spans UnionKeySpans
	if gt != nil {
		r := s.s2RegionsFromPlanarGeom(gt)
		spans = intersects(c, s.rc, r)
	}
	if clipped {
		// And lookup all shapes that exceed the bounds.
		spans = append(spans, KeySpan{Start: Key(exceedsBoundsCellID), End: Key(exceedsBoundsCellID)})
	}
	return spans, nil
}

func (s *s2GeometryIndex) DWithin(
	c context.Context, g *geo.Geometry, distance float64,
) (UnionKeySpans, error) {
	// TODO(sumeer): are the default params the correct thing to use here?
	g, err := geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), distance)
	if err != nil {
		return nil, err
	}
	return s.Intersects(c, g)
}

func (s *s2GeometryIndex) DFullyWithin(
	c context.Context, g *geo.Geometry, distance float64,
) (UnionKeySpans, error) {
	// TODO(sumeer): are the default params the correct thing to use here?
	g, err := geomfn.Buffer(g, geomfn.MakeDefaultBufferParams(), distance)
	if err != nil {
		return nil, err
	}
	return s.Covers(c, g)
}

// Converts to geom.T and clips to the rectangle bounds of the index.
func (s *s2GeometryIndex) convertToGeomTAndTryClip(g *geo.Geometry) (geom.T, bool, error) {
	gt, err := g.AsGeomT()
	if err != nil {
		return nil, false, err
	}
	clipped := false
	if s.geomExceedsBounds(gt) {
		clipped = true
		clippedEWKB, err :=
			geos.ClipEWKBByRect(g.EWKB(), s.minX+s.deltaX, s.minY+s.deltaY, s.maxX-s.deltaX, s.maxY-s.deltaY)
		if err != nil {
			return nil, false, err
		}
		gt = nil
		if clippedEWKB != nil {
			g, err = geo.ParseGeometryFromEWKBUnsafe(clippedEWKB)
			if err != nil {
				return nil, false, err
			}
			if g == nil {
				return nil, false, errors.Errorf("internal error: clippedWKB cannot be parsed")
			}
			gt, err = g.AsGeomT()
			if err != nil {
				return nil, false, err
			}
		}
	}
	return gt, clipped, nil
}

// Returns true if the point represented by (x, y) exceeds the rectangle
// bounds of the index.
func (s *s2GeometryIndex) xyExceedsBounds(x float64, y float64) bool {
	if x < (s.minX+s.deltaX) || x > (s.maxX-s.deltaX) {
		return true
	}
	if y < (s.minY+s.deltaY) || y > (s.maxY-s.deltaY) {
		return true
	}
	return false
}

// Returns true if g exceeds the rectangle bounds of the index.
func (s *s2GeometryIndex) geomExceedsBounds(g geom.T) bool {
	switch repr := g.(type) {
	case *geom.Point:
		return s.xyExceedsBounds(repr.X(), repr.Y())
	case *geom.LineString:
		for i := 0; i < repr.NumCoords(); i++ {
			p := repr.Coord(i)
			if s.xyExceedsBounds(p.X(), p.Y()) {
				return true
			}
		}
	case *geom.Polygon:
		if repr.NumLinearRings() > 0 {
			lr := repr.LinearRing(0)
			for i := 0; i < lr.NumCoords(); i++ {
				if s.xyExceedsBounds(lr.Coord(i).X(), lr.Coord(i).Y()) {
					return true
				}
			}
		}
	case *geom.GeometryCollection:
		for _, geom := range repr.Geoms() {
			if s.geomExceedsBounds(geom) {
				return true
			}
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			if s.geomExceedsBounds(repr.Point(i)) {
				return true
			}
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			if s.geomExceedsBounds(repr.LineString(i)) {
				return true
			}
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			if s.geomExceedsBounds(repr.Polygon(i)) {
				return true
			}
		}
	}
	return false
}

// stToUV() and face0UVToXYZPoint() are adapted from unexported methods in
// github.com/golang/geo/s2/stuv.go

// stToUV converts an s or t value to the corresponding u or v value.
// This is a non-linear transformation from [-1,1] to [-1,1] that
// attempts to make the cell sizes more uniform.
// This uses what the C++ version calls 'the quadratic transform'.
func stToUV(s float64) float64 {
	if s >= 0.5 {
		return (1 / 3.) * (4*s*s - 1)
	}
	return (1 / 3.) * (1 - 4*(1-s)*(1-s))
}

// Specialized version of faceUVToXYZ() for face 0
func face0UVToXYZPoint(u, v float64) s2.Point {
	return s2.Point{Vector: r3.Vector{X: 1, Y: u, Z: v}}
}

func (s *s2GeometryIndex) planarPointToS2Point(x float64, y float64) s2.Point {
	ss := (x - s.minX) / (s.maxX - s.minX)
	tt := (y - s.minY) / (s.maxY - s.minY)
	u := stToUV(ss)
	v := stToUV(tt)
	return face0UVToXYZPoint(u, v)
}

// TODO(sumeer): this is similar to s2RegionsFromGeom() but needs to do
// a different point conversion. If these functions do not diverge further,
// and turn out not to be performance critical, merge the two implementations.
func (s *s2GeometryIndex) s2RegionsFromPlanarGeom(geomRepr geom.T) []s2.Region {
	var regions []s2.Region
	switch repr := geomRepr.(type) {
	case *geom.Point:
		regions = []s2.Region{
			s.planarPointToS2Point(repr.X(), repr.Y()),
		}
	case *geom.LineString:
		points := make([]s2.Point, repr.NumCoords())
		for i := 0; i < repr.NumCoords(); i++ {
			p := repr.Coord(i)
			points[i] = s.planarPointToS2Point(p.X(), p.Y())
		}
		pl := s2.Polyline(points)
		regions = []s2.Region{&pl}
	case *geom.Polygon:
		loops := make([]*s2.Loop, repr.NumLinearRings())
		// The first ring is a "shell". Following rings are "holes".
		// All loops must be oriented CCW for S2.
		for ringIdx := 0; ringIdx < repr.NumLinearRings(); ringIdx++ {
			linearRing := repr.LinearRing(ringIdx)
			points := make([]s2.Point, linearRing.NumCoords())
			isCCW := geo.IsLinearRingCCW(linearRing)
			for pointIdx := 0; pointIdx < linearRing.NumCoords(); pointIdx++ {
				p := linearRing.Coord(pointIdx)
				pt := s.planarPointToS2Point(p.X(), p.Y())
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
			regions = append(regions, s.s2RegionsFromPlanarGeom(geom)...)
		}
	case *geom.MultiPoint:
		for i := 0; i < repr.NumPoints(); i++ {
			regions = append(regions, s.s2RegionsFromPlanarGeom(repr.Point(i))...)
		}
	case *geom.MultiLineString:
		for i := 0; i < repr.NumLineStrings(); i++ {
			regions = append(regions, s.s2RegionsFromPlanarGeom(repr.LineString(i))...)
		}
	case *geom.MultiPolygon:
		for i := 0; i < repr.NumPolygons(); i++ {
			regions = append(regions, s.s2RegionsFromPlanarGeom(repr.Polygon(i))...)
		}
	}
	return regions
}

func (s *s2GeometryIndex) TestingInnerCovering(g *geo.Geometry) s2.CellUnion {
	gt, _, err := s.convertToGeomTAndTryClip(g)
	if err != nil || gt == nil {
		return nil
	}
	r := s.s2RegionsFromPlanarGeom(gt)
	return innerCovering(s.rc, r)
}
