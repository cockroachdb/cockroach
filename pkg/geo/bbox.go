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
	"math"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

// CartesianBoundingBox is the cartesian BoundingBox representation,
// meant for use for GEOMETRY types.
type CartesianBoundingBox struct {
	geopb.BoundingBox
}

// NewCartesianBoundingBox returns a properly initialized empty bounding box
// for carestian plane types.
func NewCartesianBoundingBox() *CartesianBoundingBox {
	return &CartesianBoundingBox{
		BoundingBox: geopb.BoundingBox{
			LoX: math.MaxFloat64,
			HiX: -math.MaxFloat64,
			LoY: math.MaxFloat64,
			HiY: -math.MaxFloat64,
		},
	}
}

// AddPoint adds a point to the BoundingBox coordinates.
func (b *CartesianBoundingBox) AddPoint(x, y float64) {
	b.LoX = math.Min(b.LoX, x)
	b.HiX = math.Max(b.HiX, x)
	b.LoY = math.Min(b.LoY, y)
	b.HiY = math.Max(b.HiY, y)
}

// Buffer adds n units to each side of the bounding box.
func (b *CartesianBoundingBox) Buffer(n float64) *CartesianBoundingBox {
	if b == nil {
		return nil
	}
	return &CartesianBoundingBox{
		BoundingBox: geopb.BoundingBox{
			LoX: b.LoX - n,
			HiX: b.HiX + n,
			LoY: b.LoY - n,
			HiY: b.HiY + n,
		},
	}
}

// Intersects returns whether the BoundingBoxes intersect.
// Empty bounding boxes never intersect.
func (b *CartesianBoundingBox) Intersects(o *CartesianBoundingBox) bool {
	// If either side is empty, they do not intersect.
	if b == nil || o == nil {
		return false
	}
	if b.LoY > o.HiY || o.LoY > b.HiY ||
		b.LoX > o.HiX || o.LoX > b.HiX {
		return false
	}
	return true
}

// Covers returns whether the BoundingBox covers the other bounding box.
// Empty bounding boxes never cover.
func (b *CartesianBoundingBox) Covers(o *CartesianBoundingBox) bool {
	if b == nil || o == nil {
		return false
	}
	return b.LoX <= o.LoX && o.LoX <= b.HiX &&
		b.LoX <= o.HiX && o.HiX <= b.HiX &&
		b.LoY <= o.LoY && o.LoY <= b.HiY &&
		b.LoY <= o.HiY && o.HiY <= b.HiY
}

// boundingBoxFromGeomT returns a bounding box from a given geom.T.
// Returns nil if no bounding box was found.
func boundingBoxFromGeomT(g geom.T, soType geopb.SpatialObjectType) (*geopb.BoundingBox, error) {
	switch soType {
	case geopb.SpatialObjectType_GeometryType:
		ret := boundingBoxFromGeomTGeometryType(g)
		if ret == nil {
			return nil, nil
		}
		return &ret.BoundingBox, nil
	case geopb.SpatialObjectType_GeographyType:
		rect, err := boundingBoxFromGeomTGeographyType(g)
		if err != nil {
			return nil, err
		}
		if rect.IsEmpty() {
			return nil, nil
		}
		return &geopb.BoundingBox{
			LoX: rect.Lng.Lo,
			HiX: rect.Lng.Hi,
			LoY: rect.Lat.Lo,
			HiY: rect.Lat.Hi,
		}, nil
	}
	return nil, errors.Newf("unknown spatial type: %s", soType)
}

// boundingBoxFromGeomTGeometryType returns an appropriate bounding box for a Geometry type.
func boundingBoxFromGeomTGeometryType(g geom.T) *CartesianBoundingBox {
	if g.Empty() {
		return nil
	}
	bbox := NewCartesianBoundingBox()
	switch g := g.(type) {
	case *geom.GeometryCollection:
		for i := 0; i < g.NumGeoms(); i++ {
			shapeBBox := boundingBoxFromGeomTGeometryType(g.Geom(i))
			if shapeBBox == nil {
				continue
			}
			bbox.AddPoint(shapeBBox.LoX, shapeBBox.LoY)
			bbox.AddPoint(shapeBBox.HiX, shapeBBox.HiY)
		}
	default:
		flatCoords := g.FlatCoords()
		for i := 0; i < len(flatCoords); i += g.Stride() {
			bbox.AddPoint(flatCoords[i], flatCoords[i+1])
		}
	}
	return bbox
}

// boundingBoxFromGeomTGeographyType returns an appropriate bounding box for a Geography type.
func boundingBoxFromGeomTGeographyType(g geom.T) (s2.Rect, error) {
	if g.Empty() {
		return s2.EmptyRect(), nil
	}
	regions, err := S2RegionsFromGeom(g, EmptyBehaviorOmit)
	if err != nil {
		return s2.EmptyRect(), err
	}
	rect := s2.EmptyRect()
	for _, region := range regions {
		rect = rect.Union(region.RectBound())
	}
	return rect, nil
}
