// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geopb

import (
	"fmt"
	"unsafe"
)

// EWKBHex returns the EWKB-hex version of this data type
func (b *SpatialObject) EWKBHex() string {
	return fmt.Sprintf("%X", b.EWKB)
}

// MemSize returns the size of the spatial object in memory. If deterministic is
// true, then only length of EWKB slice is included - this option should only be
// used when determinism is favored over precision.
func (b *SpatialObject) MemSize(deterministic bool) uintptr {
	var bboxSize uintptr
	if bbox := b.BoundingBox; bbox != nil {
		bboxSize = unsafe.Sizeof(*bbox)
	}
	ewkbSize := uintptr(cap(b.EWKB))
	if deterministic {
		ewkbSize = uintptr(len(b.EWKB))
	}
	return unsafe.Sizeof(*b) + bboxSize + ewkbSize
}

// MultiType returns the corresponding multi-type for a shape type, or unset
// if there is no multi-type.
func (s ShapeType) MultiType() ShapeType {
	switch s {
	case ShapeType_Unset:
		return ShapeType_Unset
	case ShapeType_Point, ShapeType_MultiPoint:
		return ShapeType_MultiPoint
	case ShapeType_LineString, ShapeType_MultiLineString:
		return ShapeType_MultiLineString
	case ShapeType_Polygon, ShapeType_MultiPolygon:
		return ShapeType_MultiPolygon
	case ShapeType_GeometryCollection:
		return ShapeType_GeometryCollection
	default:
		return ShapeType_Unset
	}
}
