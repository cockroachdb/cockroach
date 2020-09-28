// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geopb

import (
	"fmt"
	"unsafe"
)

// EWKBHex returns the EWKB-hex version of this data type
func (b *SpatialObject) EWKBHex() string {
	return fmt.Sprintf("%X", b.EWKB)
}

// MemSize returns the size of the spatial object in memory.
func (b *SpatialObject) MemSize() uintptr {
	var bboxSize uintptr
	if bbox := b.BoundingBox; bbox != nil {
		bboxSize = unsafe.Sizeof(*bbox)
	}
	return unsafe.Sizeof(*b) + bboxSize + uintptr(len(b.EWKB))
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
