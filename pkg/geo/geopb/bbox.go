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

import "math"

// NewBoundingBox returns a properly initialized empty bounding box.
func NewBoundingBox() *BoundingBox {
	return &BoundingBox{
		MinX: math.MaxFloat64,
		MaxX: -math.MaxFloat64,
		MinY: math.MaxFloat64,
		MaxY: -math.MaxFloat64,
	}
}

// Update updates the BoundingBox coordinates.
func (b *BoundingBox) Update(x, y float64) {
	b.MinX = math.Min(b.MinX, x)
	b.MaxX = math.Max(b.MaxX, x)
	b.MinY = math.Min(b.MinY, y)
	b.MaxY = math.Max(b.MaxY, y)
}

// Intersects returns whether the BoundingBoxes intersect.
// Empty bounding boxes never intersect.
func (b *BoundingBox) Intersects(o *BoundingBox) bool {
	// If either side is empty, they do not intersect.
	if b == nil || o == nil {
		return false
	}
	// If any rectangle is completely above the other, they do not intersect.
	if b.MinY > o.MaxY || o.MinY > b.MaxY {
		return false
	}
	// If any rectangle is completely on the left of the other, they do not intersect.
	if b.MinX > o.MaxX || o.MinX > b.MaxX {
		return false
	}
	return true
}
