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

// NewBoundingBox returns a properly initialized bounding box.
func NewBoundingBox() *BoundingBox {
	return &BoundingBox{
		MinX: math.MaxFloat64,
		MaxX: math.SmallestNonzeroFloat64,
		MinY: math.MaxFloat64,
		MaxY: math.SmallestNonzeroFloat64,
	}
}

// Update updates the BoundingBox coordinates.
func (b *BoundingBox) Update(x, y float64) {
	b.MinX = math.Min(b.MinX, x)
	b.MaxX = math.Max(b.MaxX, x)
	b.MinY = math.Min(b.MinY, y)
	b.MaxY = math.Max(b.MaxY, y)
}
