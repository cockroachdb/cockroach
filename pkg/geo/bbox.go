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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/twpayne/go-geom"
)

// boundingBoxFromGeom returns a bounding box from a given geom.T.
// Returns nil if no bounding box was found.
func boundingBoxFromGeom(g geom.T) *geopb.BoundingBox {
	bbox := geopb.NewBoundingBox()
	if g.Empty() {
		return nil
	}
	switch g := g.(type) {
	case *geom.GeometryCollection:
		for i := 0; i < g.NumGeoms(); i++ {
			shapeBBox := boundingBoxFromGeom(g.Geom(i))
			if shapeBBox == nil {
				continue
			}
			bbox.Update(shapeBBox.MinX, shapeBBox.MinY)
			bbox.Update(shapeBBox.MaxX, shapeBBox.MaxY)
		}
	default:
		flatCoords := g.FlatCoords()
		for i := 0; i < len(flatCoords); i += g.Stride() {
			// i and i+1 always represent x and y.
			bbox.Update(flatCoords[i], flatCoords[i+1])
		}
	}
	return bbox
}
