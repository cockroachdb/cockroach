// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/twpayne/go-geom"
)

// FlipCoordinates returns a modified g whose X, Y coordinates are flipped.
func FlipCoordinates(g geo.Geometry) (geo.Geometry, error) {
	if g.Empty() {
		return g, nil
	}

	t, err := g.AsGeomT()
	if err != nil {
		return geo.Geometry{}, err
	}

	newT, err := applyOnCoordsForGeomT(t, func(l geom.Layout, dst, src []float64) error {
		dst[0], dst[1] = src[1], src[0]
		if l.ZIndex() != -1 {
			dst[l.ZIndex()] = src[l.ZIndex()]
		}
		if l.MIndex() != -1 {
			dst[l.MIndex()] = src[l.MIndex()]
		}
		return nil
	})
	if err != nil {
		return geo.Geometry{}, err
	}

	return geo.MakeGeometryFromGeomT(newT)
}
