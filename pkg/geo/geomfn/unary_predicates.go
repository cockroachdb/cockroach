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
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
)

// IsCollection returns whether the given geometry is of a collection type.
func IsCollection(g *geo.Geometry) (bool, error) {
	switch g.ShapeType() {
	case geopb.ShapeType_MultiPoint, geopb.ShapeType_MultiLineString, geopb.ShapeType_MultiPolygon,
		geopb.ShapeType_GeometryCollection:
		return true, nil
	default:
		return false, nil
	}
}

// IsEmpty returns whether the given geometry is empty.
func IsEmpty(g *geo.Geometry) (bool, error) {
	return g.Empty(), nil
}
