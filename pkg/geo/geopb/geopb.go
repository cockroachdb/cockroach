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

// SpatialObjectShape is a type alias for a shape inside SpatialObject.
type SpatialObjectShape = isSpatialObject_Shape

// MakeGeometryCollectionSpatialObjectShape makes the nested structure
// for the oneof shape GeometryCollection element.
func MakeGeometryCollectionSpatialObjectShape(shapes []Shape) SpatialObjectShape {
	return &SpatialObject_GeometryCollectionShape{
		GeometryCollectionShape: &GeometryCollectionShape{
			Shapes: shapes,
		},
	}
}

// MakeSingleSpatialObjectShape makes the nested structure
// for the oneof SingleShape element.
func MakeSingleSpatialObjectShape(shape Shape) SpatialObjectShape {
	return &SpatialObject_SingleShape{
		SingleShape: &shape,
	}
}
