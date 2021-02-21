// Copyright 2021 The Cockroach Authors.
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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShapeType(t *testing.T) {
	// Since protos do not allow arithmetic operators in enums, double check values match
	// the desired values using Go code.
	testCases := []struct {
		base ShapeType
		z    ShapeType
		m    ShapeType
		zm   ShapeType
	}{
		{ShapeType_Point, ShapeType_PointZ, ShapeType_PointM, ShapeType_PointZM},
		{ShapeType_LineString, ShapeType_LineStringZ, ShapeType_LineStringM, ShapeType_LineStringZM},
		{ShapeType_Polygon, ShapeType_PolygonZ, ShapeType_PolygonM, ShapeType_PolygonZM},
		{ShapeType_MultiPoint, ShapeType_MultiPointZ, ShapeType_MultiPointM, ShapeType_MultiPointZM},
		{ShapeType_MultiLineString, ShapeType_MultiLineStringZ, ShapeType_MultiLineStringM, ShapeType_MultiLineStringZM},
		{ShapeType_MultiPolygon, ShapeType_MultiPolygonZ, ShapeType_MultiPolygonM, ShapeType_MultiPolygonZM},
		{ShapeType_Geometry, ShapeType_GeometryZ, ShapeType_GeometryM, ShapeType_GeometryZM},
		{ShapeType_GeometryCollection, ShapeType_GeometryCollectionZ, ShapeType_GeometryCollectionM, ShapeType_GeometryCollectionZM},
	}

	for _, tc := range testCases {
		t.Run(tc.base.String(), func(t *testing.T) {
			require.Equal(t, tc.base, tc.z.To2D())
			require.Equal(t, tc.base, tc.m.To2D())
			require.Equal(t, tc.base, tc.zm.To2D())

			require.Equal(t, tc.z, tc.base|ZShapeTypeFlag)
			require.Equal(t, tc.m, tc.base|MShapeTypeFlag)
			require.Equal(t, tc.zm, tc.base|ZShapeTypeFlag|MShapeTypeFlag)
		})
	}
}
