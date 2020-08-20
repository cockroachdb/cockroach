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
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestParseCartesianBoundingBox(t *testing.T) {
	testCases := []struct {
		s             string
		expected      CartesianBoundingBox
		expectedError bool
	}{
		{
			s: "box(1 2,3 4)",
			expected: CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: 1,
					LoY: 2,
					HiX: 3,
					HiY: 4,
				},
			},
		},
		{
			s: "BOX(1 2,3 4)",
			expected: CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: 1,
					LoY: 2,
					HiX: 3,
					HiY: 4,
				},
			},
		},
		{
			s:             "invalid",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.s, func(t *testing.T) {
			ret, err := ParseCartesianBoundingBox(tc.s)
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ret)
			}
		})
	}
}

func TestCartesianBoundingBox(t *testing.T) {
	testCases := []struct {
		lhs      *CartesianBoundingBox
		rhs      *CartesianBoundingBox
		expected *CartesianBoundingBox
	}{
		{nil, nil, nil},
		{
			nil,
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -1, LoY: -1, HiX: 1, HiY: 1,
				},
			},
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -1, LoY: -1, HiX: 1, HiY: 1,
				},
			},
		},
		{
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -2, LoY: 0, HiX: 1, HiY: 2,
				},
			},
			nil,
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -2, LoY: 0, HiX: 1, HiY: 2,
				},
			},
		},
		{
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -2, LoY: 0, HiX: 1, HiY: 2,
				},
			},
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -1, LoY: -1, HiX: 1, HiY: 1,
				},
			},
			&CartesianBoundingBox{
				BoundingBox: geopb.BoundingBox{
					LoX: -2, LoY: -1, HiX: 1, HiY: 2,
				},
			},
		},
	}
	for i, tc := range testCases {
		t.Run(strconv.Itoa(i+1), func(t *testing.T) {
			require.Equal(t, tc.expected, tc.lhs.Combine(tc.rhs))
		})
	}
}

func TestBoundingBoxFromGeomT(t *testing.T) {
	testCases := []struct {
		soType   geopb.SpatialObjectType
		g        geom.T
		expected *geopb.BoundingBox
	}{
		{geopb.SpatialObjectType_GeometryType, geom.NewPointFlat(geom.XY, []float64{-15, -20}), &geopb.BoundingBox{LoX: -15, HiX: -15, LoY: -20, HiY: -20}},
		{geopb.SpatialObjectType_GeometryType, geom.NewPointFlat(geom.XY, []float64{0, 0}), &geopb.BoundingBox{LoX: 0, HiX: 0, LoY: 0, HiY: 0}},
		{geopb.SpatialObjectType_GeometryType, testGeomPoint, &geopb.BoundingBox{LoX: 1, HiX: 1, LoY: 2, HiY: 2}},
		{geopb.SpatialObjectType_GeometryType, testGeomLineString, &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
		{geopb.SpatialObjectType_GeometryType, geom.NewLineStringFlat(geom.XY, []float64{-15, -20, -30, -40}), &geopb.BoundingBox{LoX: -30, HiX: -15, LoY: -40, HiY: -20}},
		{geopb.SpatialObjectType_GeometryType, testGeomPolygon, &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
		{geopb.SpatialObjectType_GeometryType, testGeomMultiPoint, &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
		{geopb.SpatialObjectType_GeometryType, testGeomMultiLineString, &geopb.BoundingBox{LoX: 1, HiX: 4, LoY: 1, HiY: 4}},
		{geopb.SpatialObjectType_GeometryType, testGeomMultiPolygon, &geopb.BoundingBox{LoX: 1, HiX: 4, LoY: 1, HiY: 4}},
		{geopb.SpatialObjectType_GeometryType, testGeomGeometryCollection, &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
		{geopb.SpatialObjectType_GeometryType, emptyGeomPoint, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomLineString, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomPolygon, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomMultiPoint, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomMultiLineString, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomMultiPolygon, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomGeometryCollection, nil},
		{geopb.SpatialObjectType_GeometryType, emptyGeomPointInGeometryCollection, &geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
		{geopb.SpatialObjectType_GeometryType, emptyGeomObjectsInGeometryCollection, nil},

		{geopb.SpatialObjectType_GeographyType, geom.NewPointFlat(geom.XY, []float64{-15, -20}), &geopb.BoundingBox{LoX: -0.2617993877991494, LoY: -0.3490658503988659, HiX: -0.2617993877991494, HiY: -0.3490658503988659}},
		{geopb.SpatialObjectType_GeographyType, geom.NewPointFlat(geom.XY, []float64{0, 0}), &geopb.BoundingBox{LoX: 0, LoY: 0, HiX: 0, HiY: 0}},
		{geopb.SpatialObjectType_GeographyType, testGeomPoint, &geopb.BoundingBox{LoX: 0.017453292519943295, LoY: 0.03490658503988659, HiX: 0.017453292519943295, HiY: 0.03490658503988659}},
		{geopb.SpatialObjectType_GeographyType, testGeomLineString, &geopb.BoundingBox{LoX: 0.017453292519943292, LoY: 0.01745329251994285, HiX: 0.03490658503988659, HiY: 0.03490658503988703}},
		{geopb.SpatialObjectType_GeographyType, geom.NewLineStringFlat(geom.XY, []float64{-15, -20, -30, -40}), &geopb.BoundingBox{LoX: -0.5235987755982988, LoY: -0.6981317007977321, HiX: -0.2617993877991494, HiY: -0.34906585039886545}},
		{geopb.SpatialObjectType_GeographyType, testGeomPolygon, &geopb.BoundingBox{LoX: 0.017453292519943292, LoY: 0.01745329251994285, HiX: 0.03490658503988659, HiY: 0.03490791314678354}},
		// Reverse the orientation of testGeomPolygon -- same result.
		{geopb.SpatialObjectType_GeographyType, geom.NewPolygonFlat(geom.XY, []float64{1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 1.0, 1.0}, []int{8}),
			&geopb.BoundingBox{LoX: 0.017453292519943292, LoY: 0.01745329251994285, HiX: 0.03490658503988659, HiY: 0.03490791314678354}},
		{geopb.SpatialObjectType_GeographyType, geom.NewPolygonFlat(geom.XY, []float64{150, 85, 160, 85, -20, 85, -30, 85, 150, 85}, []int{10}),
			&geopb.BoundingBox{LoX: -3.141592653589793, LoY: 1.4835298641951797, HiX: 3.141592653589793, HiY: 1.5707963267948966}},
		// Reversed the previous polygon -- same result.
		{geopb.SpatialObjectType_GeographyType, geom.NewPolygonFlat(geom.XY, []float64{150, 85, -30, 85, -20, 85, 160, 85, 150, 85}, []int{10}),
			&geopb.BoundingBox{LoX: -3.141592653589793, LoY: 1.4835298641951797, HiX: 3.141592653589793, HiY: 1.5707963267948966}},
		{geopb.SpatialObjectType_GeographyType, testGeomMultiPoint, &geopb.BoundingBox{LoX: 0.017453292519943295, LoY: 0.017453292519943295, HiX: 0.03490658503988659, HiY: 0.03490658503988659}},
		{geopb.SpatialObjectType_GeographyType, testGeomMultiLineString, &geopb.BoundingBox{LoX: 0.017453292519943292, LoY: 0.01745329251994285, HiX: 0.06981317007977318, HiY: 0.06981317007977363}},
		{geopb.SpatialObjectType_GeographyType, testGeomMultiPolygon, &geopb.BoundingBox{LoX: 0.017453292519943292, LoY: 0.01745329251994285, HiX: 0.06981317007977318, HiY: 0.06981581982279463}},
		{geopb.SpatialObjectType_GeographyType, testGeomGeometryCollection, &geopb.BoundingBox{LoX: 0.017453292519943295, LoY: 0.017453292519943295, HiX: 0.03490658503988659, HiY: 0.03490658503988659}},
		{geopb.SpatialObjectType_GeographyType, emptyGeomPoint, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomLineString, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomPolygon, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomMultiPoint, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomMultiLineString, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomMultiPolygon, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomGeometryCollection, nil},
		{geopb.SpatialObjectType_GeographyType, emptyGeomPointInGeometryCollection, &geopb.BoundingBox{LoX: 0.017453292519943292, LoY: 0.01745329251994285, HiX: 0.03490658503988659, HiY: 0.03490658503988703}},
		{geopb.SpatialObjectType_GeographyType, emptyGeomObjectsInGeometryCollection, nil},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s: %#v", tc.soType, tc.g), func(t *testing.T) {
			bbox, err := boundingBoxFromGeomT(tc.g, tc.soType)
			require.NoError(t, err)
			require.Equal(t, tc.expected, bbox)
		})
	}
}

func TestCartesianBoundingBoxIntersects(t *testing.T) {
	testCases := []struct {
		desc     string
		a        *CartesianBoundingBox
		b        *CartesianBoundingBox
		expected bool
	}{
		{
			desc:     "same bounding box intersects",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			expected: true,
		},
		{
			desc:     "overlapping bounding box intersects",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.5, HiX: 1.5, LoY: 0.5, HiY: 1.5}},
			expected: true,
		},
		{
			desc:     "overlapping bounding box intersects",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.5, HiX: 1.5, LoY: 0.5, HiY: 1.5}},
			expected: true,
		},
		{
			desc:     "touching bounding box intersects",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
			expected: true,
		},
		{
			desc:     "bounding box that is left does not intersect",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 1.5, HiX: 2, LoY: 0, HiY: 1}},
			expected: false,
		},
		{
			desc:     "higher bounding box does not intersect",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 1.5, HiY: 2}},
			expected: false,
		},
		{
			desc:     "completely disjoint bounding box does not intersect",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: -3, HiX: -2, LoY: 1.5, HiY: 2}},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.a.Intersects(tc.b))
			require.Equal(t, tc.expected, tc.b.Intersects(tc.a))
		})
	}
}

func TestCartesianBoundingBoxCovers(t *testing.T) {
	testCases := []struct {
		desc     string
		a        *CartesianBoundingBox
		b        *CartesianBoundingBox
		expected bool
	}{
		{
			desc:     "same bounding box covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			expected: true,
		},
		{
			desc:     "nested bounding box covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.1, HiX: 0.9, LoY: 0.1, HiY: 0.9}},
			expected: true,
		},
		{
			desc:     "side touching bounding box covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.1, HiX: 0.9, LoY: 0.1, HiY: 0.9}},
			expected: true,
		},
		{
			desc:     "top touching bounding box covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.1, HiX: 0.9, LoY: 0, HiY: 1}},
			expected: true,
		},
		{
			desc:     "reversed nested bounding box does not cover",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.1, HiX: 0.9, LoY: 0.1, HiY: 0.9}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			expected: false,
		},
		{
			desc:     "overlapping bounding box from the left covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.5, HiX: 1.5, LoY: 0.5, HiY: 1.5}},
			expected: false,
		},
		{
			desc:     "overlapping bounding box from the right covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0.5, HiX: 1.5, LoY: 0.5, HiY: 1.5}},
			expected: false,
		},
		{
			desc:     "touching bounding box covers",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 1, HiX: 2, LoY: 1, HiY: 2}},
			expected: false,
		},
		{
			desc:     "bounding box that is left does not cover",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 1.5, HiX: 2, LoY: 0, HiY: 1}},
			expected: false,
		},
		{
			desc:     "higher bounding box does not cover",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 1.5, HiY: 2}},
			expected: false,
		},
		{
			desc:     "completely disjoint bounding box does not cover",
			a:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: 0, HiX: 1, LoY: 0, HiY: 1}},
			b:        &CartesianBoundingBox{BoundingBox: geopb.BoundingBox{LoX: -3, HiX: -2, LoY: 1.5, HiY: 2}},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.a.Covers(tc.b))
		})
	}
}
