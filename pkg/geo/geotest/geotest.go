// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package geotest

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

// Epsilon is the default epsilon to use in spatial tests.
// This corresponds to ~1cm in lat/lng.
const Epsilon = 0.000001

// RequireGeographyInEpsilon and ensures the geometry shape and SRID are equal,
// and that each coordinate is within the provided epsilon.
func RequireGeographyInEpsilon(t *testing.T, expected, got geo.Geography, epsilon float64) {
	expectedT, err := expected.AsGeomT()
	require.NoError(t, err)
	gotT, err := got.AsGeomT()
	require.NoError(t, err)
	RequireGeomTInEpsilon(t, expectedT, gotT, epsilon)
}

// RequireGeometryInEpsilon and ensures the geometry shape and SRID are equal,
// and that each coordinate is within the provided epsilon.
func RequireGeometryInEpsilon(t *testing.T, expected, got geo.Geometry, epsilon float64) {
	expectedT, err := expected.AsGeomT()
	require.NoError(t, err)
	gotT, err := got.AsGeomT()
	require.NoError(t, err)
	RequireGeomTInEpsilon(t, expectedT, gotT, epsilon)
}

// FlatCoordsInEpsilon ensures the flat coords are within the expected epsilon.
func FlatCoordsInEpsilon(t *testing.T, expected []float64, actual []float64, epsilon float64) {
	require.Equal(t, len(expected), len(actual), "expected %#v, got %#v", expected, actual)
	for i := range expected {
		require.True(t, math.Abs(expected[i]-actual[i]) < epsilon, "expected %#v, got %#v (mismatching at position %d)", expected, actual, i)
	}
}

// RequireGeomTInEpsilon ensures that the geom.T are equal, except for
// coords which should be within the given epsilon.
func RequireGeomTInEpsilon(t *testing.T, expectedT, gotT geom.T, epsilon float64) {
	require.Equal(t, expectedT.SRID(), gotT.SRID())
	require.Equal(t, expectedT.Layout(), gotT.Layout())
	require.IsType(t, expectedT, gotT)
	switch lhs := expectedT.(type) {
	case *geom.Point, *geom.LineString:
		FlatCoordsInEpsilon(t, expectedT.FlatCoords(), gotT.FlatCoords(), epsilon)
	case *geom.MultiPoint, *geom.Polygon, *geom.MultiLineString:
		require.Equal(t, expectedT.Ends(), gotT.Ends())
		FlatCoordsInEpsilon(t, expectedT.FlatCoords(), gotT.FlatCoords(), epsilon)
	case *geom.MultiPolygon:
		require.Equal(t, expectedT.Ends(), gotT.Ends())
		require.Equal(t, expectedT.Endss(), gotT.Endss())
		FlatCoordsInEpsilon(t, expectedT.FlatCoords(), gotT.FlatCoords(), epsilon)
	case *geom.GeometryCollection:
		rhs, ok := gotT.(*geom.GeometryCollection)
		require.True(t, ok)
		require.Len(t, rhs.Geoms(), len(lhs.Geoms()))
		for i := range lhs.Geoms() {
			RequireGeomTInEpsilon(
				t,
				lhs.Geom(i),
				rhs.Geom(i),
				epsilon,
			)
		}
	default:
		panic(errors.AssertionFailedf("unknown geometry type: %T", expectedT))
	}
}
