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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeLongitudeDegrees(t *testing.T) {
	testCases := []struct {
		lng      float64
		expected float64
	}{
		{180, 180},
		{-180, -180},
		{181, -179},
		{360, 0},
		{-360, 0},
		{95, 95},
		{0, 0},
		{-10, -10},
		{10, 10},
		{555, -165},
		{-555, 165},
	}

	for _, tc := range testCases {
		t.Run(strconv.FormatFloat(tc.lng, 'f', -1, 64), func(t *testing.T) {
			require.Equal(t, tc.expected, NormalizeLongitudeDegrees(tc.lng))
		})
	}
}

func TestNormalizeLatitudeDegrees(t *testing.T) {
	testCases := []struct {
		lat      float64
		expected float64
	}{
		{0, 0},
		{10, 10},
		{-10, -10},
		{95, 85},
		{-95, -85},
		{90, 90},
		{-90, -90},
		{-180, 0},
		{180, 0},
		{270, -90},
		{-270, 90},
		{555, -15},
		{-555, 15},
	}

	for _, tc := range testCases {
		t.Run(strconv.FormatFloat(tc.lat, 'f', -1, 64), func(t *testing.T) {
			require.Equal(t, tc.expected, NormalizeLatitudeDegrees(tc.lat))
		})
	}
}
