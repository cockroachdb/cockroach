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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundingBoxIntersects(t *testing.T) {
	testCases := []struct {
		desc     string
		a        *BoundingBox
		b        *BoundingBox
		expected bool
	}{
		{
			desc:     "same bounding box intersects",
			a:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			b:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			expected: true,
		},
		{
			desc:     "overlapping bounding box intersects",
			a:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			b:        &BoundingBox{MinX: 0.5, MaxX: 1.5, MinY: 0.5, MaxY: 1.5},
			expected: true,
		},
		{
			desc:     "touching bounding box intersects",
			a:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			b:        &BoundingBox{MinX: 1, MaxX: 2, MinY: 1, MaxY: 2},
			expected: true,
		},
		{
			desc:     "bounding box that is left does not intersect",
			a:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			b:        &BoundingBox{MinX: 1.5, MaxX: 2, MinY: 0, MaxY: 1},
			expected: false,
		},
		{
			desc:     "higher bounding box does not intersect",
			a:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			b:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 1.5, MaxY: 2},
			expected: false,
		},
		{
			desc:     "completely disjoint bounding box does not intersect",
			a:        &BoundingBox{MinX: 0, MaxX: 1, MinY: 0, MaxY: 1},
			b:        &BoundingBox{MinX: -3, MaxX: -2, MinY: 1.5, MaxY: 2},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.a.Intersects(tc.b))
		})
	}
}
