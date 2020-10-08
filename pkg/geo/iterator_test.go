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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
)

func TestGeomTIterator(t *testing.T) {
	testCases := []struct {
		desc          string
		emptyBehavior EmptyBehavior
		t             geom.T
		expected      []geom.T
		expectedErr   error
	}{
		{
			"POINT",
			EmptyBehaviorError,
			geom.NewPointFlat(geom.XY, []float64{1, 2}),
			[]geom.T{geom.NewPointFlat(geom.XY, []float64{1, 2})},
			nil,
		},
		{
			"MULTIPOINT",
			EmptyBehaviorError,
			geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 3, 4, 5, 6}),
			[]geom.T{
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{3, 4}),
				geom.NewPointFlat(geom.XY, []float64{5, 6}),
			},
			nil,
		},
		{
			"GEOMETRYCOLLECTION",
			EmptyBehaviorError,
			geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 3, 4, 5, 6}),
			),
			[]geom.T{
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{3, 4}),
				geom.NewPointFlat(geom.XY, []float64{5, 6}),
			},
			nil,
		},
		{
			"GEOMETRYCOLLECTION with EMPTY as error",
			EmptyBehaviorError,
			geom.NewGeometryCollection().MustPush(
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointEmpty(geom.XY),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 3, 4, 5, 6}),
			),
			[]geom.T{
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
			},
			NewEmptyGeometryError(),
		},
		{
			"GEOMETRYCOLLECTION with EMPTY omitted",
			EmptyBehaviorOmit,
			geom.NewGeometryCollection().MustPush(
				geom.NewPointEmpty(geom.XY),
				geom.NewPointEmpty(geom.XY),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointEmpty(geom.XY),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 3, 4, 5, 6}),
				geom.NewPointEmpty(geom.XY),
			),
			[]geom.T{
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{3, 4}),
				geom.NewPointFlat(geom.XY, []float64{5, 6}),
			},
			nil,
		},
		{
			"nested GEOMETRYCOLLECTION with EMPTY omitted",
			EmptyBehaviorOmit,
			geom.NewGeometryCollection().MustPush(
				geom.NewPointEmpty(geom.XY),
				geom.NewGeometryCollection().MustPush(
					geom.NewPointFlat(geom.XY, []float64{10, 11}),
					geom.NewPointFlat(geom.XY, []float64{12, 13}),
					geom.NewGeometryCollection().MustPush(
						geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 3, 4, 5, 6}),
					),
				),
				geom.NewPointEmpty(geom.XY),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointEmpty(geom.XY),
				geom.NewMultiPointFlat(geom.XY, []float64{1, 2, 3, 4, 5, 6}),
				geom.NewPointEmpty(geom.XY),
			),
			[]geom.T{
				geom.NewPointFlat(geom.XY, []float64{10, 11}),
				geom.NewPointFlat(geom.XY, []float64{12, 13}),

				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{3, 4}),
				geom.NewPointFlat(geom.XY, []float64{5, 6}),

				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{1, 2}),
				geom.NewPointFlat(geom.XY, []float64{3, 4}),
				geom.NewPointFlat(geom.XY, []float64{5, 6}),
			},
			nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			it := NewGeomTIterator(tc.t, tc.emptyBehavior)
			var err error
			results := []geom.T{}
			for {
				next, hasNext, currErr := it.Next()
				if currErr != nil {
					err = currErr
					break
				}
				if !hasNext {
					break
				}
				results = append(results, next)
			}
			if tc.expectedErr != nil {
				require.EqualError(t, err, tc.expectedErr.Error())
				require.Equal(t, tc.expected, results)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, results)
			}
		})
	}
}
