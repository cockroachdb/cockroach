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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/stretchr/testify/require"
)

func TestParseBufferParams(t *testing.T) {
	testCases := []struct {
		s string
		d float64

		expected  BufferParams
		expectedD float64
	}{
		{
			s: "",
			d: 100,
			expected: BufferParams{p: geos.BufferParams{
				EndCapStyle:      geos.BufferParamsEndCapStyleRound,
				JoinStyle:        geos.BufferParamsJoinStyleRound,
				SingleSided:      false,
				QuadrantSegments: 8,
				MitreLimit:       5.0,
			}},
			expectedD: 100,
		},
		{
			s: "endcap=flat  join=mitre quad_segs=4",
			d: 100,
			expected: BufferParams{p: geos.BufferParams{
				EndCapStyle:      geos.BufferParamsEndCapStyleFlat,
				JoinStyle:        geos.BufferParamsJoinStyleMitre,
				SingleSided:      false,
				QuadrantSegments: 4,
				MitreLimit:       5.0,
			}},
			expectedD: 100,
		},
		{
			s: "side=left",
			d: 100,
			expected: BufferParams{p: geos.BufferParams{
				EndCapStyle:      geos.BufferParamsEndCapStyleRound,
				JoinStyle:        geos.BufferParamsJoinStyleRound,
				SingleSided:      true,
				QuadrantSegments: 8,
				MitreLimit:       5.0,
			}},
			expectedD: 100,
		},
		{
			s: "side=right",
			d: 100,
			expected: BufferParams{p: geos.BufferParams{
				EndCapStyle:      geos.BufferParamsEndCapStyleRound,
				JoinStyle:        geos.BufferParamsJoinStyleRound,
				SingleSided:      true,
				QuadrantSegments: 8,
				MitreLimit:       5.0,
			}},
			expectedD: -100,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.s, func(t *testing.T) {
			s, d, err := ParseBufferParams(tc.s, tc.d)
			require.NoError(t, err)
			require.Equal(t, tc.expected, s)
			require.Equal(t, tc.expectedD, d)
		})
	}
}
