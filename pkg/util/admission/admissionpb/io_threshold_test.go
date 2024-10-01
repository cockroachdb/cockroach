// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admissionpb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIOThreshold(t *testing.T) {
	var testCases = []struct {
		iot   IOThreshold
		score float64
	}{
		{
			// Score is 30/20.
			iot: IOThreshold{
				L0NumSubLevels:           30,
				L0NumSubLevelsThreshold:  20,
				L0NumFiles:               200,
				L0NumFilesThreshold:      1000,
				L0Size:                   50,
				L0MinimumSizePerSubLevel: 0,
			},
			score: 1.5,
		},
		// Score is 2500/1000.
		{
			iot: IOThreshold{
				L0NumSubLevels:           30,
				L0NumSubLevelsThreshold:  20,
				L0NumFiles:               2500,
				L0NumFilesThreshold:      1000,
				L0Size:                   50,
				L0MinimumSizePerSubLevel: 0,
			},
			score: 2.5,
		},
		// Max sub-levels is 5000/1000=5, and min sub-levels is 30/3=10. Actual
		// sub-levels is 30. So we use 10 sub-levels and score is 10/20.
		{
			iot: IOThreshold{
				L0NumSubLevels:           30,
				L0NumSubLevelsThreshold:  20,
				L0NumFiles:               250,
				L0NumFilesThreshold:      1000,
				L0Size:                   5000,
				L0MinimumSizePerSubLevel: 1000,
			},
			score: 0.5,
		},
		// Max sub-levels is 5000/1000=5, and min sub-levels is 13/3=4. Actual
		// sub-levels is 13. So we use 5 sub-levels and score is 5/20.
		{
			iot: IOThreshold{
				L0NumSubLevels:           13,
				L0NumSubLevelsThreshold:  20,
				L0NumFiles:               250,
				L0NumFilesThreshold:      1000,
				L0Size:                   5000,
				L0MinimumSizePerSubLevel: 1000,
			},
			score: 0.25,
		},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc.iot), func(t *testing.T) {
			score, overloaded := tc.iot.Score()
			require.Equal(t, tc.score > 1.0, overloaded)
			require.Equal(t, tc.score, score)
		})
	}
}
