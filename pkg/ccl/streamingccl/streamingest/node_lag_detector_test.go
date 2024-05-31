// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDetectLaggingNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stats := func(frontier int) *ingestProcStats {
		return &ingestProcStats{
			LowWaterMark: hlc.Timestamp{WallTime: int64(frontier)},
		}
	}

	statsByNode := func(stats ...*ingestProcStats) ingestStatsByNode {
		ingestStats := make(ingestStatsByNode)
		for i, s := range stats {
			ingestStats[base.SQLInstanceID(i+1)] = s
		}
		return ingestStats
	}

	for _, tc := range []struct {
		name             string
		details          ingestStatsByNode
		laggingNode      base.SQLInstanceID
		minLagDifference time.Duration
	}{
		{
			name: "simple",
			details: statsByNode(
				stats(3),
				stats(1),
				stats(6)),
			laggingNode:      2,
			minLagDifference: 2,
		},
		{
			name:             "singleNode",
			details:          statsByNode(stats(1)),
			laggingNode:      0,
			minLagDifference: 0,
		},
		{
			name:             "zeroNode",
			details:          statsByNode(),
			laggingNode:      0,
			minLagDifference: 0,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			laggingNode, minLagDifference := computeMinLagDifference(tc.details)
			require.Equal(t, tc.laggingNode, laggingNode)
			require.Equal(t, tc.minLagDifference, minLagDifference)
		})
	}
}
