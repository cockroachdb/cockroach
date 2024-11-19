// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestDetectLaggingNode(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	detail := func(srcID, dstID, frontier int) frontierExecutionDetails {
		unixTime := timeutil.Unix(int64(frontier), 0)
		return frontierExecutionDetails{
			srcInstanceID:  base.SQLInstanceID(srcID),
			destInstanceID: base.SQLInstanceID(dstID),
			frontierTS:     hlc.Timestamp{WallTime: unixTime.UnixNano()},
		}
	}

	makeDetails := func(details ...frontierExecutionDetails) []frontierExecutionDetails {
		for i := len(details) - 1; i > 0; i-- {
			j := rng.Intn(i + 1)
			details[i], details[j] = details[j], details[i]
		}
		return details
	}

	for _, tc := range []struct {
		name                     string
		details                  []frontierExecutionDetails
		laggingNode              base.SQLInstanceID
		meanLagDifferenceSeconds int
	}{
		{
			name: "simple",
			details: makeDetails(
				detail(1, 1, 3),
				detail(2, 2, 1),
				detail(3, 3, 8)),
			laggingNode:              2,
			meanLagDifferenceSeconds: 3,
		},
		{
			name: "aggregateOnLagger",
			details: makeDetails(
				detail(1, 1, 4),
				detail(2, 2, 1),
				detail(3, 2, 5),
				detail(4, 3, 4)),
			laggingNode:              2,
			meanLagDifferenceSeconds: 2,
		},
		{
			name: "aggregateOnBoth",
			details: makeDetails(
				detail(1, 1, 1),
				detail(2, 1, 6),
				detail(3, 2, 2),
				detail(4, 2, 4)),

			laggingNode:              1,
			meanLagDifferenceSeconds: 0,
		},
		{
			name: "singleNode",
			details: makeDetails(
				detail(1, 1, 1)),
			laggingNode:              0,
			meanLagDifferenceSeconds: 0,
		},
		{
			name: "singleNodeMultiplePartitions",
			details: makeDetails(
				detail(1, 1, 1),
				detail(2, 1, 2),
			),
			laggingNode:              0,
			meanLagDifferenceSeconds: 0,
		},
		{
			name:                     "zeroNode",
			details:                  makeDetails(),
			laggingNode:              0,
			meanLagDifferenceSeconds: 0,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			laggingNode, meanLagDifference := computeMeanLagDifference(ctx, tc.details)
			require.Equal(t, tc.laggingNode, laggingNode)
			require.Equal(t, time.Duration(tc.meanLagDifferenceSeconds)*time.Second, meanLagDifference)
		})
	}
}
