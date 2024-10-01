// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rditer

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestComputeStats verifies that the sum of the stats returned by
// ComputeStatsForRangeUserOnly and ComputeStatsForRangeExcludingUser equals the
// total range stats returned by ComputeStatsForRange.
func TestComputeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storage.DisableMetamorphicSimpleValueEncoding(t)

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}

	createRangeData(t, eng, desc)
	nowNanos := time.Now().UnixNano()

	userOnly, err := ComputeStatsForRangeUserOnly(context.Background(), &desc, eng, nowNanos)
	require.NoError(t, err)
	nonUserOnly, err := ComputeStatsForRangeExcludingUser(context.Background(), &desc, eng, nowNanos)
	require.NoError(t, err)
	all, err := ComputeStatsForRange(context.Background(), &desc, eng, nowNanos)
	require.NoError(t, err)

	userPlusNonUser := userOnly
	userPlusNonUser.Add(nonUserOnly)
	require.Equal(t, all, userPlusNonUser)
}
