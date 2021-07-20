// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvprober

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPlannerEnforcesRateLimit(t *testing.T) {
	p := newMeta2Planner(nil, cluster.MakeTestingClusterSettings())
	p.getRateLimit = func(settings *cluster.Settings) time.Duration {
		return 1 * time.Second
	}

	now := timeutil.Now()
	fakeNow := func() time.Time {
		return now
	}
	p.now = fakeNow

	p.getNMeta2KVs = func(context.Context, dbScan, int64, roachpb.Key, time.Duration) (values []kv.KeyValue, keys roachpb.Key, e error) {
		return nil, nil, nil
	}
	p.meta2KVsToPlan = func([]kv.KeyValue) (steps []Step, e error) {
		return []Step{{
			RangeID: 3,
		}}, nil
	}

	// Rate limit not hit since first call to next.
	ctx := context.Background()
	_, err := p.next(ctx)
	require.NoError(t, err)

	// Rate limit hit since time not moved forward.
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "planner rate limit hit", err.Error())

	// Rate limit not hit since time moved forward enough.
	now = now.Add(2 * time.Second)
	_, err = p.next(ctx)
	require.NoError(t, err)

	// Rate limit hit since time not moved forward enough.
	now = now.Add(600 * time.Millisecond)
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "planner rate limit hit", err.Error())

	// Rate limit not hit since time moved forward enough. 600ms + 600ms
	// is enough wait time to not hit the rate limit.
	now = now.Add(600 * time.Millisecond)
	_, err = p.next(ctx)
	require.NoError(t, err)

	// Rate limit hit since time not moved forward enough.
	now = now.Add(400 * time.Millisecond)
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "planner rate limit hit", err.Error())

	// Rate limit hit since time not moved forward enough. 400ms + 400ms
	// is not enough wait time to not hit the rate limit.
	now = now.Add(400 * time.Millisecond)
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "planner rate limit hit", err.Error())

	// Rate limit not hit since time moved forward enough. 400ms + 400ms +
	// 400ms is enough wait time to not hit the rate limit.
	now = now.Add(400 * time.Millisecond)
	_, err = p.next(ctx)
	require.NoError(t, err)

	// Whether planning succeeds or fails shouldn't affect the rate limiting!
	p.meta2KVsToPlan = func([]kv.KeyValue) (steps []Step, e error) {
		return nil, errors.New("boom")
	}

	// Rate limit hit since time not moved forward enough.
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "planner rate limit hit", err.Error())

	// Rate limit hit since time not moved forward enough.
	now = now.Add(600 * time.Millisecond)
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "planner rate limit hit", err.Error())

	// Rate limit not hit since time moved forward enough. 600ms + 600ms
	// is enough wait time to not hit the rate limit.
	now = now.Add(600 * time.Millisecond)
	_, err = p.next(ctx)
	require.Error(t, err)
	require.Regexp(t, "boom", err.Error()) // plan failure instead of rate limit!
}

func TestGetRateLimit(t *testing.T) {
	ctx := context.Background()
	s := cluster.MakeTestingClusterSettings()

	readInterval.Override(ctx, &s.SV, time.Second)
	numStepsToPlanAtOnce.Override(ctx, &s.SV, 60)

	got := getRateLimitImpl(s)
	require.Equal(t, 30*time.Second, got)
}
