// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import (
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestTokenFill(t *testing.T) {
	tests := []struct {
		size         int
		period       time.Duration
		duration     time.Duration
		fillDuration time.Duration
		before       uint
		after        uint
	}{
		{size: 10, period: time.Second, duration: time.Second, fillDuration: time.Second, before: 0, after: 1},
		{size: 10, period: time.Second, duration: time.Second, fillDuration: time.Second, before: 5, after: 6},
		{size: 10, period: time.Second, duration: 5 * time.Second, fillDuration: 5 * time.Second, before: 5, after: 10},
		{size: 10, period: time.Second, duration: time.Second, fillDuration: time.Second, before: 9, after: 10},
		{size: 10, period: time.Second, duration: time.Second, fillDuration: time.Second, before: 10, after: 10},
		{size: 10, period: time.Second, duration: time.Second, fillDuration: time.Second, before: 100, after: 10},
		{size: 10, period: time.Second, duration: time.Second, fillDuration: time.Second, before: 100, after: 10},

		{size: 60, period: time.Minute, duration: time.Minute, fillDuration: 60 * time.Second, before: 32, after: 33},

		// If not enough time has passed to generate a new token, the filledAt time should stay the same.
		{size: 10, period: time.Second, duration: time.Millisecond, fillDuration: time.Duration(0), before: 0, after: 0},
		{size: 10, period: time.Second, duration: time.Millisecond, fillDuration: time.Duration(0), before: 10, after: 10},
		{size: 60, period: time.Minute, duration: 59 * time.Second, fillDuration: time.Duration(0), before: 32, after: 32},

		// The filled at time only advances far enough to generate a whole token.
		{size: 60, period: time.Minute, duration: 90 * time.Second, fillDuration: 60 * time.Second, before: 32, after: 33},
		{size: 10, period: time.Second, duration: 100*time.Second + 30*time.Millisecond, fillDuration: 100 * time.Second, before: 2, after: 10},
	}
	for _, tc := range tests {
		now := timeutil.Now()
		bucket := tokenBucket{
			policy: BucketPolicy{
				Capacity:   tc.size,
				FillPeriod: tc.period,
			},
			tokens:   tc.before,
			filledAt: now,
		}
		fillAtBefore := bucket.filledAt
		bucket.fill(now.Add(tc.duration))
		require.Equal(t, bucket.tokens, tc.after, "test parameters: %v", tc)
		require.Equal(t, bucket.filledAt.Sub(fillAtBefore), tc.fillDuration, "test parameters: %v", tc)
	}
}

func TestReturnToken(t *testing.T) {
	tests := []struct {
		size   int
		before uint
		after  uint
	}{
		{size: 10, before: 0, after: 1},
		{size: 10, before: 5, after: 6},
		{size: 10, before: 9, after: 10},
		{size: 10, before: 10, after: 10},
		{size: 5, before: 10, after: 5},
	}
	for _, tc := range tests {
		bucket := tokenBucket{
			policy: BucketPolicy{
				Capacity:   tc.size,
				FillPeriod: math.MaxInt32,
			},
			tokens: tc.before,
		}
		bucket.returnToken()
		require.Equal(t, bucket.tokens, tc.after, "test parameters: %v", tc)
	}
}

func TestTryConsume(t *testing.T) {
	tests := []struct {
		size   int
		before uint
		after  uint
		result bool
	}{
		{size: 10, before: 0, after: 0, result: false},
		{size: 10, before: 5, after: 4, result: true},
		{size: 10, before: 15, after: 14, result: true},
	}
	for _, tc := range tests {
		bucket := tokenBucket{
			policy: BucketPolicy{
				Capacity:   tc.size,
				FillPeriod: math.MaxInt64,
			},
			tokens: tc.before,
		}
		require.Equal(t, tc.result, bucket.tryConsume(), "test case: %v", tc)
		require.Equal(t, bucket.tokens, tc.after, "test case: %v", tc)
	}
}

func TestZeroPeriodDisablesBucket(t *testing.T) {
	bucket := tokenBucket{
		policy: BucketPolicy{
			Capacity:   1,
			FillPeriod: 0,
		},
		tokens: 1337,
	}
	bucket.fill(timeutil.Now())
	require.Equal(t, bucket.tokens, uint(1337))
	require.True(t, bucket.tryConsume())
	require.Equal(t, bucket.tokens, uint(1337))
}

func TestClockRollback(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   10,
		FillPeriod: time.Millisecond,
	}
	now := timeutil.Now()
	// Rollback by a time that is not divisible by FillPeriod.
	nowRolledBack := now.Add(-(time.Second + time.Nanosecond))

	bucket := newBucket(now, policy)
	bucket.tokens = 3

	require.Equal(t, bucket.filledAt, now)

	bucket.fill(nowRolledBack)

	require.Equal(t, bucket.filledAt, nowRolledBack)
	require.Equal(t, bucket.tokens, uint(3))
}

func TestNewBucket(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   12,
		FillPeriod: time.Hour,
	}
	now := timeutil.Now()
	bucket := newBucket(now, policy)
	require.Equal(t, bucket.filledAt, now)
	require.Equal(t, bucket.policy, policy)
	require.Equal(t, bucket.tokens, uint(12))
}
