// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package throttler

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	next time.Time
}

func (f *fakeClock) Now() time.Time {
	return f.next
}

func (f *fakeClock) advance(d time.Duration) {
	f.next = f.next.Add(d)
}

type testLocalService struct {
	*localService
	clock fakeClock
}

func newTestLocalService(opts ...LocalOption) *testLocalService {
	s := &testLocalService{
		localService: NewLocalService(opts...).(*localService),
	}
	s.clock.next = timeutil.Now()
	s.localService.clock = s.clock.Now
	return s
}

func TestReportSuccessReturnsToken(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   1,
		FillPeriod: math.MaxInt64,
	}
	s := newTestLocalService(WithPolicy(policy))

	t1 := ConnectionTags{IP: "0.0.0.0", TenantID: "1"}
	t2 := ConnectionTags{IP: "0.0.0.0", TenantID: "2"}

	require.NoError(t, s.LoginCheck(t1))
	require.Equal(t, s.LoginCheck(t2), errRequestDenied)
	require.Equal(t, s.LoginCheck(t1), errRequestDenied)

	s.ReportSuccess(t1)

	require.NoError(t, s.LoginCheck(t1))
	require.NoError(t, s.LoginCheck(t2))
	require.Equal(t, s.LoginCheck(t2), errRequestDenied)

	s.ReportSuccess(t2)

	require.NoError(t, s.LoginCheck(t1))
	require.NoError(t, s.LoginCheck(t2))
}

func TestIPHasUniqueBucket(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   1,
		FillPeriod: math.MaxInt64,
	}
	s := newTestLocalService(WithPolicy(policy))

	t1 := ConnectionTags{IP: "0.0.0.0", TenantID: "1"}
	t2 := ConnectionTags{IP: "0.0.0.1", TenantID: "1"}

	require.NoError(t, s.LoginCheck(t1))
	require.Equal(t, s.LoginCheck(t1), errRequestDenied)

	require.NoError(t, s.LoginCheck(t2))
	require.Equal(t, s.LoginCheck(t2), errRequestDenied)
}

func TestPoolRefills(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   20,
		FillPeriod: time.Second,
	}
	s := newTestLocalService(WithPolicy(policy))

	tags := ConnectionTags{IP: "0.0.0.0", TenantID: "1"}

	countSuccess := func() int {
		result := 0
		for {
			if err := s.LoginCheck(tags); err != nil {
				return result
			}
			result++
		}
	}

	// The bucket is initialized to its max size.
	require.Equal(t, int(policy.Capacity), countSuccess())

	s.clock.advance(10 * time.Second)
	require.Equal(t, 10, countSuccess())

	s.clock.advance(time.Minute)
	require.Equal(t, int(policy.Capacity), countSuccess())
}

func TestReportSuccessDisablesThrottle(t *testing.T) {
	emptyBucket := BucketPolicy{Capacity: 0, FillPeriod: time.Minute}
	s := newTestLocalService(WithPolicy(emptyBucket))

	tags := ConnectionTags{IP: "0.0.0.0", TenantID: "1"}
	require.Error(t, s.LoginCheck(tags))
	s.ReportSuccess(tags)
	for i := 0; i < 1000; i++ {
		require.NoError(t, s.LoginCheck(tags))
	}
}

func TestLocalService_Eviction(t *testing.T) {
	s := newTestLocalService()
	s.maxCacheSize = 10

	for i := 0; i < 20; i++ {
		tags := ConnectionTags{IP: fmt.Sprintf("%d", i)}
		_ = s.LoginCheck(tags)
		s.ReportSuccess(tags)
		require.Less(t, s.mu.ipCache.Len(), 11)
		require.Less(t, s.mu.successCache.Len(), 11)
	}
	require.Equal(t, s.mu.ipCache.Len(), 10)
	require.Equal(t, s.mu.successCache.Len(), 10)
}
