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

func (s *localService) checkInvariants() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.mu.limiters) != len(s.mu.addrs) {
		return fmt.Errorf("len(limiters) [%d] != len(addrs) [%d]", len(s.mu.limiters), len(s.mu.addrs))
	}
	return nil
}

func TestReportSuccessReturnsToken(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   1,
		FillPeriod: math.MaxInt32,
	}
	s := newTestLocalService(WithAuthenticatedPolicy(policy), WithUnauthenticatedPolicy(policy))

	addr := "0.0.0.0"
	require.NoError(t, s.LoginCheck(addr))
	require.Equal(t, s.LoginCheck(addr), errRequestDenied)
	s.ReportSuccess(addr)
	require.NoError(t, s.LoginCheck(addr))
	require.Equal(t, s.LoginCheck(addr), errRequestDenied)
}

func TestPoolRefills(t *testing.T) {
	policy := BucketPolicy{
		Capacity:   20,
		FillPeriod: time.Second,
	}
	s := newTestLocalService(WithUnauthenticatedPolicy(policy))

	addr := "0.0.0.0"

	countSuccess := func() int {
		result := 0
		for {
			if err := s.LoginCheck(addr); err != nil {
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

func TestReportSuccessUpgradesPolicy(t *testing.T) {
	unauthenticatedPolicy := BucketPolicy{Capacity: 1, FillPeriod: time.Minute}
	authenticatedPolicy := BucketPolicy{Capacity: 10, FillPeriod: time.Second}
	s := newTestLocalService(
		WithAuthenticatedPolicy(authenticatedPolicy),
		WithUnauthenticatedPolicy(unauthenticatedPolicy),
	)

	addr := "0.0.0.0"
	require.Equal(t, s.getBucketLocked(addr).policy, unauthenticatedPolicy)
	s.ReportSuccess(addr)
	require.Equal(t, s.getBucketLocked(addr).policy, authenticatedPolicy)
}

func TestLocalService_Eviction(t *testing.T) {
	s := newTestLocalService()
	s.maxMapSize = 10

	for i := 0; i < 20; i++ {
		ipAddress := fmt.Sprintf("%d", i)
		_ = s.LoginCheck(ipAddress)
		require.Less(t, len(s.mu.limiters), 11)
		require.NoError(t, s.checkInvariants())
	}
}
