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
	"testing"
	"time"

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
	s.localService.clock = s.clock.Now
	return s
}

func (s *localService) checkInvariants() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.mu.limiters) != len(s.mu.addrs) {
		return fmt.Errorf("len(limiters) [%d] != len(addrs) [%d]", len(s.mu.limiters), len(s.mu.addrs))
	}

	for i, addr := range s.mu.addrs {
		l := s.mu.limiters[addr]
		if l.index != i {
			return fmt.Errorf("limiters[addrs[%d]].index != %d (addr=%s index=%d)", i, i, addr, l.index)
		}
	}
	return nil
}

var expectedBackOff = []int{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 3600, 3600, 3600}

func TestLocalService_BackOff(t *testing.T) {
	s := newTestLocalService()

	const ipAddress = "127.0.0.1"

	verifyBackOff := func() {
		require.NoError(t, s.LoginCheck(ipAddress, s.clock.Now()))

		for _, delay := range expectedBackOff {
			require.EqualError(t, s.LoginCheck(ipAddress, s.clock.Now()), errRequestDenied.Error())

			s.clock.advance(time.Duration(delay)*time.Second - time.Nanosecond)
			require.EqualError(t, s.LoginCheck(ipAddress, s.clock.Now()), errRequestDenied.Error())

			s.clock.advance(time.Nanosecond)
			require.NoError(t, s.LoginCheck(ipAddress, s.clock.Now()))
		}
	}

	verifyBackOff()
}

func TestLocalService_WithBaseDelay(t *testing.T) {
	s := newTestLocalService(WithBaseDelay(time.Second))

	const ipAddress = "127.0.0.1"

	require.NoError(t, s.LoginCheck(ipAddress, s.clock.Now()))

	s.clock.advance(time.Second - time.Nanosecond)
	require.EqualError(t, s.LoginCheck(ipAddress, s.clock.Now()), errRequestDenied.Error())

	s.clock.advance(time.Nanosecond)
	require.NoError(t, s.LoginCheck(ipAddress, s.clock.Now()))
}

func TestLocalService_Eviction(t *testing.T) {
	s := newTestLocalService()
	s.maxMapSize = 10

	for i := 0; i < 20; i++ {
		ipAddress := fmt.Sprintf("%d", i)
		_ = s.LoginCheck(ipAddress, s.clock.Now())
		require.Less(t, len(s.mu.limiters), 11)
		require.NoError(t, s.checkInvariants())
	}
}
