// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package throttler

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/testutilsccl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

var _ Service = (*testLocalService)(nil)

func newTestLocalService(opts ...LocalOption) *testLocalService {
	s := &testLocalService{
		localService: NewLocalService(opts...).(*localService),
	}
	s.clock.next = timeutil.Now()
	s.localService.clock = s.clock.Now
	return s
}

func countGuesses(
	t *testing.T,
	connection ConnectionTags,
	throttle *testLocalService,
	step time.Duration,
	period time.Duration,
) int {
	ctx := context.Background()
	count := 0
	for i := 0; step*time.Duration(i) < period; i++ {
		throttle.clock.advance(step)

		throttleTime, err := throttle.LoginCheck(ctx, connection)
		if err != nil {
			continue
		}

		err = throttle.ReportAttempt(ctx, connection, throttleTime, AttemptInvalidCredentials)
		require.NoError(t, err, "ReportAttempt should only return errors in the case of racing requests")

		count++
	}
	return count
}

func TestThrottleLimitsCredentialGuesses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	throttle := newTestLocalService(WithBaseDelay(time.Second))
	ip1Tenant1 := ConnectionTags{IP: "1.1.1.1", TenantID: "1"}
	ip1Tenant2 := ConnectionTags{IP: "1.1.1.1", TenantID: "2"}
	ip2Tenant1 := ConnectionTags{IP: "1.1.1.2", TenantID: "1"}

	require.Equal(t,
		35,
		countGuesses(t, ip1Tenant1, throttle, time.Second, time.Hour*24),
	)

	// Verify throttling logic is tenant specific.
	require.Equal(t,
		12,
		countGuesses(t, ip1Tenant2, throttle, time.Second, time.Hour),
	)
	require.Equal(t,
		12,
		countGuesses(t, ip2Tenant1, throttle, time.Second, time.Hour),
	)
}

func TestReportSuccessDisablesLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	throttle := newTestLocalService()
	tenant1 := ConnectionTags{IP: "1.1.1.1", TenantID: "1"}
	tenant2 := ConnectionTags{IP: "1.1.1.1", TenantID: "2"}

	throttleTime, err := throttle.LoginCheck(ctx, tenant1)
	require.NoError(t, err)
	require.NoError(t, throttle.ReportAttempt(ctx, tenant1, throttleTime, AttemptOK))

	require.Equal(t,
		int(time.Hour/time.Second),
		countGuesses(t, tenant1, throttle, time.Second, time.Hour),
	)

	// Verify the unlimited throttle only applies to the tenant with the
	// successful connection.
	require.Equal(t,
		12,
		countGuesses(t, tenant2, throttle, time.Second, time.Hour),
	)
}

func TestRacingRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutilsccl.ServerlessOnly(t)

	ctx := context.Background()
	throttle := newTestLocalService()
	connection := ConnectionTags{IP: "1.1.1.1", TenantID: "1"}

	throttleTime, err := throttle.LoginCheck(ctx, connection)
	require.NoError(t, err)

	require.NoError(t, throttle.ReportAttempt(ctx, connection, throttleTime, AttemptInvalidCredentials))

	l := throttle.lockedGetThrottle(connection)
	nextTime := l.nextTime

	for _, status := range []AttemptStatus{AttemptOK, AttemptInvalidCredentials} {
		err := throttle.ReportAttempt(ctx, connection, throttleTime, status)
		require.Error(t, err)
		require.Regexp(t, "throttler refused connection", err.Error())

		// Verify the throttled report has no affect on limiter state.
		l := throttle.lockedGetThrottle(connection)
		require.NotNil(t, l)
		require.NotEqual(t, l.nextBackoff, throttleDisabled)
		require.Equal(t, l.nextTime, nextTime)
	}
}
