// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

// setup an impossibly low cpu threshold, test clusters
// do not seem to record cpu utilization per replica.
const lowCPUThreshold = time.Duration(-1)
const highCPUThreshold = time.Second
const defaultTestWait = time.Second
const lowDelay = 50 * time.Millisecond
const highDelay = time.Minute

// TestHotRangeLogger tests that hot ranges stats are logged per node.
// It uses system ranges to verify behavior.
func TestHotRangeLoggerSettings(t *testing.T) {
	skip.UnderRace(t)
	skip.UnderStress(t)
	ctx := context.Background()

	// We only want to run this once within the suite, as the
	// subsystem we depend on takes on the order of whole seconds
	// to warm.
	s, spy, teardown := setupTestServer(t, ctx)
	defer teardown()

	for _, test := range []struct {
		enabled            bool
		tickerInterval     time.Duration
		logSettingInterval time.Duration
		waitFor            time.Duration
		logCPUThreshold    time.Duration
		hasLogs            bool
	}{
		// Tests the straightforward use case, where we expect no threshold,
		// a minimal interval, minimal loop, and zero threshold should
		// result in multiple logs.
		{true, lowDelay, lowDelay, defaultTestWait, lowCPUThreshold, true},

		// This test is the same as the default case, except the
		// cluster setting which controls logging is off.
		{false, lowDelay, lowDelay, defaultTestWait, lowCPUThreshold, false},

		// This test validates that even when we check on a low cadance,
		// if the threshold is not passed and the interval is long,
		// no logs will appear.
		{true, lowDelay, highDelay, defaultTestWait, highCPUThreshold, false},

		// This test validates that even if the interval is long,
		// if the cpu threshold is low, and its checked, the system
		// will produce logs.
		{true, lowDelay, highDelay, defaultTestWait, lowCPUThreshold, true},

		// This test validates with a high check cadance, no logs
		// will appear, even if the interval and thresholds are low.
		{true, highDelay, lowDelay, defaultTestWait, lowCPUThreshold, false},

		// This test checks that if there's a low logging interval
		// if the cpuThreshold is high, logs will still appear.
		{true, lowDelay, lowDelay, defaultTestWait, highCPUThreshold, true},
	} {
		t.Run(fmt.Sprintf("settings tests %v", test), func(t *testing.T) {
			setupTest(ctx, s.ClusterSettings(), test.enabled, test.logSettingInterval, test.tickerInterval, test.logCPUThreshold, spy)
			time.Sleep(test.waitFor)
			require.Equal(t, test.hasLogs, hasNonZeroQPSRange(spy.Logs()))
		})
	}

	t.Run("the per node restriction limits the number of unique logs", func(t *testing.T) {
		countSeenRanges := func(logs []testLog) int {
			counts := make(map[int64]int)
			for _, l := range logs {
				counts[l.Stats.RangeID]++
			}
			return len(counts)
		}

		// without a limit set, we should see many ranges.
		setupTest(ctx, s.ClusterSettings(), true, lowDelay, lowDelay, lowCPUThreshold, spy)
		time.Sleep(time.Second)
		require.Greater(t, countSeenRanges(spy.Logs()), 1)

		// with a limit, only one range should show up.
		structlogging.ReportTopHottestRanges = 1
		setupTest(ctx, s.ClusterSettings(), true, lowDelay, lowDelay, lowCPUThreshold, spy)
		time.Sleep(time.Second)
		require.Equal(t, countSeenRanges(spy.Logs()), 1)
	})
}

// For multi-tenancy, we want to test the differing behavior.
// Specifically, we want to test the following:
//   - Logs are written separately for system and app tenants.
//   - For app tenants, the interval at which logs are collected
//     is longer.
//   - For app tenants, a job is initialized for the hot ranges
//     logger, whereas for the system tenant it runs as a task.
func TestHotRangeLoggerMultitenant(t *testing.T) {
	skip.UnderRace(t)
	ctx := context.Background()
	s, spy, teardown := setupTestServer(t, ctx)
	tenantID := roachpb.MustMakeTenantID(2)
	tt, err := s.TenantController().StartTenant(ctx, base.TestTenantArgs{
		TenantID: tenantID,
	})
	spy.Logs()
	require.NoError(t, err)
	require.NotNil(t, tt)
	// TODO (brian): the jobs system isn't registering this correctly,
	// this will be fixed in a short follow pr.
	defer teardown()
}

type testLog struct {
	TenantID string
	Stats    eventpb.HotRangesStats
}

type hotRangesLogSpy struct {
	t  *testing.T
	mu struct {
		syncutil.RWMutex
		logs []testLog
	}
}

func (spy *hotRangesLogSpy) Intercept(e []byte) {
	var entry logpb.Entry
	if err := json.Unmarshal(e, &entry); err != nil {
		spy.t.Fatal(err)
	}

	re := regexp.MustCompile(`"EventType":"hot_ranges_stats"`)
	if entry.Channel != logpb.Channel_HEALTH || !re.MatchString(entry.Message) {
		return
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	var rangesLog eventpb.HotRangesStats
	if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &rangesLog); err != nil {
		spy.t.Fatal(err)
	}

	spy.mu.logs = append(spy.mu.logs, testLog{entry.TenantID, rangesLog})
}

func (spy *hotRangesLogSpy) Logs() []testLog {
	spy.mu.RLock()
	defer spy.mu.RUnlock()
	logs := make([]testLog, len(spy.mu.logs))
	copy(logs, spy.mu.logs)
	return logs
}

func (spy *hotRangesLogSpy) Reset() {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	spy.mu.logs = nil
}

// setupTestServer is a somewhat lengthy warmup process
// to ensure that the hot ranges tests are ready to run.
// It sets up a cluster, runs it until the hot range stats are
// warm by dialing the knobs to noisy, and checking for output,
// then redials the knobs back to quiet so the test can take over.
func setupTestServer(
	t *testing.T, ctx context.Context,
) (serverutils.TestServerInterface, *hotRangesLogSpy, func()) {
	sc := log.ScopeWithoutShowLogs(t)
	spy := &hotRangesLogSpy{t: t}

	// override internal settings.
	structlogging.ReportTopHottestRanges = 1000
	structlogging.CheckInterval = 100 * time.Millisecond

	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				ReplicaPlannerKnobs: plan.ReplicaPlannerTestingKnobs{
					DisableReplicaRebalancing: true,
				},
			},
		},
	})

	leakChecker := leaktest.AfterTest(t)
	logInterceptor := log.InterceptWith(ctx, spy)
	stopper := s.Stopper()
	teardown := func() {
		stopper.Stop(ctx)
		sc.Close(t)
		logInterceptor()
		leakChecker()
	}

	ts := s.ApplicationLayer()

	// lower settings so that we can wait for the stats to warm.
	structlogging.TelemetryHotRangesStatsEnabled.Override(ctx, &ts.ClusterSettings().SV, true)
	structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &ts.ClusterSettings().SV, time.Millisecond)
	structlogging.TelemetryHotRangesStatsLoggingDelay.Override(ctx, &ts.ClusterSettings().SV, 0*time.Millisecond)

	// simulate some queries.
	for range 1000 {
		_, err := ts.SQLConn(t).Exec("SELECT * FROM system.namespace")
		require.NoError(t, err)
	}

	testutils.SucceedsSoon(t, func() error {
		logs := spy.Logs()

		if hasNonZeroQPSRange(logs) {
			return nil
		}
		return errors.New("waited too long for the synthetic data")
	})

	return s, spy, teardown
}

// Utility function which generally indicates that the hot ranges
// are being logged.
func hasNonZeroQPSRange(logs []testLog) bool {
	for _, l := range logs {
		if l.Stats.Qps == 0 {
			continue
		}
		return true
	}
	return false
}

func setupTest(
	ctx context.Context,
	st *cluster.Settings,
	enabled bool,
	logInterval, tickerInterval time.Duration,
	logCPUThreshold time.Duration,
	spy *hotRangesLogSpy,
) {
	structlogging.TelemetryHotRangesStatsEnabled.Override(ctx, &st.SV, enabled)
	structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &st.SV, logInterval)
	structlogging.TelemetryHotRangesStatsCPUThreshold.Override(ctx, &st.SV, logCPUThreshold)
	structlogging.CheckInterval = tickerInterval
	// wait for the activity from the previous test to drain.
	time.Sleep(100 * time.Millisecond)
	structlogging.TestLoopChannel <- struct{}{}
	spy.Reset()
}
