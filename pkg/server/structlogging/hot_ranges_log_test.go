// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging_test

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// setup an impossibly low cpu threshold, test clusters
// do not seem to record cpu utilization per replica.
const lowCPUThreshold = time.Duration(-1)
const highCPUThreshold = time.Second
const lowDelay = 50 * time.Millisecond
const highDelay = time.Minute

// TestHotRangeLogger tests that hot ranges stats are logged per node.
// It uses system ranges to verify behavior.
func TestHotRangeLoggerSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t)
	ctx := context.Background()

	settings, spy, teardown := setupTestServer(t, ctx)
	defer teardown()

	for _, test := range []struct {
		enabled            bool
		tickerInterval     time.Duration
		logSettingInterval time.Duration
		logCPUThreshold    time.Duration
		hasLogs            bool
	}{
		// Tests the straightforward use case, where we expect no threshold,
		// a minimal interval, minimal loop, and zero threshold should
		// result in multiple logs.
		{true, lowDelay, lowDelay, lowCPUThreshold, true},

		// This test is the same as the default case, except the
		// cluster setting which controls logging is off.
		{false, lowDelay, lowDelay, lowCPUThreshold, false},

		// This test validates that even when we check on a low cadance,
		// if the threshold is not passed and the interval is long,
		// no logs will appear.
		{true, lowDelay, highDelay, highCPUThreshold, false},

		// This test validates that even if the interval is long,
		// if the cpu threshold is low, and its checked, the system
		// will produce logs.
		{true, lowDelay, highDelay, lowCPUThreshold, true},

		// This test validates with a high check cadance, no logs
		// will appear, even if the interval and thresholds are low.
		{true, highDelay, lowDelay, lowCPUThreshold, false},

		// This test checks that if there's a low logging interval
		// if the cpuThreshold is high, logs will still appear.
		{true, lowDelay, lowDelay, highCPUThreshold, true},
	} {
		t.Run(fmt.Sprintf("settings tests %v", test), func(t *testing.T) {
			setupTest(ctx, settings, test.enabled, test.logSettingInterval, test.tickerInterval, test.logCPUThreshold, spy)
			testutils.SucceedsSoon(t, func() error {
				actual := hasNonZeroQPSRange(spy.Logs())
				if test.hasLogs != actual {
					return errors.Errorf("expected hasLogs %v, got %v", test.hasLogs, actual)
				}
				return nil
			})
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
		setupTest(ctx, settings, true, lowDelay, lowDelay, lowCPUThreshold, spy)
		testutils.SucceedsSoon(t, func() error {
			if actual := countSeenRanges(spy.Logs()); actual <= 1 {
				return fmt.Errorf("expected >1 range, got %d", actual)
			}
			return nil
		})

		// with a limit, only one range should show up.
		structlogging.ReportTopHottestRanges = 1
		setupTest(ctx, settings, true, lowDelay, lowDelay, lowCPUThreshold, spy)
		testutils.SucceedsSoon(t, func() error {
			if actual := countSeenRanges(spy.Logs()); actual != 1 {
				return fmt.Errorf("expected 1 range, got %d", actual)
			}
			return nil
		})
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
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t)
	ctx := context.Background()
	_, spy, teardown := setupTestServer(t, ctx)
	spy.Logs()
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

type testHotRangeGetter struct{}

func (t testHotRangeGetter) HotRangesV2(
	ctx context.Context, req *serverpb.HotRangesRequest,
) (*serverpb.HotRangesResponseV2, error) {
	if req.PerNodeLimit == 1 {
		return &serverpb.HotRangesResponseV2{
			Ranges: []*serverpb.HotRangesResponseV2_HotRange{
				{
					RangeID:          1,
					CPUTimePerSecond: float64(100 * time.Millisecond),
					QPS:              float64(100),
				},
			},
		}, nil
	}
	return &serverpb.HotRangesResponseV2{
		Ranges: []*serverpb.HotRangesResponseV2_HotRange{
			{
				RangeID:          1,
				CPUTimePerSecond: float64(100 * time.Millisecond),
				QPS:              float64(100),
			},
			{
				RangeID:          2,
				CPUTimePerSecond: float64(1300 * time.Millisecond),
				QPS:              float64(100),
			},
			{
				RangeID:          3,
				CPUTimePerSecond: float64(900 * time.Millisecond),
				QPS:              float64(100),
			},
		},
	}, nil
}

var _ structlogging.HotRangeGetter = testHotRangeGetter{}

// setupTestServer is a somewhat lengthy warmup process
// to ensure that the hot ranges tests are ready to run.
// It sets up a cluster, runs it until the hot range stats are
// warm by dialing the knobs to noisy, and checking for output,
// then redials the knobs back to quiet so the test can take over.
func setupTestServer(
	t *testing.T, ctx context.Context,
) (*cluster.Settings, *hotRangesLogSpy, func()) {
	sc := log.ScopeWithoutShowLogs(t)
	spy := &hotRangesLogSpy{t: t}

	// override internal settings.
	structlogging.ReportTopHottestRanges = 1000
	structlogging.CheckInterval = 100 * time.Millisecond

	logInterceptor := log.InterceptWith(ctx, spy)
	stopper := stop.NewStopper()
	settings := cluster.MakeTestingClusterSettings()
	teardown := func() {
		stopper.Stop(ctx)
		sc.Close(t)
		logInterceptor()
	}

	// lower settings so that we can wait for the stats to warm.
	structlogging.TelemetryHotRangesStatsEnabled.Override(ctx, &settings.SV, true)
	structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &settings.SV, time.Millisecond)
	structlogging.TelemetryHotRangesStatsLoggingDelay.Override(ctx, &settings.SV, 0*time.Millisecond)

	err := structlogging.StartSystemHotRangesLogger(ctx, stopper, testHotRangeGetter{}, settings)
	require.NoError(t, err)

	return settings, spy, teardown
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
	structlogging.TestLoopChannel <- struct{}{}
	log.FlushAllSync()
	spy.Reset()
}
