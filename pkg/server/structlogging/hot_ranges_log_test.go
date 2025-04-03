// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type hotRangesLogSpy struct {
	t  *testing.T
	mu struct {
		syncutil.RWMutex
		logs []eventpb.HotRangesStats
	}
}

func (spy *hotRangesLogSpy) Intercept(e []byte) {
	var entry logpb.Entry
	if err := json.Unmarshal(e, &entry); err != nil {
		spy.t.Fatal(err)
	}

	re := regexp.MustCompile(`"EventType":"hot_ranges_stats"`)
	if entry.Channel != logpb.Channel_TELEMETRY || !re.MatchString(entry.Message) {
		return
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	var rangesLog eventpb.HotRangesStats
	if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &rangesLog); err != nil {
		spy.t.Fatal(err)
	}

	spy.mu.logs = append(spy.mu.logs, rangesLog)
}

func (spy *hotRangesLogSpy) Logs() []eventpb.HotRangesStats {
	spy.mu.RLock()
	defer spy.mu.RUnlock()
	logs := make([]eventpb.HotRangesStats, len(spy.mu.logs))
	copy(logs, spy.mu.logs)
	return logs
}

func (spy *hotRangesLogSpy) Reset() {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	spy.mu.logs = nil
}

func setupHotRangesLogTest(
	t *testing.T, ctx context.Context,
) (serverutils.ApplicationLayerInterface, *hotRangesLogSpy, func()) {
	sc := log.ScopeWithoutShowLogs(t)
	spy := &hotRangesLogSpy{t: t}
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					ReplicaPlannerKnobs: plan.ReplicaPlannerTestingKnobs{
						DisableReplicaRebalancing: true,
					},
				},
			},
		},
	})

	leakChecker := leaktest.AfterTest(t)
	logInterceptor := log.InterceptWith(ctx, spy)
	stopper := tc.Stopper()
	teardown := func() {
		stopper.Stop(ctx)
		sc.Close(t)
		logInterceptor()
		leakChecker()
	}

	ts := tc.ApplicationLayer(0)
	return ts, spy, teardown
}

// TestHotRangeLogger tests that hot ranges stats are logged per node.
// The test will ensure each node contains 5 distinct range replicas for hot
// ranges logging. Each node should thus log 5 distinct range ids.
func TestHotRangeLogger(t *testing.T) {
	skip.UnderRace(t)
	ctx := context.Background()
	ts, spy, teardown := setupHotRangesLogTest(t, ctx)
	defer teardown()

	structlogging.TelemetryHotRangesStatsEnabled.Override(ctx, &ts.ClusterSettings().SV, true)
	structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &ts.ClusterSettings().SV, time.Millisecond)
	structlogging.TelemetryHotRangesStatsLoggingDelay.Override(ctx, &ts.ClusterSettings().SV, 0*time.Millisecond)

	testutils.SucceedsSoon(t, func() error {
		logs := spy.Logs()

		// Look for a descriptor, which we always expect to exist in the system.
		for _, l := range logs {
			if !slices.Equal(l.Databases, []string{"‹system›"}) {
				continue
			}
			if !slices.Equal(l.Tables, []string{"‹sqlliveness›"}) {
				continue
			}
			if !slices.Equal(l.Indexes, []string{"‹primary›"}) {
				continue
			}
			return nil
		}
		return errors.New("waited too long for the synthetic data")
	})
}
