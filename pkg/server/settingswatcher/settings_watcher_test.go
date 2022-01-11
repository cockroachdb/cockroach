// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settingswatcher

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test that an error occurring during processing of the
// rangefeedcache.Watcher can be recovered after a permanent
// rangefeed failure.
func TestOverflowRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sideSettings := cluster.MakeTestingClusterSettings()
	w := New(
		s.Clock(),
		s.ExecutorConfig().(sql.ExecutorConfig).Codec,
		sideSettings,
		s.RangeFeedFactory().(*rangefeed.Factory),
		s.Stopper(),
		nil,
	)
	var exitCalled int64 // accessed with atomics
	errCh := make(chan error)
	w.testingWatcherKnobs = &rangefeedcache.TestingKnobs{
		PreExit:          func() { atomic.AddInt64(&exitCalled, 1) },
		ErrorInjectionCh: errCh,
	}
	require.NoError(t, w.Start(ctx))
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	// Shorten the closed timestamp duration as a cheeky way to check the
	// checkpointing code while also speeding up the test.
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")

	checkSettings := func() {
		testutils.SucceedsSoon(t, func() error {
			return CheckSettingsValuesMatch(t, s.ClusterSettings(), sideSettings)
		})
	}
	checkExits := func(exp int64) {
		require.Equal(t, exp, atomic.LoadInt64(&exitCalled))
	}
	waitForExits := func(exp int64) {
		require.Eventually(t, func() bool {
			return atomic.LoadInt64(&exitCalled) == exp
		}, time.Minute, time.Millisecond)
	}

	checkSettings()
	tdb.Exec(t, "SET CLUSTER SETTING kv.queue.process.guaranteed_time_budget = '1m'")
	checkSettings()
	checkExits(0)
	errCh <- errors.New("boom")
	waitForExits(1)
	tdb.Exec(t, "SET CLUSTER SETTING kv.queue.process.guaranteed_time_budget = '2s'")
	checkSettings()
	checkExits(1)
}

// CheckSettingsValuesMatch is a test helper function to return an error when
// two settings do not match. It generally gets used with SucceeedsSoon.
func CheckSettingsValuesMatch(t *testing.T, a, b *cluster.Settings) error {
	for _, k := range settings.Keys(false /* forSystemTenant */) {
		s, ok := settings.Lookup(k, settings.LookupForLocalAccess, false /* forSystemTenant */)
		require.True(t, ok)
		if s.Class() == settings.SystemOnly {
			continue
		}
		if av, bv := s.String(&a.SV), s.String(&b.SV); av != bv {
			return errors.Errorf("values do not match for %s: %s != %s", k, av, bv)
		}
	}
	return nil
}
