// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedSettingsStoreAndLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testSettings []roachpb.KeyValue
	for i := 0; i < 5; i++ {
		testKey := fmt.Sprintf("key_%d", i)
		testVal := fmt.Sprintf("val_%d", i)
		testSettings = append(testSettings, roachpb.KeyValue{
			Key:   []byte(testKey),
			Value: roachpb.MakeValueFromString(testVal),
		})
	}

	ctx := context.Background()
	engine, err := storage.Open(ctx, storage.InMemory(),
		cluster.MakeClusterSettings(),
		storage.MaxSizeBytes(512<<20 /* 512 MiB */),
		storage.ForTesting)
	require.NoError(t, err)
	defer engine.Close()

	require.NoError(t, storeCachedSettingsKVs(ctx, engine, testSettings))

	actualSettings, err := loadCachedSettingsKVs(ctx, engine)
	require.NoError(t, err)
	require.Equal(t, testSettings, actualSettings)
}

func TestCachedSettingsServerRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stickyVFSRegistry := fs.NewStickyRegistry()

	serverArgs := base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		StoreSpecs: []base.StoreSpec{
			{InMemory: true, StickyVFSID: "1"},
		},
		Knobs: base.TestingKnobs{
			Server: &TestingKnobs{
				StickyVFSRegistry: stickyVFSRegistry,
			},
		},
	}
	var expectedSettingsCache []roachpb.KeyValue
	ts := serverutils.StartServerOnly(t, serverArgs)
	closedts.TargetDuration.Override(ctx, &ts.ClusterSettings().SV, 10*time.Millisecond)
	closedts.SideTransportCloseInterval.Override(ctx, &ts.ClusterSettings().SV, 10*time.Millisecond)
	kvserver.RangeFeedRefreshInterval.Override(ctx, &ts.ClusterSettings().SV, 10*time.Millisecond)
	const expectedSettingsCount = 3
	testutils.SucceedsSoon(t, func() error {
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(1)
		if err != nil {
			return err
		}
		settings, err := loadCachedSettingsKVs(context.Background(), store.TODOEngine())
		if err != nil {
			return err
		}

		// Previously, we checked if len(settings) > 0, which led to a race
		// condition where, in rare cases (under --race), the settings watcher
		// had not yet received some settings through rangefeed. If we exit this
		// function and assign expectedSettingsCount with those incomplete
		// settings, the settings watcher may receive the remaining settings
		// before we stop the server. See issue #124419 for more details.
		if len(settings) < expectedSettingsCount {
			return errors.Newf("unexpected count of settings: expected %d, found %d",
				expectedSettingsCount, len(settings))
		}
		expectedSettingsCache = settings
		return nil
	})
	ts.Stopper().Stop(context.Background())

	s, err := serverutils.NewServer(serverArgs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.Background())

	var initServer *initServer
	{
		getDialOpts := s.RPCContext().GRPCDialOptions

		cfg := s.SystemLayer().(*testServer).topLevelServer.cfg
		initConfig := newInitServerConfig(ctx, cfg, getDialOpts)
		inspectState, err := inspectEngines(
			context.Background(),
			s.Engines(),
			s.ClusterSettings().Version.LatestVersion(),
			s.ClusterSettings().Version.MinSupportedVersion(),
		)
		require.NoError(t, err)

		initServer = newInitServer(s.AmbientCtx(), inspectState, initConfig)
	}

	// ServeAndWait should return immediately since the server is already initialized
	// and thus we can verify if the initial state of the server stores the same settings
	// KVs as the ones loaded with loadCachedSettingsKVs, i.e., cached on the local store.
	testutils.SucceedsSoon(t, func() error {
		state, initialBoot, err := initServer.ServeAndWait(
			context.Background(),
			s.Stopper(),
			&s.ClusterSettings().SV,
		)
		if err != nil {
			return err
		}
		if initialBoot {
			return errors.New("server should not require initialization")
		}
		if !assert.ObjectsAreEqual(expectedSettingsCache, state.initialSettingsKVs) {
			return errors.Newf(`initial state settings KVs does not match expected settings
Expected: %+v
Actual:   %+v
`, expectedSettingsCache, state.initialSettingsKVs)
		}
		return nil
	})
}

func TestCachedSettingDeletionIsPersisted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	hasKey := func(kvs []roachpb.KeyValue, key string) bool {
		for _, kv := range kvs {
			if strings.Contains(string(kv.Key), key) {
				return true
			}
		}
		return false
	}

	ctx := context.Background()

	ts, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer ts.Stopper().Stop(ctx)
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Make the test faster.
	st := ts.ClusterSettings()
	factor := 1
	if util.RaceEnabled {
		// Under race, all the goroutines are generally slower. If we
		// accelerate the rangefeeds and the closed ts framework too much,
		// it can start overwhelming the scheduler and starve everything
		// of CPU time. So give everything some breathing time in that
		// case.
		//
		// TODO(knz): Replace this by the change in #111753.
		factor = 4
	}
	closedts.TargetDuration.Override(ctx, &st.SV, 10*time.Millisecond*time.Duration(factor))
	closedts.SideTransportCloseInterval.Override(ctx, &st.SV, 10*time.Millisecond*time.Duration(factor))
	kvserver.RangeFeedRefreshInterval.Override(ctx, &st.SV, 10*time.Millisecond*time.Duration(factor))

	// Customize a setting.
	db.Exec(t, `SET CLUSTER SETTING ui.display_timezone = 'America/New_York'`)
	// The setting won't propagate to the store until the setting watcher caches
	// up with the rangefeed, which might take a while.
	testutils.SucceedsSoon(t, func() error {
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(1)
		require.NoError(t, err)
		settings, err := loadCachedSettingsKVs(context.Background(), store.TODOEngine())
		require.NoError(t, err)
		if !hasKey(settings, `ui.display_timezone`) {
			return errors.New("cached setting not found")
		}
		return nil
	})

	// Reset the setting.
	db.Exec(t, `RESET CLUSTER SETTING ui.display_timezone`)
	// Check that the setting is eventually deleted from the store.
	testutils.SucceedsSoon(t, func() error {
		store, err := ts.GetStores().(*kvserver.Stores).GetStore(1)
		require.NoError(t, err)
		settings, err := loadCachedSettingsKVs(context.Background(), store.TODOEngine())
		require.NoError(t, err)
		if hasKey(settings, `ui.display_timezone`) {
			return errors.New("cached setting was still found")
		}
		return nil
	})
}
