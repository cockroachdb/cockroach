// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settingswatcher_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSettingsWatcherOnTenant constructs a SettingsWatcher under a hypothetical
// tenant and then copies some values over to that tenant. It then ensures that
// the initial settings are picked up and that changes are also eventually
// picked up.
func TestSettingWatcherOnTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	const systemOnlySetting = "kv.snapshot_rebalance.max_rate"
	toSet := map[string][]interface{}{
		"kv.queue.process.guaranteed_time_budget": {"17s", "20s"},
		"sql.txn_stats.sample_rate":               {.23, .55},
		"cluster.organization":                    {"foobar", "bazbax"},
		// Include a system-only setting to verify that we don't try to change its
		// value (which would cause a panic in test builds).
		systemOnlySetting: {2 << 20, 4 << 20},
	}
	fakeTenant := roachpb.MakeTenantID(2)
	systemTable := keys.SystemSQLCodec.TablePrefix(keys.SettingsTableID)
	fakeCodec := keys.MakeSQLCodec(fakeTenant)
	fakeTenantPrefix := keys.MakeTenantPrefix(fakeTenant)

	db := tc.Server(0).DB()
	getSourceClusterRows := func() []kv.KeyValue {
		rows, err := db.Scan(ctx, systemTable, systemTable.PrefixEnd(), 0 /* maxRows */)
		require.NoError(t, err)
		return rows
	}
	filterSystemOnly := func(rows []kv.KeyValue) (filtered []kv.KeyValue) {
		for _, row := range rows {
			if !bytes.Contains(row.Key, []byte(systemOnlySetting)) {
				filtered = append(filtered, row)
			}
		}
		return filtered
	}
	copySettingsFromSystemToFakeTenant := func() int {
		_, err := db.DelRange(
			ctx,
			fakeTenantPrefix,
			fakeTenantPrefix.PrefixEnd(),
			false,
		)
		require.NoError(t, err)
		rows := getSourceClusterRows()
		for _, row := range rows {
			rem, _, err := keys.DecodeTenantPrefix(row.Key)
			require.NoError(t, err)
			tenantKey := append(fakeTenantPrefix, rem...)
			row.Value.ClearChecksum()
			row.Value.Timestamp = hlc.Timestamp{}
			require.NoError(t, db.Put(ctx, tenantKey, row.Value))
		}
		return len(rows)
	}
	checkSettingsValuesMatch := func(a, b *cluster.Settings) error {
		return CheckSettingsValuesMatch(t, a, b)
	}
	checkStoredValuesMatch := func(expected []roachpb.KeyValue) error {
		got := filterSystemOnly(getSourceClusterRows())
		if len(got) != len(expected) {
			return errors.Errorf("expected %d rows, got %d", len(expected), len(got))
		}
		for i, kv := range got {
			rem, _, err := keys.DecodeTenantPrefix(kv.Key)
			require.NoError(t, err)
			tenantKey := append(fakeTenantPrefix, rem...)
			if !tenantKey.Equal(expected[i].Key) {
				return errors.Errorf("mismatched key %d: %v expected, got %d", i, expected[i].Key, tenantKey)
			}
			// Look past the checksum because it uses the key too.
			const checksumLen = 4
			if !bytes.Equal(
				kv.Value.RawBytes[checksumLen:],
				expected[i].Value.RawBytes[checksumLen:],
			) {
				return errors.Errorf("mismatched value %d: %q expected, got %q",
					i, kv.Value.RawBytes, expected[i].Value.RawBytes)
			}
		}
		return nil
	}
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[0])
	}
	copySettingsFromSystemToFakeTenant()
	s0 := tc.Server(0)
	tenantSettings := cluster.MakeTestingClusterSettings()
	tenantSettings.SV.SetNonSystemTenant()
	storage := &fakeStorage{}
	sw := settingswatcher.New(s0.Clock(), fakeCodec, tenantSettings,
		s0.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		s0.Stopper(), storage)
	require.NoError(t, sw.Start(ctx))
	require.NoError(t, checkSettingsValuesMatch(s0.ClusterSettings(), tenantSettings))
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[1])
	}
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkSettingsValuesMatch(s0.ClusterSettings(), tenantSettings)
	})
	// Shorten the closed timestamp duration as a cheeky way to check the
	// checkpointing code while also speeding up the test.
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10 ms'")
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkStoredValuesMatch(storage.getKVs())
	})

	// Unset and set.
	for k := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = DEFAULT")
	}
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkStoredValuesMatch(storage.getKVs())
	})

	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[1])
	}
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkStoredValuesMatch(storage.getKVs())
	})

	// Make sure we're not spinning writing updates.
	before := storage.getNumWrites()
	<-time.After(20 * time.Millisecond) // two of the resolve intervals
	require.Equal(t, before, storage.getNumWrites())
}

type fakeStorage struct {
	syncutil.Mutex
	kvs       []roachpb.KeyValue
	numWrites int
}

func (f *fakeStorage) SnapshotKVs(ctx context.Context, kvs []roachpb.KeyValue) {
	f.Lock()
	defer f.Unlock()
	f.kvs = kvs
	f.numWrites++
}

func (f *fakeStorage) getKVs() []roachpb.KeyValue {
	f.Lock()
	defer f.Unlock()
	return f.kvs
}

func (f *fakeStorage) getNumWrites() int {
	f.Lock()
	defer f.Unlock()
	return f.numWrites
}

var _ = settings.RegisterStringSetting(settings.TenantWritable, "str.foo", "desc", "")
var _ = settings.RegisterStringSetting(settings.TenantWritable, "str.bar", "desc", "bar")
var _ = settings.RegisterIntSetting(settings.TenantWritable, "i0", "desc", 0)
var _ = settings.RegisterIntSetting(settings.TenantWritable, "i1", "desc", 1)

func TestSettingsWatcherWithOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	// Set up a test cluster for the system table.
	ts, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	stopper := ts.Stopper()
	defer stopper.Stop(ctx)

	r := sqlutils.MakeSQLRunner(db)
	// Set some settings (to verify handling of existing rows).
	r.Exec(t, "SET CLUSTER SETTING str.foo = 'foo'")
	r.Exec(t, "SET CLUSTER SETTING i1 = 10")

	m := newTestingOverrideMonitor()
	// Set an override (to verify that it does work when it is already set).
	m.set("str.foo", "override", "s")

	st := cluster.MakeTestingClusterSettings()
	f, err := rangefeed.NewFactory(stopper, kvDB, st, &rangefeed.TestingKnobs{})
	require.NoError(t, err)
	w := settingswatcher.NewWithOverrides(ts.Clock(), keys.SystemSQLCodec, st, f, stopper, m, nil)
	require.NoError(t, w.Start(ctx))

	expect := func(setting, value string) {
		t.Helper()
		s, ok := settings.Lookup(setting, settings.LookupForLocalAccess, settings.ForSystemTenant)
		require.True(t, ok)
		require.Equal(t, value, s.String(&st.SV))
	}

	expectSoon := func(setting, value string) {
		t.Helper()
		s, ok := settings.Lookup(setting, settings.LookupForLocalAccess, settings.ForSystemTenant)
		require.True(t, ok)
		testutils.SucceedsSoon(t, func() error {
			if actual := s.String(&st.SV); actual != value {
				return errors.Errorf("expected '%s', got '%s'", value, actual)
			}
			return nil
		})
	}

	expect("str.foo", "override")
	expect("str.bar", "bar")
	expect("i0", "0")
	expect("i1", "10")

	m.unset("str.foo")
	m.set("str.bar", "override", "s")
	m.notify()

	expectSoon("str.bar", "override")
	// str.foo should now be the value we set above.
	expectSoon("str.foo", "foo")

	// Verify that a new setting in the table does not affect the override.
	r.Exec(t, "SET CLUSTER SETTING str.bar = 'baz'")
	// Sleep a bit so the settings watcher has a chance to react.
	time.Sleep(time.Millisecond)
	expect("str.bar", "override")

	m.set("i1", "15", "i")
	m.set("i0", "20", "i")
	m.notify()
	expectSoon("i1", "15")
	expectSoon("i0", "20")

	m.unset("str.bar")
	m.notify()
	expectSoon("str.bar", "baz")

	m.unset("i0")
	m.unset("i1")
	m.notify()

	// i0 should revert to the default.
	expectSoon("i0", "0")
	// i1 should revert to value in the table.
	expectSoon("i1", "10")

	// Verify that version cannot be overridden.
	version, ok := settings.Lookup("version", settings.LookupForLocalAccess, settings.ForSystemTenant)
	require.True(t, ok)
	versionValue := version.String(&st.SV)

	m.set("version", "12345", "m")
	m.notify()
	// Sleep a bit so the settings watcher has a chance to react.
	time.Sleep(time.Millisecond)
	expect("version", versionValue)
}

// testingOverrideMonitor is a test-only implementation of OverrideMonitor.
type testingOverrideMonitor struct {
	ch chan struct{}

	mu struct {
		syncutil.Mutex
		overrides map[string]settings.EncodedValue
	}
}

var _ settingswatcher.OverridesMonitor = (*testingOverrideMonitor)(nil)

func newTestingOverrideMonitor() *testingOverrideMonitor {
	m := &testingOverrideMonitor{
		ch: make(chan struct{}, 1),
	}
	m.mu.overrides = make(map[string]settings.EncodedValue)
	return m
}

func (m *testingOverrideMonitor) notify() {
	select {
	case m.ch <- struct{}{}:
	default:
	}
}

func (m *testingOverrideMonitor) set(key string, val string, valType string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mu.overrides[key] = settings.EncodedValue{
		Value: val,
		Type:  valType,
	}
}

func (m *testingOverrideMonitor) unset(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mu.overrides, key)
}

// RegisterOverridesChannel is part of the settingswatcher.OverridesMonitor interface.
func (m *testingOverrideMonitor) RegisterOverridesChannel() <-chan struct{} {
	return m.ch
}

// Overrides is part of the settingswatcher.OverridesMonitor interface.
func (m *testingOverrideMonitor) Overrides() map[string]settings.EncodedValue {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make(map[string]settings.EncodedValue)
	for k, v := range m.mu.overrides {
		res[k] = v
	}
	return res
}

// Test that an error occurring during processing of the
// rangefeedcache.Watcher can be recovered after a permanent
// rangefeed failure.
func TestOverflowRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sideSettings := cluster.MakeTestingClusterSettings()
	w := settingswatcher.New(
		s.Clock(),
		s.ExecutorConfig().(sql.ExecutorConfig).Codec,
		sideSettings,
		s.RangeFeedFactory().(*rangefeed.Factory),
		s.Stopper(),
		nil,
	)
	var exitCalled int64 // accessed with atomics
	errCh := make(chan error)
	w.SetTestingKnobs(&rangefeedcache.TestingKnobs{
		PreExit:          func() { atomic.AddInt64(&exitCalled, 1) },
		ErrorInjectionCh: errCh,
	})
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
