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
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
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

	toSet := map[string][]interface{}{
		"sql.defaults.experimental_hash_sharded_indexes.enabled": {true, false},
		"kv.queue.process.guaranteed_time_budget":                {"17s", "20s"},
		"sql.txn_stats.sample_rate":                              {.23, .55},
		"cluster.organization":                                   {"foobar", "bazbax"},
		// Include a system-only setting to verify that we don't try to change its
		// value (which would cause a panic in test builds).
		"kv.snapshot_rebalance.max_rate": {1024, 2048},
	}
	fakeTenant := roachpb.MakeTenantID(2)
	systemTable := keys.SystemSQLCodec.TablePrefix(keys.SettingsTableID)
	fakeCodec := keys.MakeSQLCodec(fakeTenant)
	fakeTenantPrefix := keys.MakeTenantPrefix(fakeTenant)

	db := tc.Server(0).DB()

	copySettingsFromSystemToFakeTenant := func() {
		rows, err := db.Scan(ctx, systemTable, systemTable.PrefixEnd(), 0 /* maxRows */)
		require.NoError(t, err)
		for _, row := range rows {
			rem, _, err := keys.DecodeTenantPrefix(row.Key)
			require.NoError(t, err)
			tenantKey := append(fakeTenantPrefix, rem...)
			row.Value.ClearChecksum()
			row.Value.Timestamp = hlc.Timestamp{}
			require.NoError(t, db.Put(ctx, tenantKey, row.Value))
		}
	}
	checkSettingsValuesMatch := func(a, b *cluster.Settings) error {
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
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[0])
	}
	copySettingsFromSystemToFakeTenant()
	s0 := tc.Server(0)
	tenantSettings := cluster.MakeTestingClusterSettings()
	tenantSettings.SV.SetNonSystemTenant()
	sw := settingswatcher.New(s0.Clock(), fakeCodec, tenantSettings,
		s0.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		tc.Stopper())
	require.NoError(t, sw.Start(ctx))
	require.NoError(t, checkSettingsValuesMatch(s0.ClusterSettings(), tenantSettings))
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[1])
	}
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkSettingsValuesMatch(s0.ClusterSettings(), tenantSettings)
	})
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
	w := settingswatcher.NewWithOverrides(ts.Clock(), keys.SystemSQLCodec, st, f, stopper, m)
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
		overrides map[string]settingswatcher.RawValue
	}
}

var _ settingswatcher.OverridesMonitor = (*testingOverrideMonitor)(nil)

func newTestingOverrideMonitor() *testingOverrideMonitor {
	m := &testingOverrideMonitor{
		ch: make(chan struct{}, 1),
	}
	m.mu.overrides = make(map[string]settingswatcher.RawValue)
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

	m.mu.overrides[key] = settingswatcher.RawValue{
		Value: val,
		Type:  valType,
	}
}

func (m *testingOverrideMonitor) unset(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mu.overrides, key)
}

// NotifyCh is part of the settingswatcher.OverridesMonitor interface.
func (m *testingOverrideMonitor) NotifyCh() <-chan struct{} {
	return m.ch
}

// Overrides is part of the settingswatcher.OverridesMonitor interface.
func (m *testingOverrideMonitor) Overrides() map[string]settingswatcher.RawValue {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make(map[string]settingswatcher.RawValue)
	for k, v := range m.mu.overrides {
		res[k] = v
	}
	return res
}
