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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSettingsWatcher constructs a SettingsWatcher under a hypothetical tenant
// and then copies some values over to that tenant. It then ensures that the
// initial settings are picked up and that changes are also eventually picked
// up.
func TestSettingWatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// Interleaved tables are overridden to be on in testservers even though
	// that is not the default value.
	tdb.Exec(t, "SET CLUSTER SETTING sql.defaults.interleaved_tables.enabled = false")

	toSet := map[string][]interface{}{
		"sql.defaults.experimental_hash_sharded_indexes.enabled": {true, false},
		"kv.queue.process.guaranteed_time_budget":                {"17s", "20s"},
		"kv.closed_timestamp.close_fraction":                     {.23, .55},
		"cluster.organization":                                   {"foobar", "bazbax"},
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
		for _, k := range settings.Keys() {
			s, ok := settings.Lookup(k, settings.LookupForLocalAccess)
			require.True(t, ok)
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
	fakeSettings := cluster.MakeTestingClusterSettings()
	sw := settingswatcher.New(s0.Clock(), fakeCodec, fakeSettings,
		s0.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		tc.Stopper())
	require.NoError(t, sw.Start(ctx))
	// TestCluster randomizes the value of SeparatedIntentsEnabled, so set it to
	// the same as in fakeSettings for the subsequent equality check.
	storage.SeparatedIntentsEnabled.Override(ctx, &s0.ClusterSettings().SV, storage.SeparatedIntentsEnabled.Get(&fakeSettings.SV))
	require.NoError(t, checkSettingsValuesMatch(s0.ClusterSettings(), fakeSettings))
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[1])
	}
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkSettingsValuesMatch(s0.ClusterSettings(), fakeSettings)
	})
}
