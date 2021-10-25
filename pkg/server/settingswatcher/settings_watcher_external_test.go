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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	toSet := map[string][]interface{}{
		"sql.defaults.experimental_hash_sharded_indexes.enabled": {true, false},
		"kv.queue.process.guaranteed_time_budget":                {"17s", "20s"},
		"sql.txn_stats.sample_rate":                              {.23, .55},
		"cluster.organization":                                   {"foobar", "bazbax"},
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
	copySettingsFromSystemToFakeTenant := func() (numSet int) {
		rows := getSourceClusterRows()
		require.NoError(t, db.Txn(ctx, func(
			ctx context.Context, txn *kv.Txn,
		) error {
			if _, err := txn.DelRange(
				ctx,
				fakeCodec.TablePrefix(keys.SettingsTableID),
				fakeCodec.TablePrefix(keys.SettingsTableID).PrefixEnd(),
				false,
			); err != nil {
				return err
			}

			ba := txn.NewBatch()
			for _, row := range rows {
				rem, _, err := keys.DecodeTenantPrefix(row.Key)
				require.NoError(t, err)
				var req roachpb.PutRequest
				req.Key = append(fakeTenantPrefix, rem...)
				req.Value = *row.Value
				req.Value.ClearChecksum()
				req.Value.Timestamp = hlc.Timestamp{}
				ba.AddRawRequest(&req)
			}
			return txn.Run(ctx, ba)
		}))
		return len(rows)
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
	checkStoredValuesMatch := func(expected []roachpb.KeyValue) error {
		got := getSourceClusterRows()
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
	baseNumSet := copySettingsFromSystemToFakeTenant()
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[0])
	}
	copySettingsFromSystemToFakeTenant()
	s0 := tc.Server(0)
	fakeSettings := cluster.MakeTestingClusterSettings()
	storage := &fakeStorage{}
	sw := settingswatcher.New(s0.Clock(), fakeCodec, fakeSettings,
		s0.ExecutorConfig().(sql.ExecutorConfig).RangeFeedFactory,
		tc.Stopper(), storage)
	require.NoError(t, sw.Start(ctx))
	require.NoError(t, checkSettingsValuesMatch(s0.ClusterSettings(), fakeSettings))

	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[1])
	}

	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkSettingsValuesMatch(s0.ClusterSettings(), fakeSettings)
	})
	// Shorten the closed timestamp duration as a cheeky way to check the
	// checkpointing code while also speeding up the test.
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
	copySettingsFromSystemToFakeTenant()
	testutils.SucceedsSoon(t, func() error {
		return checkStoredValuesMatch(storage.getKVs())
	})

	// Set and unset.
	for k, v := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = $1", v[1])
	}
	copySettingsFromSystemToFakeTenant()
	for k := range toSet {
		tdb.Exec(t, "SET CLUSTER SETTING "+k+" = DEFAULT")
	}
	copySettingsFromSystemToFakeTenant()

	// Make sure that the storage layer gets the right ultimate values.
	testutils.SucceedsSoon(t, func() error {
		return checkStoredValuesMatch(storage.getKVs())
	})
	// Confirm that we've removed the unset values.
	expectedSet := baseNumSet + 1 // for the closed_timestamp setting
	require.Equal(t, expectedSet, len(storage.getKVs()))
}

type fakeStorage struct {
	syncutil.Mutex
	kvs []roachpb.KeyValue
}

func (f *fakeStorage) WriteKVs(ctx context.Context, kvs []roachpb.KeyValue) error {
	f.Lock()
	defer f.Unlock()
	f.kvs = kvs
	return nil
}

func (f *fakeStorage) getKVs() []roachpb.KeyValue {
	f.Lock()
	defer f.Unlock()
	return f.kvs
}
