// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/sqllivenesstestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// Test that the column appears only after the upgrade.
func TestSQLInstancesAddLocalityAddresses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_4)

	ctx := context.Background()
	ts, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
	})
	defer ts.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`SELECT locality_addresses FROM system.sql_instances`)
	require.Error(t, err, "column should be absent before upgrade")

	upgrades.Upgrade(t, sqlDB, clusterversion.V25_4_SQLInstancesAddLocalityAddresses, nil, false)

	_, err = sqlDB.Exec(`SELECT locality_addresses FROM system.sql_instances`)
	require.NoError(t, err, "column should exist after upgrade")
}

// More exhaustive checks: creating rows before and after the upgrade
func TestCreateInstancesAndUpgradesLocalityAddresses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_4)

	ctx := context.Background()

	setupServer := func(t *testing.T) (serverutils.TestServerInterface, *gosql.DB, *kv.DB) {
		return serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		})
	}

	setup := func(
		t *testing.T, ts serverutils.TestServerInterface, kvDB *kv.DB,
	) (*stop.Stopper, *instancestorage.Storage, *hlc.Clock) {
		s := ts.ApplicationLayer()

		const preallocated = 5
		instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocated)

		stopper := stop.NewStopper()
		clock := hlc.NewClockForTesting(nil)
		sl := slstorage.NewFakeStorage()
		factory := s.RangeFeedFactory().(*rangefeed.Factory)

		st := instancestorage.NewStorage(
			kvDB, s.Codec(), sl, s.ClusterSettings(), s.Clock(), factory,
			s.SettingsWatcher().(*settingswatcher.SettingsWatcher),
		)
		return stopper, st, clock
	}

	makeSession := func() *sqllivenesstestutils.FakeSession {
		id, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		return &sqllivenesstestutils.FakeSession{SessionID: id}
	}

	localityAddresses := []roachpb.LocalityAddress{
		{Address: util.MakeUnresolvedAddr("tcp", "1.0.0.1"), LocalityTier: roachpb.Tier{Key: "region", Value: "east"}},
		{Address: util.MakeUnresolvedAddr("tcp", "2.0.0.1"), LocalityTier: roachpb.Tier{Key: "zone", Value: "b"}},
	}

	// -----------------------------------------------------------------------

	t.Run("create-and-upgrade", func(t *testing.T) {
		ts, sqlDB, kvDB := setupServer(t)
		defer ts.Stopper().Stop(ctx)
		stopper, storage, clock := setup(t, ts, kvDB)
		defer stopper.Stop(ctx)

		const rpcAddr, sqlAddr = "rpcAddr", "sqlAddr"
		locality := roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "test"}, {Key: "az", Value: "a"},
		}}
		version := roachpb.Version{Major: 29, Minor: 1}
		const expiry = time.Minute

		create := func(id base.SQLInstanceID) {
			s := makeSession()
			now := clock.Now()
			s.StartTS, s.ExpTS = now, now.Add(expiry.Nanoseconds(), 0)
			inst, err := storage.CreateNodeInstance(
				ctx, s, rpcAddr, sqlAddr, locality, version, roachpb.NodeID(id), localityAddresses)
			require.NoError(t, err)
			require.Equal(t, id, inst.InstanceID)
		}

		// Create on the old schema.
		const firstID = base.SQLInstanceID(1000)
		create(firstID)

		// Upgrade.
		upgrades.Upgrade(t, sqlDB, clusterversion.V25_4_SQLInstancesAddLocalityAddresses, nil, false)

		// Create on the new schema including locality‑addresses.
		const secondID = base.SQLInstanceID(1001)
		create(secondID)

		// older cluster version do not break if they
		// encounter a new schema value before the cluster upgrade.
		// SQL‑level validation.
		sql := sqlutils.MakeSQLRunner(sqlDB)
		sql.CheckQueryResults(t,
			`SELECT id, locality_addresses IS NOT NULL FROM system.sql_instances WHERE id>=1000`,
			[][]string{{"1000", "false"}, {"1001", "true"}},
		)

		// KV‑level validation
		all, err := storage.GetAllInstancesDataForTest(ctx)
		require.NoError(t, err)
		instancestorage.SortInstances(all)
		require.Equal(t, base.SQLInstanceID(1), all[0].GetInstanceID())
		require.Equal(t, firstID, all[1].GetInstanceID())
		require.Equal(t, []roachpb.LocalityAddress(nil), all[1].LocalityAddresses)
		require.Equal(t, secondID, all[2].GetInstanceID())
		require.Equal(t, []roachpb.LocalityAddress{
			{Address: util.UnresolvedAddr{NetworkField: "tcp", AddressField: "1.0.0.1"}, LocalityTier: roachpb.Tier{Key: "region", Value: "east"}},
			{Address: util.UnresolvedAddr{NetworkField: "tcp", AddressField: "2.0.0.1"}, LocalityTier: roachpb.Tier{Key: "zone", Value: "b"}}},
			all[2].LocalityAddresses)
	})
}
