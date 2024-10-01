// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestSQLInstancesAddIsDraining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	_, err := sqlDB.Exec("SELECT is_draining FROM system.sql_instances")
	require.Error(t, err, "system.sql_instances is_draining columns should not exist")
	upgrades.Upgrade(t, sqlDB, clusterversion.V24_3_SQLInstancesAddDraining, nil, false)
	_, err = sqlDB.Exec("SELECT is_draining FROM system.sql_instances")
	require.NoError(t, err, "system.sql_instances is_draining columns should exist")
}

func TestCreateInstancesAndUpgrades(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	setup := func(t *testing.T, ts serverutils.TestServerInterface, kvDB *kv.DB) (
		*stop.Stopper, *instancestorage.Storage, *hlc.Clock) {
		s := ts.ApplicationLayer()
		const preallocatedCount = 5
		instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)
		clock := hlc.NewClockForTesting(nil)
		stopper := stop.NewStopper()
		slStorage := slstorage.NewFakeStorage()
		f := s.RangeFeedFactory().(*rangefeed.Factory)
		storage := instancestorage.NewStorage(kvDB, s.Codec(), slStorage, s.ClusterSettings(),
			s.Clock(), f, s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
		return stopper, storage, clock
	}

	makeSession := func() *sqllivenesstestutils.FakeSession {
		sessionID, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		if err != nil {
			panic(err)
		}
		return &sqllivenesstestutils.FakeSession{SessionID: sessionID}
	}

	t.Run("create-instance-before-and-after-upgrade", func(t *testing.T) {
		ts, sqlDB, kvDB := setupServer(t)
		defer ts.Stopper().Stop(ctx)
		stopper, storage, clock := setup(t, ts, kvDB)
		defer stopper.Stop(ctx)
		const rpcAddr = "rpcAddr"
		const sqlAddr = "sqlAddr"
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		binaryVersion := roachpb.Version{Major: 28, Minor: 4}
		const expiration = time.Minute

		createInstance := func(id base.SQLInstanceID) {
			session := makeSession()
			session.StartTS = clock.Now()
			session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
			instance, err := storage.CreateNodeInstance(
				ctx, session, rpcAddr, sqlAddr, locality, binaryVersion, roachpb.NodeID(id))
			require.NoError(t, err)
			require.Equal(t, id, instance.InstanceID)
		}

		const firstId = base.SQLInstanceID(1000)
		createInstance(firstId)
		upgrades.Upgrade(t, sqlDB, clusterversion.V24_3_SQLInstancesAddDraining, nil, false)
		const secondId = base.SQLInstanceID(1001)
		createInstance(secondId)

		// Verify the rows using SQL
		r := sqlutils.MakeSQLRunner(sqlDB)
		r.CheckQueryResults(t,
			`SELECT id, is_draining FROM system.sql_instances WHERE id >= 1000`,
			[][]string{{"1000", "NULL"}, {"1001", "false"}})

		// Verify the rows by decoding key-values
		instances, err := storage.GetAllInstancesDataForTest(ctx)
		require.NoError(t, err)
		instancestorage.SortInstances(instances)
		require.Equal(t, 3, len(instances)) // Instead with id 1 also exists
		require.Equal(t, base.SQLInstanceID(1), instances[0].GetInstanceID())
		require.Equal(t, firstId, instances[1].GetInstanceID())
		require.Equal(t, false, instances[1].IsDraining)
		require.Equal(t, secondId, instances[2].GetInstanceID())
		require.Equal(t, false, instances[2].IsDraining)
	})

	t.Run("old-readers-reading-new-schema-values", func(t *testing.T) {
		ts, sqlDB, kvDB := setupServer(t)
		defer ts.Stopper().Stop(ctx)
		stopper, storage, clock := setup(t, ts, kvDB)
		defer stopper.Stop(ctx)
		const rpcAddr = "rpcAddr"
		const sqlAddr = "sqlAddr"
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		binaryVersion := roachpb.Version{Major: 28, Minor: 4}
		const expiration = time.Minute
		// createInstance bypasses version guard and directly writes the key-value
		// to storage as it uses CreateInstanceDataForTest instead of
		// CreateNodeInstance.
		createInstance := func(id base.SQLInstanceID, encodeIsDraining bool, isDraining bool) {
			session := makeSession()
			err := storage.CreateInstanceDataForTest(
				ctx, enum.One, id, rpcAddr, sqlAddr,
				session.SessionID, clock.Now().Add(expiration.Nanoseconds(), 0),
				locality, binaryVersion, encodeIsDraining, isDraining)
			require.NoError(t, err)
		}

		const firstId = base.SQLInstanceID(1000)
		createInstance(firstId, true, true)
		const secondId = base.SQLInstanceID(1001)
		createInstance(secondId, true, false)
		const thirdId = base.SQLInstanceID(1002)
		createInstance(thirdId, false, false)

		// This demonstrates that readers (SQL, instancereader) on an older
		// cluster version do not break if they encounter a new schema value
		// before the cluster upgrade. This provides additional confidence on
		// top of the version guard we already have.
		_, err := sqlDB.Exec("SELECT is_draining FROM system.sql_instances")
		require.Error(t, err, "system.sql_instances is_draining columns should not exist")
		// Verify the rows using SQL
		r := sqlutils.MakeSQLRunner(sqlDB)
		r.CheckQueryResults(t,
			`SELECT id, sql_addr FROM system.sql_instances WHERE id >= 1000`,
			[][]string{{"1000", sqlAddr}, {"1001", sqlAddr}, {"1002", sqlAddr}})

		// Verify the rows by decoding key-values
		instances, err := storage.GetAllInstancesDataForTest(ctx)
		require.NoError(t, err)
		instancestorage.SortInstances(instances)
		require.Equal(t, 4, len(instances)) // Instead with id 1 also exists
		require.Equal(t, base.SQLInstanceID(1), instances[0].GetInstanceID())
		require.Equal(t, firstId, instances[1].GetInstanceID())
		require.Equal(t, true, instances[1].IsDraining)
		require.Equal(t, secondId, instances[2].GetInstanceID())
		require.Equal(t, false, instances[2].IsDraining)
		require.Equal(t, thirdId, instances[3].GetInstanceID())
		require.Equal(t, false, instances[3].IsDraining)
	})
}
