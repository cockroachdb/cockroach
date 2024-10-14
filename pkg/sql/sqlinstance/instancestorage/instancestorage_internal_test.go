// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage

import (
	"context"
	gosql "database/sql"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func makeSession() sqlliveness.SessionID {
	session, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
	if err != nil {
		panic(err)
	}
	return session
}

func TestGetAvailableInstanceIDForRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	getAvailableInstanceID := func(storage *Storage, region []byte) (id base.SQLInstanceID, err error) {
		err = storage.db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			var err error
			id, err = storage.getAvailableInstanceIDForRegion(ctx, region, txn)
			return err
		})
		return
	}

	t.Run("no rows", func(t *testing.T) {
		stopper, storage, _, _ := setup(t, sqlDB, s)
		defer stopper.Stop(ctx)

		id, err := getAvailableInstanceID(storage, nil)
		require.Error(t, err, errNoPreallocatedRows.Error())
		require.Equal(t, base.SQLInstanceID(0), id)
	})

	t.Run("preallocated rows", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t, sqlDB, s)
		defer stopper.Stop(ctx)

		// Pre-allocate four instances.
		region := enum.One
		instanceIDs := [...]base.SQLInstanceID{4, 3, 2, 1}
		rpcAddresses := [...]string{"addr4", "addr3", "addr2", "addr1"}
		sqlAddresses := [...]string{"addr8", "addr7", "addr6", "addr5"}
		sessionIDs := [...]sqlliveness.SessionID{makeSession(), makeSession(), makeSession(), makeSession()}
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
		for _, id := range instanceIDs {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				region,
				id,
				"",
				"",
				sqlliveness.SessionID([]byte{}),
				sessionExpiry,
				roachpb.Locality{},
				roachpb.Version{},
				/* encodeIsDraining */ true,
				/* isDraining */ false,
			))
		}

		// Take instance 4. 1 should be prioritized.
		claim(ctx, t, instanceIDs[0], rpcAddresses[0], sqlAddresses[0], sessionIDs[0], sessionExpiry, storage, slStorage)
		id, err := getAvailableInstanceID(storage, region)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(1), id)

		// Take instance 1 and 2. 3 should be prioritized.
		claim(ctx, t, instanceIDs[2], rpcAddresses[2], sqlAddresses[2], sessionIDs[2], sessionExpiry, storage, slStorage)
		claim(ctx, t, instanceIDs[3], rpcAddresses[3], sqlAddresses[3], sessionIDs[3], sessionExpiry, storage, slStorage)
		id, err = getAvailableInstanceID(storage, region)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(3), id)

		// Take instance 3. No rows left.
		claim(ctx, t, instanceIDs[1], rpcAddresses[1], sqlAddresses[1], sessionIDs[1], sessionExpiry, storage, slStorage)
		id, err = getAvailableInstanceID(storage, region)
		require.Error(t, err, errNoPreallocatedRows.Error())
		require.Equal(t, base.SQLInstanceID(0), id)

		// Make instance 3 and 4 expire. 3 should be prioritized.
		require.NoError(t, slStorage.Delete(ctx, sessionIDs[0]))
		require.NoError(t, slStorage.Delete(ctx, sessionIDs[1]))
		id, err = getAvailableInstanceID(storage, region)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(3), id)
	})
}

func TestIdsToAllocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		name       string
		target     int
		regions    [][]byte
		instances  []instancerow
		toAllocate []instancerow
	}

	nonEmptySession := makeSession()
	regions := [][]byte{{0}, {1}, {2}, {3}, {4}}

	for _, tc := range []testCase{
		{
			name:    "initial-allocation",
			target:  2,
			regions: [][]byte{regions[1], regions[2], regions[3]},
			toAllocate: []instancerow{
				{region: regions[1], instanceID: 1},
				{region: regions[1], instanceID: 2},
				{region: regions[2], instanceID: 3},
				{region: regions[2], instanceID: 4},
				{region: regions[3], instanceID: 5},
				{region: regions[3], instanceID: 6},
			},
		},
		{
			name:    "partial-allocation",
			target:  2,
			regions: [][]byte{regions[1], regions[2], regions[3]},
			instances: []instancerow{
				{region: regions[1], instanceID: 1, sessionID: nonEmptySession},
				{region: regions[1], instanceID: 2},
				/* 3 is a hole */
				{region: regions[2], instanceID: 4, sessionID: nonEmptySession},
				/* 5 and 6 are holes */
				{region: regions[2], instanceID: 7, sessionID: nonEmptySession},
			},
			toAllocate: []instancerow{
				{region: regions[1], instanceID: 3},
				{region: regions[2], instanceID: 5},
				{region: regions[2], instanceID: 6},
				{region: regions[3], instanceID: 8},
				{region: regions[3], instanceID: 9},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.toAllocate, idsToAllocate(tc.target, tc.regions, tc.instances))
		})
	}
}

func TestIdsToReclaim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type instance struct {
		id      int
		session sqlliveness.SessionID
		dead    bool
	}

	type testCase struct {
		name      string
		target    int
		instances []instance
		toReclaim []base.SQLInstanceID
		toDelete  []base.SQLInstanceID
	}

	sessions := make([]sqlliveness.SessionID, 10)
	for i := range sessions {
		sessions[i] = makeSession()
	}

	for _, tc := range []testCase{
		{
			name:   "reclaim-and-delete",
			target: 2,
			instances: []instance{
				{id: 1, session: sessions[1], dead: true},
				{id: 2, session: sessions[2], dead: true},
				{id: 3, session: sessions[3], dead: true},
				{id: 4, session: sessions[4]},
				{id: 5, session: ""},
			},
			toReclaim: []base.SQLInstanceID{1, 2},
			toDelete:  []base.SQLInstanceID{3, 5},
		},
		{
			name:   "reclaim",
			target: 5,
			instances: []instance{
				{id: 1, session: sessions[1], dead: true},
				{id: 2, session: sessions[2], dead: true},
				{id: 3, session: sessions[3], dead: true},
				{id: 4, session: sessions[4]},
				{id: 5, session: ""},
			},
			toReclaim: []base.SQLInstanceID{1, 2, 3},
		},
		{
			name:   "delete",
			target: 2,
			instances: []instance{
				{id: 1},
				{id: 2},
				{id: 3},
				{id: 4},
			},
			toDelete: []base.SQLInstanceID{3, 4},
		},
		{
			name:   "all-in-use",
			target: 5,
			instances: []instance{
				{id: 1, session: sessions[1]},
				{id: 2, session: sessions[2]},
				{id: 3, session: sessions[3]},
				{id: 4, session: sessions[4]},
			},
		},
		{
			name:   "all-pre-allocated",
			target: 5,
			instances: []instance{
				{id: 1},
				{id: 2},
				{id: 3},
				{id: 4},
				{id: 5},
			},
		},
		{
			name:   "empty",
			target: 5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			instances := make([]instancerow, len(tc.instances))
			isExpired := map[sqlliveness.SessionID]bool{}
			for i, instance := range tc.instances {
				instances[i] = instancerow{instanceID: base.SQLInstanceID(instance.id), sessionID: instance.session}
				if instance.session != "" && instance.dead {
					isExpired[instance.session] = true
				}
			}

			toReclaim, toDelete := idsToReclaim(tc.target, instances, isExpired)
			require.Equal(t, tc.toReclaim, toReclaim)
			require.Equal(t, tc.toDelete, toDelete)
		})
	}
}

func TestReclaimAndGenerateInstanceRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	const expiration = time.Minute
	const preallocatedCount = 5
	PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)

	regions := [][]byte{enum.One}

	t.Run("nothing preallocated", func(t *testing.T) {
		stopper, storage, _, clock := setup(t, sqlDB, s)
		defer stopper.Stop(ctx)

		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

		require.NoError(t, storage.generateAvailableInstanceRows(ctx, regions, sessionExpiry))

		instances, err := storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		for i := 1; i <= preallocatedCount; i++ {
			require.Equal(t, base.SQLInstanceID(i), instances[i-1].InstanceID)
			require.Empty(t, instances[i-1].SessionID)
			require.Empty(t, instances[i-1].InstanceRPCAddr)
			require.Empty(t, instances[i-1].InstanceSQLAddr)
		}
	})

	t.Run("with some preallocated", func(t *testing.T) {
		stopper, storage, slStorage, clock := setup(t, sqlDB, s)
		defer stopper.Stop(ctx)

		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

		region := enum.One
		instanceIDs := [...]base.SQLInstanceID{1, 3, 5, 8}
		rpcAddresses := [...]string{"addr1", "addr3", "addr5", "addr8"}
		sqlAddresses := [...]string{"addr9", "addr10", "addr11", "addr12"}
		sessionIDs := [...]sqlliveness.SessionID{makeSession(), makeSession(), makeSession(), makeSession()}

		// Preallocate first two, and claim the other two (have not expired)
		for _, i := range []int{0, 1} {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				region,
				instanceIDs[i],
				"",
				"",
				sqlliveness.SessionID([]byte{}),
				sessionExpiry,
				roachpb.Locality{},
				roachpb.Version{},
				/* encodeIsDraining */ true,
				/* isDraining */ false,
			))
		}
		for _, i := range []int{2, 3} {
			claim(ctx, t, instanceIDs[i], rpcAddresses[i], sqlAddresses[i], sessionIDs[i], sessionExpiry, storage, slStorage)
		}

		// Generate available rows.
		require.NoError(t, storage.generateAvailableInstanceRows(ctx, regions, sessionExpiry))

		instances, err := storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount+2, len(instances), "instances: %+v", instances)
		{
			var foundIDs []base.SQLInstanceID
			for i := 0; i < preallocatedCount; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceRPCAddr)
				require.Empty(t, instances[i].InstanceSQLAddr)
			}
			for i := preallocatedCount; i < preallocatedCount+2; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.NotEmpty(t, instances[i].SessionID)
				require.NotEmpty(t, instances[i].InstanceRPCAddr)
				require.NotEmpty(t, instances[i].InstanceSQLAddr)
			}
			require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 6, 5, 8}, foundIDs)
		}

		// Now make 5 and 8 expire.
		for i := 2; i < 4; i++ {
			require.NoError(t, slStorage.Delete(ctx, sessionIDs[i]))
		}

		// Delete the expired rows.
		require.NoError(t, storage.reclaimRegion(ctx, region))

		instances, err = storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances), "instances: %+v", instances)
		{
			var foundIDs []base.SQLInstanceID
			for i := 0; i < preallocatedCount; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceRPCAddr)
				require.Empty(t, instances[i].InstanceSQLAddr)
			}
			require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 5}, foundIDs)
		}

		// Claim 1 and 3, and make them expire.
		for i := 0; i < 2; i++ {
			claim(ctx, t, instanceIDs[i], rpcAddresses[i], sqlAddresses[i], sessionIDs[i], sessionExpiry, storage, slStorage)
			require.NoError(t, slStorage.Delete(ctx, sessionIDs[i]))
		}

		// Should reclaim expired rows.
		require.NoError(t, storage.reclaimRegion(ctx, region))

		instances, err = storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		{
			var foundIDs []base.SQLInstanceID
			for i := 0; i < preallocatedCount; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceRPCAddr)
				require.Empty(t, instances[i].InstanceSQLAddr)
			}
			require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 5}, foundIDs)
		}
	})
}

func sortInstancesForTest(instances []sqlinstance.InstanceInfo) {
	sort.SliceStable(instances, func(idx1, idx2 int) bool {
		addr1, addr2 := instances[idx1].InstanceRPCAddr, instances[idx2].InstanceRPCAddr
		switch {
		case addr1 == "" && addr2 == "":
			// Both are available.
			return instances[idx1].InstanceID < instances[idx2].InstanceID
		case addr1 == "":
			// addr1 should go before addr2.
			return true
		case addr2 == "":
			// addr2 should go before addr1.
			return false
		default:
			// Both are used.
			return instances[idx1].InstanceID < instances[idx2].InstanceID
		}
	})
}

func setup(
	t *testing.T, sqlDB *gosql.DB, s serverutils.ApplicationLayerInterface,
) (*stop.Stopper, *Storage, *slstorage.FakeStorage, *hlc.Clock) {
	dbName := t.Name()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := GetTableSQLForDatabase(dbName)
	tDB.Exec(t, schema)
	table := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), dbName, "sql_instances")
	clock := hlc.NewClockForTesting(nil)
	stopper := stop.NewStopper()
	slStorage := slstorage.NewFakeStorage()
	f := s.RangeFeedFactory().(*rangefeed.Factory)
	storage := NewTestingStorage(s.DB(), s.Codec(), table, slStorage, s.ClusterSettings(), clock, f, s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
	return stopper, storage, slStorage, clock
}

func claim(
	ctx context.Context,
	t *testing.T,
	instanceID base.SQLInstanceID,
	rpcAddr string,
	sqlAddr string,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	storage *Storage,
	slStorage *slstorage.FakeStorage,
) {
	t.Helper()
	region, _, err := slstorage.UnsafeDecodeSessionID(sessionID)
	require.NoError(t, err)
	require.NoError(t, slStorage.Insert(ctx, sessionID, sessionExpiration))
	require.NoError(t, storage.CreateInstanceDataForTest(
		ctx, region, instanceID, rpcAddr, sqlAddr, sessionID,
		sessionExpiration, roachpb.Locality{}, roachpb.Version{},
		/* encodeIsDraining */ true,
		/* isDraining */ false,
	))
}
