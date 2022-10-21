// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestGetAvailableInstanceIDForRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("no rows", func(t *testing.T) {
		stopper, storage, _, _ := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		id, err := storage.getAvailableInstanceIDForRegion(ctx, storage.db)
		require.Error(t, err, errNoPreallocatedRows.Error())
		require.Equal(t, base.SQLInstanceID(0), id)
	})

	t.Run("preallocated rows", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		// Pre-allocate four instances.
		instanceIDs := [...]base.SQLInstanceID{4, 3, 2, 1}
		addresses := [...]string{"addr4", "addr3", "addr2", "addr1"}
		sessionIDs := [...]sqlliveness.SessionID{"session4", "session3", "session2", "session1"}
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
		for _, id := range instanceIDs {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				id,
				"",
				sqlliveness.SessionID([]byte{}),
				sessionExpiry,
				roachpb.Locality{},
			))
		}

		// Take instance 4. 1 should be prioritized.
		claim(ctx, t, instanceIDs[0], addresses[0], sessionIDs[0], sessionExpiry, storage, slStorage)
		id, err := storage.getAvailableInstanceIDForRegion(ctx, storage.db)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(1), id)

		// Take instance 1 and 2. 3 should be prioritized.
		claim(ctx, t, instanceIDs[2], addresses[2], sessionIDs[2], sessionExpiry, storage, slStorage)
		claim(ctx, t, instanceIDs[3], addresses[3], sessionIDs[3], sessionExpiry, storage, slStorage)
		id, err = storage.getAvailableInstanceIDForRegion(ctx, storage.db)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(3), id)

		// Take instance 3. No rows left.
		claim(ctx, t, instanceIDs[1], addresses[1], sessionIDs[1], sessionExpiry, storage, slStorage)
		id, err = storage.getAvailableInstanceIDForRegion(ctx, storage.db)
		require.Error(t, err, errNoPreallocatedRows.Error())
		require.Equal(t, base.SQLInstanceID(0), id)

		// Make instance 3 and 4 expire. 3 should be prioritized.
		require.NoError(t, slStorage.Delete(ctx, sessionIDs[0]))
		require.NoError(t, slStorage.Delete(ctx, sessionIDs[1]))
		id, err = storage.getAvailableInstanceIDForRegion(ctx, storage.db)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(3), id)
	})
}

func TestIDsToReclaim(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const preallocatedCount = 5
	PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)

	sortAsc := func(ids []base.SQLInstanceID) {
		sort.SliceStable(ids, func(i, j int) bool {
			return ids[i] < ids[j]
		})
	}

	t.Run("no rows", func(t *testing.T) {
		stopper, storage, _, _ := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		toClaim, toDelete, err := storage.idsToReclaim(ctx, storage.db)
		require.NoError(t, err)
		require.Len(t, toClaim, preallocatedCount)
		require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 5}, toClaim)
		require.Len(t, toDelete, 0)
	})

	t.Run("nothing to reclaim", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, _, clock := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		// Pre-allocate two instances.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3, 4, 5}
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
		for _, id := range instanceIDs {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				id,
				"",
				sqlliveness.SessionID([]byte{}),
				sessionExpiry,
				roachpb.Locality{},
			))
		}

		toClaim, toDelete, err := storage.idsToReclaim(ctx, storage.db)
		require.NoError(t, err)
		require.Len(t, toClaim, 0)
		require.Len(t, toDelete, 0)
	})

	t.Run("preallocated rows", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		// Pre-allocate two instances.
		instanceIDs := [...]base.SQLInstanceID{5, 2}
		addresses := [...]string{"addr5", "addr2"}
		sessionIDs := [...]sqlliveness.SessionID{"session5", "session2"}
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
		for _, id := range instanceIDs {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				id,
				"",
				sqlliveness.SessionID([]byte{}),
				sessionExpiry,
				roachpb.Locality{},
			))
		}

		toClaim, toDelete, err := storage.idsToReclaim(ctx, storage.db)
		sortAsc(toClaim)
		require.NoError(t, err)
		require.Len(t, toClaim, 3)
		require.Equal(t, []base.SQLInstanceID{1, 3, 4}, toClaim)
		require.Len(t, toDelete, 0)

		// Take instance 5.
		claim(ctx, t, instanceIDs[0], addresses[0], sessionIDs[0], sessionExpiry, storage, slStorage)
		toClaim, toDelete, err = storage.idsToReclaim(ctx, storage.db)
		sortAsc(toClaim)
		require.NoError(t, err)
		require.Len(t, toClaim, 4)
		require.Equal(t, []base.SQLInstanceID{1, 3, 4, 6}, toClaim)
		require.Len(t, toDelete, 0)

		// Take instance 2.
		claim(ctx, t, instanceIDs[1], addresses[1], sessionIDs[1], sessionExpiry, storage, slStorage)
		toClaim, toDelete, err = storage.idsToReclaim(ctx, storage.db)
		sortAsc(toClaim)
		require.NoError(t, err)
		require.Len(t, toClaim, 5)
		require.Equal(t, []base.SQLInstanceID{1, 3, 4, 6, 7}, toClaim)
		require.Len(t, toDelete, 0)

		// Make instance 5 expire.
		require.NoError(t, slStorage.Delete(ctx, sessionIDs[0]))
		toClaim, toDelete, err = storage.idsToReclaim(ctx, storage.db)
		sortAsc(toClaim)
		require.NoError(t, err)
		require.Len(t, toClaim, 5)
		require.Equal(t, []base.SQLInstanceID{1, 3, 4, 5, 6}, toClaim)
		require.Len(t, toDelete, 0)
	})

	t.Run("too many expired rows", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, _, clock := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		// Pre-allocate 10 instances.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
		for _, id := range instanceIDs {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				id,
				fmt.Sprintf("addr%d", id),
				sqlliveness.SessionID([]byte(fmt.Sprintf("session%d", id))),
				sessionExpiry,
				roachpb.Locality{},
			))
		}

		toClaim, toDelete, err := storage.idsToReclaim(ctx, storage.db)
		sortAsc(toClaim)
		require.NoError(t, err)
		require.Len(t, toClaim, 5)
		require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 5}, toClaim)
		require.Len(t, toDelete, 5)
		require.Equal(t, []base.SQLInstanceID{6, 7, 8, 9, 10}, toDelete)
	})
}

func TestGenerateAvailableInstanceRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const expiration = time.Minute
	const preallocatedCount = 5
	PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)

	t.Run("nothing preallocated", func(t *testing.T) {
		stopper, storage, _, clock := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

		require.NoError(t, storage.generateAvailableInstanceRows(ctx, sessionExpiry))

		instances, err := storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		for i := 1; i <= preallocatedCount; i++ {
			require.Equal(t, base.SQLInstanceID(i), instances[i-1].InstanceID)
			require.Empty(t, instances[i-1].SessionID)
			require.Empty(t, instances[i-1].InstanceAddr)
		}
	})

	t.Run("with some preallocated", func(t *testing.T) {
		stopper, storage, slStorage, clock := setup(t, sqlDB, kvDB, s.ClusterSettings())
		defer stopper.Stop(ctx)

		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

		instanceIDs := [...]base.SQLInstanceID{1, 3, 5, 8}
		addresses := [...]string{"addr1", "addr3", "addr5", "addr8"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session3", "session5", "session8"}

		// Preallocate first two, and claim the other two (have not expired)
		for i := 0; i < 2; i++ {
			require.NoError(t, storage.CreateInstanceDataForTest(
				ctx,
				instanceIDs[i],
				"",
				sqlliveness.SessionID([]byte{}),
				sessionExpiry,
				roachpb.Locality{},
			))
		}
		for i := 2; i < 4; i++ {
			claim(ctx, t, instanceIDs[i], addresses[i], sessionIDs[i], sessionExpiry, storage, slStorage)
		}

		// Generate available rows.
		require.NoError(t, storage.generateAvailableInstanceRows(ctx, sessionExpiry))

		instances, err := storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount+2, len(instances))
		{
			var foundIDs []base.SQLInstanceID
			for i := 0; i < preallocatedCount; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceAddr)
			}
			for i := preallocatedCount; i < preallocatedCount+2; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.NotEmpty(t, instances[i].SessionID)
				require.NotEmpty(t, instances[i].InstanceAddr)
			}
			require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 6, 5, 8}, foundIDs)
		}

		// Now make 5 and 8 expire.
		for i := 2; i < 4; i++ {
			require.NoError(t, slStorage.Delete(ctx, sessionIDs[i]))
		}

		// Delete the expired rows.
		require.NoError(t, storage.generateAvailableInstanceRows(ctx, sessionExpiry))

		instances, err = storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		{
			var foundIDs []base.SQLInstanceID
			for i := 0; i < preallocatedCount; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceAddr)
			}
			require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 6}, foundIDs)
		}

		// Claim 1 and 3, and make them expire.
		for i := 0; i < 2; i++ {
			claim(ctx, t, instanceIDs[i], addresses[i], sessionIDs[i], sessionExpiry, storage, slStorage)
			require.NoError(t, slStorage.Delete(ctx, sessionIDs[i]))
		}

		// Should reclaim expired rows.
		require.NoError(t, storage.generateAvailableInstanceRows(ctx, sessionExpiry))

		instances, err = storage.GetAllInstancesDataForTest(ctx)
		sortInstancesForTest(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		{
			var foundIDs []base.SQLInstanceID
			for i := 0; i < preallocatedCount; i++ {
				foundIDs = append(foundIDs, instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceAddr)
			}
			require.Equal(t, []base.SQLInstanceID{1, 2, 3, 4, 6}, foundIDs)
		}
	})
}

func getTableID(
	t *testing.T, db *sqlutils.SQLRunner, dbName, tableName string,
) (tableID descpb.ID) {
	t.Helper()
	db.QueryRow(t, `
 select u.id
  from system.namespace t
  join system.namespace u
  on t.id = u."parentID"
  where t.name = $1 and u.name = $2`,
		dbName, tableName).Scan(&tableID)
	return tableID
}

func sortInstancesForTest(instances []sqlinstance.InstanceInfo) {
	sort.SliceStable(instances, func(idx1, idx2 int) bool {
		addr1, addr2 := instances[idx1].InstanceAddr, instances[idx2].InstanceAddr
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
	t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, settings *cluster.Settings,
) (*stop.Stopper, *Storage, *slstorage.FakeStorage, *hlc.Clock) {
	dbName := t.Name()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := strings.Replace(systemschema.SQLInstancesTableSchema,
		`CREATE TABLE system.sql_instances`,
		`CREATE TABLE "`+dbName+`".sql_instances`, 1)
	tDB.Exec(t, schema)
	tableID := getTableID(t, tDB, dbName, "sql_instances")
	clock := hlc.NewClock(timeutil.NewTestTimeSource(), base.DefaultMaxClockOffset)
	stopper := stop.NewStopper()
	slStorage := slstorage.NewFakeStorage()
	storage := NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage, settings)
	return stopper, storage, slStorage, clock
}

func claim(
	ctx context.Context,
	t *testing.T,
	instanceID base.SQLInstanceID,
	addr string,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	storage *Storage,
	slStorage *slstorage.FakeStorage,
) {
	require.NoError(t, slStorage.Insert(ctx, sessionID, sessionExpiration))
	require.NoError(t, storage.CreateInstanceDataForTest(
		ctx, instanceID, addr, sessionID, sessionExpiration, roachpb.Locality{},
	))
}
