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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

func TestGetAvailableInstanceIDsForRegion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	t.Run("no rows", func(t *testing.T) {
		stopper, storage, _, _ := setup(t, sqlDB, kvDB)
		defer stopper.Stop(ctx)

		ids, err := storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 0 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 0)

		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 1 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 1)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(1): false,
		}, ids)

		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 3 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(1): false,
			base.SQLInstanceID(2): false,
			base.SQLInstanceID(3): false,
		}, ids)
	})

	t.Run("preallocated rows", func(t *testing.T) {
		const expiration1 = time.Minute
		const expiration2 = 2 * time.Minute
		stopper, storage, slStorage, clock := setup(t, sqlDB, kvDB)
		defer stopper.Stop(ctx)

		// Pre-allocate four instances.
		instanceIDs := [...]base.SQLInstanceID{4, 3, 2, 1}
		addresses := [...]string{"addr4", "addr3", "addr2", "addr1"}
		sessionIDs := [...]sqlliveness.SessionID{"session4", "session3", "session2", "session1"}
		sessionExpiry := clock.Now().Add(expiration1.Nanoseconds(), 0)
		{
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
		}

		// Take instance 4. Both 1 and 2 should be prioritized.
		claim(ctx, t, instanceIDs[0], addresses[0], sessionIDs[0],
			sessionExpiry, storage, slStorage)
		ids, err := storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 2 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 2)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(1): true,
			base.SQLInstanceID(2): true,
		}, ids)

		// Take instance 1. Both 2 and 3 should be prioritized.
		claim(ctx, t, instanceIDs[3], addresses[3], sessionIDs[3],
			clock.Now().Add(expiration2.Nanoseconds(), 0), storage, slStorage)
		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 2 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 2)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(2): true,
			base.SQLInstanceID(3): true,
		}, ids)

		// Request for 3; insufficient, so take 2, 3, and 5.
		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 3 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(2): true,
			base.SQLInstanceID(3): true,
			base.SQLInstanceID(5): false,
		}, ids)

		// Make instance 4 expire. We will take 2, 3, 4.
		require.NoError(t, slStorage.Delete(ctx, sessionIDs[0]))
		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 3 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(2): true,
			base.SQLInstanceID(3): true,
			base.SQLInstanceID(4): false,
		}, ids)

		// Request for 4; insufficient, so take 2, 3, 4, 5.
		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 4 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 4)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(2): true,
			base.SQLInstanceID(3): true,
			base.SQLInstanceID(4): false,
			base.SQLInstanceID(5): false,
		}, ids)

	})

	t.Run("mixed rows", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t, sqlDB, kvDB)
		defer stopper.Stop(ctx)

		// Pre-allocate three instances, then claim 1 and 7.
		// 1 (used), 3 (preallocated), 7 (used).
		instanceIDs := [...]base.SQLInstanceID{1, 3, 7}
		addresses := [...]string{"addr1", "addr3", "addr7"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session3", "session7"}
		{
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
			claim(ctx, t, instanceIDs[0], addresses[0], sessionIDs[0], sessionExpiry, storage, slStorage)
			claim(ctx, t, instanceIDs[2], addresses[2], sessionIDs[2], sessionExpiry, storage, slStorage)
		}

		ids, err := storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 3 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 3)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(2): false,
			base.SQLInstanceID(3): true,
			base.SQLInstanceID(4): false,
		}, ids)

		ids, err = storage.getAvailableInstanceIDsForRegion(ctx, storage.db, 6 /* count */)
		require.NoError(t, err)
		require.Len(t, ids, 6)
		require.Equal(t, map[base.SQLInstanceID]bool{
			base.SQLInstanceID(2): false,
			base.SQLInstanceID(3): true,
			base.SQLInstanceID(4): false,
			base.SQLInstanceID(5): false,
			base.SQLInstanceID(6): false,
			base.SQLInstanceID(8): false,
		}, ids)
	})
}

func TestGenerateAvailableInstanceRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	const expiration = time.Minute
	stopper, storage, _, clock := setup(t, sqlDB, kvDB)
	defer stopper.Stop(ctx)

	sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

	t.Run("nothing preallocated", func(t *testing.T) {
		require.NoError(t, storage.generateAvailableInstanceRows(ctx, sessionExpiry))

		instances, err := storage.GetAllInstancesDataForTest(ctx)
		sortInstances(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		for i := 1; i < preallocatedCount; i++ {
			require.Equal(t, base.SQLInstanceID(i), instances[i-1].InstanceID)
			require.Empty(t, instances[i-1].SessionID)
			require.Empty(t, instances[i-1].InstanceAddr)
		}
	})

	t.Run("with some preallocated", func(t *testing.T) {
		instanceIDs := [...]base.SQLInstanceID{1, 3, 5, 8}
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

		instances, err := storage.GetAllInstancesDataForTest(ctx)
		sortInstances(instances)
		require.NoError(t, err)
		require.Equal(t, preallocatedCount, len(instances))
		for i := 1; i < preallocatedCount; i++ {
			require.Equal(t, base.SQLInstanceID(i), instances[i-1].InstanceID)
			require.Empty(t, instances[i-1].SessionID)
			require.Empty(t, instances[i-1].InstanceAddr)
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

func sortInstances(instances []sqlinstance.InstanceInfo) {
	sort.SliceStable(instances, func(idx1, idx2 int) bool {
		return instances[idx1].InstanceID < instances[idx2].InstanceID
	})
}

func setup(
	t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB,
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
	storage := NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage)
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
