// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage_test

import (
	"context"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestStorage verifies that instancestorage stores and retrieves SQL instance data correctly.
// Also, it verifies that released instance IDs are correctly updated within the database
// and reused for new SQL instances.
func TestStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	setup := func(t *testing.T) (
		*stop.Stopper, *instancestorage.Storage, *slstorage.FakeStorage, *hlc.Clock,
	) {
		dbName := t.Name()
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := strings.Replace(systemschema.SQLInstancesTableSchema,
			`CREATE TABLE system.sql_instances`,
			`CREATE TABLE "`+dbName+`".sql_instances`, 1)
		tDB.Exec(t, schema)
		tableID := getTableID(t, tDB, dbName, "sql_instances")
		clock := hlc.NewClock(func() int64 {
			return timeutil.NewTestTimeSource().Now().UnixNano()
		}, base.DefaultMaxClockOffset)
		stopper := stop.NewStopper()
		slStorage := slstorage.NewFakeStorage()
		storage := instancestorage.NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage)
		return stopper, storage, slStorage, clock
	}

	t.Run("create-instance-get-instance", func(t *testing.T) {
		stopper, storage, _, clock := setup(t)
		defer stopper.Stop(ctx)
		const id = base.SQLInstanceID(1)
		const sessionID = sqlliveness.SessionID("session_id")
		const addr = "addr"
		const expiration = time.Minute
		{
			instanceID, err := storage.CreateInstance(ctx, sessionID, clock.Now().Add(expiration.Nanoseconds(), 0), addr)
			require.NoError(t, err)
			require.Equal(t, id, instanceID)
		}
	})
	t.Run("release-instance-get-all-instances", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t)
		defer stopper.Stop(ctx)
		// Create three instances and release one.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3}
		addresses := [...]string{"addr1", "addr2", "addr3"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session2", "session3"}
		{
			for index, addr := range addresses {
				sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
				instanceID, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addr)
				require.NoError(t, err)
				err = slStorage.Insert(ctx, sessionIDs[index], sessionExpiry)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, instanceIDs[index], instanceID)
			}
		}

		// Verify all instances are returned by GetAllInstancesDataForTest.
		{
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			sort.SliceStable(instances, func(idx1, idx2 int) bool {
				return instances[idx1].InstanceID < instances[idx2].InstanceID
			})
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs), len(instances))
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index], instance.InstanceID)
				require.Equal(t, sessionIDs[index], instance.SessionID)
				require.Equal(t, addresses[index], instance.InstanceAddr)
			}
		}

		// Release an instance and verify all instances are returned.
		{
			require.NoError(t, storage.ReleaseInstanceID(ctx, instanceIDs[0]))
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs)-1, len(instances))
			sortInstances(instances)
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index+1], instance.InstanceID)
				require.Equal(t, sessionIDs[index+1], instance.SessionID)
				require.Equal(t, addresses[index+1], instance.InstanceAddr)

			}
		}

		// Verify released instance ID gets reused.
		{
			var err error
			var instanceID base.SQLInstanceID
			newSessionID := sqlliveness.SessionID("session4")
			newAddr := "addr4"
			newSessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
			instanceID, err = storage.CreateInstance(ctx, newSessionID, newSessionExpiry, newAddr)
			require.NoError(t, err)
			require.Equal(t, instanceIDs[0], instanceID)
			var instances []sqlinstance.InstanceInfo
			instances, err = storage.GetAllInstancesDataForTest(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs), len(instances))
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index], instance.InstanceID)
				if index == 0 {
					require.Equal(t, newSessionID, instance.SessionID)
					require.Equal(t, newAddr, instance.InstanceAddr)
					continue
				}
				require.Equal(t, sessionIDs[index], instance.SessionID)
				require.Equal(t, addresses[index], instance.InstanceAddr)
			}
		}

		// Verify instance ID associated with an expired session gets reused.
		{
			var err error
			var instanceID base.SQLInstanceID
			newSessionID := sqlliveness.SessionID("session5")
			newAddr := "addr5"
			newSessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
			instanceID, err = storage.CreateInstance(ctx, newSessionID, newSessionExpiry, newAddr)
			require.NoError(t, err)
			require.Equal(t, instanceIDs[0], instanceID)
			var instances []sqlinstance.InstanceInfo
			instances, err = storage.GetAllInstancesDataForTest(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs), len(instances))
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index], instance.InstanceID)
				if index == 0 {
					require.Equal(t, newSessionID, instance.SessionID)
					require.Equal(t, newAddr, instance.InstanceAddr)
					continue
				}
				require.Equal(t, sessionIDs[index], instance.SessionID)
				require.Equal(t, addresses[index], instance.InstanceAddr)
			}
		}
	})
}

// TestConcurrentCreateAndRelease verifies that concurrent access to instancestorage
// to create and release SQL instance IDs works as expected.
func TestConcurrentCreateAndRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	clock := hlc.NewClock(func() int64 {
		return timeutil.NewTestTimeSource().Now().UnixNano()
	}, base.DefaultMaxClockOffset)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	dbName := t.Name()
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := strings.Replace(systemschema.SQLInstancesTableSchema,
		`CREATE TABLE system.sql_instances`,
		`CREATE TABLE "`+dbName+`".sql_instances`, 1)
	tDB.Exec(t, schema)
	tableID := getTableID(t, tDB, dbName, "sql_instances")
	stopper := stop.NewStopper()
	slStorage := slstorage.NewFakeStorage()
	defer stopper.Stop(ctx)
	storage := instancestorage.NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage)

	const (
		runsPerWorker   = 100
		workers         = 100
		controllerSteps = 100
		sessionID       = sqlliveness.SessionID("session")
		addr            = "addr"
		expiration      = time.Minute
	)
	sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
	err := slStorage.Insert(ctx, sessionID, sessionExpiry)
	if err != nil {
		t.Fatal(err)
	}
	var (
		state = struct {
			syncutil.RWMutex
			liveInstances map[base.SQLInstanceID]struct{}
			freeInstances map[base.SQLInstanceID]struct{}
			maxInstanceID base.SQLInstanceID
		}{
			liveInstances: make(map[base.SQLInstanceID]struct{}),
			freeInstances: make(map[base.SQLInstanceID]struct{}),
		}
		createInstance = func(t *testing.T) {
			t.Helper()
			state.Lock()
			defer state.Unlock()
			sessionExpiry = clock.Now().Add(expiration.Nanoseconds(), 0)
			_, err = slStorage.Update(ctx, sessionID, sessionExpiry)
			if err != nil {
				t.Fatal(err)
			}
			instanceID, err := storage.CreateInstance(ctx, sessionID, sessionExpiry, addr)
			require.NoError(t, err)
			if len(state.freeInstances) > 0 {
				_, free := state.freeInstances[instanceID]
				// Confirm that a free id was repurposed.
				require.True(t, free)
				delete(state.freeInstances, instanceID)
			}
			state.liveInstances[instanceID] = struct{}{}
			if instanceID > state.maxInstanceID {
				state.maxInstanceID = instanceID
			}
		}

		releaseInstance = func(t *testing.T) {
			t.Helper()
			state.Lock()
			defer state.Unlock()
			i := base.SQLInstanceID(-1)
			for i = range state.liveInstances {
			}
			if i == -1 {
				return
			}
			require.NoError(t, storage.ReleaseInstanceID(ctx, i))
			state.freeInstances[i] = struct{}{}
			delete(state.liveInstances, i)
		}

		step = func(t *testing.T) {
			r := rand.Float64()
			switch {
			case r < .6:
				createInstance(t)
			default:
				releaseInstance(t)
			}
		}

		pickInstance = func() base.SQLInstanceID {
			state.RLock()
			defer state.RUnlock()
			i := rand.Intn(int(state.maxInstanceID)) + 1
			return base.SQLInstanceID(i)
		}

		// checkGetInstance verifies that GetInstance returns the instance
		// details irrespective of whether the instance is live or not.
		checkGetInstance = func(t *testing.T, i base.SQLInstanceID) {
			t.Helper()
			state.RLock()
			defer state.RUnlock()
			instanceInfo, err := storage.GetInstanceDataForTest(ctx, i)
			if _, free := state.freeInstances[i]; free {
				require.Error(t, err)
				require.ErrorIs(t, err, sqlinstance.NonExistentInstanceError)
			} else {
				require.NoError(t, err)
				require.Equal(t, addr, instanceInfo.InstanceAddr)
				require.Equal(t, sessionID, instanceInfo.SessionID)
				_, live := state.liveInstances[i]
				require.True(t, live)
			}
		}

		wg        sync.WaitGroup
		runWorker = func() {
			defer wg.Done()
			for i := 0; i < runsPerWorker; i++ {
				time.Sleep(time.Microsecond)
				instance := pickInstance()
				checkGetInstance(t, instance)
			}
		}
	)

	// Ensure that there's at least one instance.
	createInstance(t)
	// Run the workers.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go runWorker()
	}
	// Step the random steps.
	for i := 0; i < controllerSteps; i++ {
		step(t)
	}
	wg.Wait()
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
