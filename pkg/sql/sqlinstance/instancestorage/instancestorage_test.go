// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	setup := func(t *testing.T) (
		*stop.Stopper, *instancestorage.Storage,
	) {
		dbName := t.Name()
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := strings.Replace(systemschema.SQLInstancesTableSchema,
			`CREATE TABLE system.sql_instances`,
			`CREATE TABLE "`+dbName+`".sql_instances`, 1)
		tDB.Exec(t, schema)
		tableID := getTableID(t, tDB, dbName, "sql_instances")
		stopper := stop.NewStopper()
		storage := instancestorage.NewTestingStorage(stopper, kvDB, keys.SystemSQLCodec, tableID)
		return stopper, storage
	}

	t.Run("storage-not-started", func(t *testing.T) {
		stopper, storage := setup(t)
		defer stopper.Stop(ctx)
		const id = base.SQLInstanceID(1)
		const sessionID = sqlliveness.SessionID("session_id")
		const httpAddr = "http_addr"
		var err error
		_, err = storage.CreateInstance(ctx, sessionID, httpAddr)
		require.Error(t, err)
		require.Equal(t, sqlinstance.NotStartedError, err)
		_, err = storage.GetInstanceAddr(ctx, id)
		require.Error(t, err)
		require.Equal(t, sqlinstance.NotStartedError, err)
		_, err = storage.GetAllInstancesForTenant(ctx)
		require.Error(t, err)
		require.Equal(t, sqlinstance.NotStartedError, err)
	})
	t.Run("create-instance-get-instance", func(t *testing.T) {
		stopper, storage := setup(t)
		storage.Start()
		defer stopper.Stop(ctx)
		const id = base.SQLInstanceID(1)
		const sessionID = sqlliveness.SessionID("session_id")
		const httpAddr = "http_addr"
		{
			instance, err := storage.CreateInstance(ctx, sessionID, httpAddr)
			require.NoError(t, err)
			require.Equal(t, id, instance.InstanceID())
			require.Equal(t, sessionID, instance.SessionID())
			require.Equal(t, httpAddr, instance.InstanceAddr())
		}
		{
			addr, err := storage.GetInstanceAddr(ctx, id)
			require.NoError(t, err)
			require.Equal(t, httpAddr, addr)
		}
		{
			// Non existent Instance.
			nonExistentID := base.SQLInstanceID(2)
			addr, err := storage.GetInstanceAddr(ctx, nonExistentID)
			require.Errorf(t, err, "could not fetch instance %d non existent instance id", nonExistentID)
			require.Equal(t, 0, len(addr))
		}
	})
	t.Run("release-Instance-get-all-instances", func(t *testing.T) {
		stopper, storage := setup(t)
		defer stopper.Stop(ctx)
		storage.Start()
		// Create three instances and release one.
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3}
		addresses := [...]string{"addr1", "addr2", "addr3"}
		sessionIDs := [...]sqlliveness.SessionID{"session1", "session2", "session3"}
		{
			for index, addr := range addresses {
				instance, err := storage.CreateInstance(ctx, sessionIDs[index], addr)
				require.NoError(t, err)
				require.Equal(t, instanceIDs[index], instance.InstanceID())
			}
		}

		// Verify all instances are returned
		// for GetAllInstancesForTenant.
		{
			instances, err := storage.GetAllInstancesForTenant(ctx)
			sort.SliceStable(instances, func(idx1, idx2 int) bool {
				return instances[idx1].InstanceID() < instances[idx2].InstanceID()
			})
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs), len(instances))
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index], instance.InstanceID())
				require.Equal(t, sessionIDs[index].String(), instance.SessionID().String())
				require.Equal(t, addresses[index], instance.InstanceAddr())
			}
		}

		// Release an Instance and verify only active instances
		// are returned by GetAllInstancesForTenant.
		{
			require.NoError(t, storage.ReleaseInstanceID(ctx, instanceIDs[0]))
			instances, err := storage.GetAllInstancesForTenant(ctx)
			require.NoError(t, err)
			require.Equal(t, 2, len(instances))
			sortInstances(instances)
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index+1], instance.InstanceID())
				require.Equal(t, sessionIDs[index+1].String(), instance.SessionID().String())
				require.Equal(t, addresses[index+1], instance.InstanceAddr())
			}
		}

		// Verify released Instance id gets reused.
		{
			var err error
			var instance sqlinstance.SQLInstance
			newSessionID := sqlliveness.SessionID("session4")
			newAddr := "addr4"
			instance, err = storage.CreateInstance(ctx, newSessionID, newAddr)
			require.NoError(t, err)
			require.Equal(t, instanceIDs[0], instance.InstanceID())
			var instances []sqlinstance.SQLInstance
			instances, err = storage.GetAllInstancesForTenant(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, len(instanceIDs), len(instances))
			for index, instance := range instances {
				require.Equal(t, instanceIDs[index], instance.InstanceID())
				if index == 0 {
					require.Equal(t, newSessionID.String(), instance.SessionID().String())
					require.Equal(t, newAddr, instance.InstanceAddr())
					continue
				}
				require.Equal(t, sessionIDs[index].String(), instance.SessionID().String())
				require.Equal(t, addresses[index], instance.InstanceAddr())
			}
		}
	})
}

func TestConcurrentCreateAndRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	dbName := t.Name()
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := strings.Replace(systemschema.SQLInstancesTableSchema,
		`CREATE TABLE system.sql_instances`,
		`CREATE TABLE "`+dbName+`".sql_instances`, 1)
	tDB.Exec(t, schema)
	tableID := getTableID(t, tDB, dbName, "sql_instances")
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	storage := instancestorage.NewTestingStorage(stopper, kvDB, keys.SystemSQLCodec, tableID)
	storage.Start()

	const (
		runsPerWorker   = 5
		workers         = 5
		controllerSteps = 5
		sessionID       = sqlliveness.SessionID("session")
		httpAddr        = "http_addr"
	)

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
			instance, err := storage.CreateInstance(ctx, sessionID, httpAddr)
			require.NoError(t, err)
			if len(state.freeInstances) > 0 {
				_, free := state.freeInstances[instance.InstanceID()]
				// Confirm that a free id was repurposed.
				require.True(t, free)
				delete(state.freeInstances, instance.InstanceID())
			}
			state.liveInstances[instance.InstanceID()] = struct{}{}
			if instance.InstanceID() > state.maxInstanceID {
				state.maxInstanceID = instance.InstanceID()
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

		// checkGetInstance verifies that GetInstance returns the Instance
		// details when the Instance is live and error otherwise.
		checkGetInstance = func(t *testing.T, i base.SQLInstanceID) {
			t.Helper()
			state.RLock()
			defer state.RUnlock()
			addr, err := storage.GetInstanceAddr(ctx, i)
			if _, free := state.freeInstances[i]; free {
				require.Error(t, err)
				require.Equal(t, 0, len(addr))
			} else {
				require.NoError(t, err)
				require.Equal(t, httpAddr, addr)
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

func sortInstances(instances []sqlinstance.SQLInstance) {
	sort.SliceStable(instances, func(idx1, idx2 int) bool {
		return instances[idx1].InstanceID() < instances[idx2].InstanceID()
	})
}
