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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeSession() sqlliveness.SessionID {
	session, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
	if err != nil {
		panic(err)
	}
	return session
}

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
		clock := hlc.NewClock(timeutil.NewTestTimeSource(), base.DefaultMaxClockOffset)
		stopper := stop.NewStopper()
		slStorage := slstorage.NewFakeStorage()
		storage := instancestorage.NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage, s.ClusterSettings())
		return stopper, storage, slStorage, clock
	}

	const preallocatedCount = 5
	instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)

	t.Run("create-instance-get-instance", func(t *testing.T) {
		stopper, storage, _, clock := setup(t)
		defer stopper.Stop(ctx)
		const id = base.SQLInstanceID(1)
		sessionID := makeSession()
		const addr = "addr"
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		const expiration = time.Minute
		{
			instance, err := storage.CreateInstance(ctx, sessionID, clock.Now().Add(expiration.Nanoseconds(), 0), addr, locality)
			require.NoError(t, err)
			require.Equal(t, id, instance.InstanceID)
		}
	})

	t.Run("release-instance-get-all-instances", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t)
		defer stopper.Stop(ctx)

		// Create three instances and release one.
		region := enum.One
		instanceIDs := [...]base.SQLInstanceID{1, 2, 3, 4, 5}
		addresses := [...]string{"addr1", "addr2", "addr3", "addr4", "addr5"}
		sessionIDs := [...]sqlliveness.SessionID{makeSession(), makeSession(), makeSession(), makeSession(), makeSession()}
		localities := [...]roachpb.Locality{
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region3"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region4"}}},
			{Tiers: []roachpb.Tier{{Key: "region", Value: "region5"}}},
		}
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
		for _, index := range []int{0, 1, 2} {
			instance, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addresses[index], localities[index])
			require.NoError(t, err)
			require.NoError(t, slStorage.Insert(ctx, sessionIDs[index], sessionExpiry))
			require.Equal(t, instanceIDs[index], instance.InstanceID)
		}

		// Verify all instances are returned by GetAllInstancesDataForTest.
		{
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount, len(instances))
			for _, i := range []int{0, 1, 2} {
				require.Equal(t, instanceIDs[i], instances[i].InstanceID)
				require.Equal(t, sessionIDs[i], instances[i].SessionID)
				require.Equal(t, addresses[i], instances[i].InstanceAddr)
				require.Equal(t, localities[i], instances[i].Locality)
			}
			for _, i := range []int{3, 4} {
				require.Equal(t, base.SQLInstanceID(i+1), instances[i].InstanceID)
				require.Empty(t, instances[i].SessionID)
				require.Empty(t, instances[i].InstanceAddr)
				require.Empty(t, instances[i].Locality)
			}
		}

		// Create two more instances.
		for _, index := range []int{3, 4} {
			instance, err := storage.CreateInstance(ctx, sessionIDs[index], sessionExpiry, addresses[index], localities[index])
			require.NoError(t, err)
			require.NoError(t, slStorage.Insert(ctx, sessionIDs[index], sessionExpiry))
			require.Equal(t, instanceIDs[index], instance.InstanceID)
		}

		// Verify all instances are returned by GetAllInstancesDataForTest.
		{
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount, len(instances))
			for i := range instances {
				require.Equal(t, instanceIDs[i], instances[i].InstanceID)
				require.Equal(t, sessionIDs[i], instances[i].SessionID)
				require.Equal(t, addresses[i], instances[i].InstanceAddr)
				require.Equal(t, localities[i], instances[i].Locality)
			}
		}

		// Release an instance and verify all instances are returned.
		{
			require.NoError(t, storage.ReleaseInstanceID(ctx, region, instanceIDs[0]))
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount-1, len(instances))
			sortInstances(instances)
			for i := 0; i < len(instances)-1; i++ {
				require.Equal(t, instanceIDs[i+1], instances[i].InstanceID)
				require.Equal(t, sessionIDs[i+1], instances[i].SessionID)
				require.Equal(t, addresses[i+1], instances[i].InstanceAddr)
				require.Equal(t, localities[i+1], instances[i].Locality)
			}
		}

		// Verify instance ID associated with an expired session gets reused.
		sessionID6 := makeSession()
		addr6 := "addr6"
		locality6 := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "region6"}}}
		{
			require.NoError(t, slStorage.Delete(ctx, sessionIDs[4]))
			require.NoError(t, slStorage.Insert(ctx, sessionID6, sessionExpiry))
			instance, err := storage.CreateInstance(ctx, sessionID6, sessionExpiry, addr6, locality6)
			require.NoError(t, err)
			require.Equal(t, instanceIDs[4], instance.InstanceID)
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount-1, len(instances))
			var foundIDs []base.SQLInstanceID
			for index, instance := range instances {
				foundIDs = append(foundIDs, instance.InstanceID)
				if index == 3 {
					require.Equal(t, sessionID6, instance.SessionID)
					require.Equal(t, addr6, instance.InstanceAddr)
					require.Equal(t, locality6, instance.Locality)
					continue
				}
				require.Equal(t, sessionIDs[index+1], instance.SessionID)
				require.Equal(t, addresses[index+1], instance.InstanceAddr)
				require.Equal(t, localities[index+1], instance.Locality)
			}
			require.Equal(t, []base.SQLInstanceID{2, 3, 4, 5}, foundIDs)
		}

		// Verify released instance ID gets reused.
		{
			newSessionID := makeSession()
			newAddr := "addr7"
			newLocality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "region7"}}}
			instance, err := storage.CreateInstance(ctx, newSessionID, sessionExpiry, newAddr, newLocality)
			require.NoError(t, err)
			require.Equal(t, instanceIDs[0], instance.InstanceID)
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			sortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount+4, len(instances))
			for index := 0; index < len(instanceIDs); index++ {
				require.Equal(t, instanceIDs[index], instances[index].InstanceID)
				switch index {
				case 0:
					require.Equal(t, newSessionID, instances[index].SessionID)
					require.Equal(t, newAddr, instances[index].InstanceAddr)
					require.Equal(t, newLocality, instances[index].Locality)
				case 4:
					require.Equal(t, sessionID6, instances[index].SessionID)
					require.Equal(t, addr6, instances[index].InstanceAddr)
					require.Equal(t, locality6, instances[index].Locality)
				default:
					require.Equal(t, sessionIDs[index], instances[index].SessionID)
					require.Equal(t, addresses[index], instances[index].InstanceAddr)
					require.Equal(t, localities[index], instances[index].Locality)
				}
			}
			for index := len(instanceIDs); index < len(instances); index++ {
				require.Equal(t, base.SQLInstanceID(index+1), instances[index].InstanceID)
				require.Empty(t, instances[index].SessionID)
				require.Empty(t, instances[index].InstanceAddr)
				require.Empty(t, instances[index].Locality)
			}
		}
	})
}

// TestSQLAccess verifies that the sql_instances table is accessible
// through SQL API.
func TestSQLAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	clock := hlc.NewClock(timeutil.NewTestTimeSource(), base.DefaultMaxClockOffset)
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
	storage := instancestorage.NewTestingStorage(
		kvDB, keys.SystemSQLCodec, tableID, slstorage.NewFakeStorage(), s.ClusterSettings())
	const (
		tierStr         = "region=test1,zone=test2"
		expiration      = time.Minute
		expectedNumCols = 4
	)
	var locality roachpb.Locality
	require.NoError(t, locality.Set(tierStr))
	instance, err := storage.CreateInstance(
		ctx,
		makeSession(),
		clock.Now().Add(expiration.Nanoseconds(), 0),
		"addr",
		locality,
	)
	require.NoError(t, err)

	// Query the table through SQL and verify the query completes successfully.
	rows := tDB.Query(t, fmt.Sprintf("SELECT id, addr, session_id, locality FROM \"%s\".sql_instances", dbName))
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, expectedNumCols, len(columns))
	var parsedInstanceID base.SQLInstanceID
	var parsedSessionID gosql.NullString
	var parsedAddr gosql.NullString
	var parsedLocality gosql.NullString
	rows.Next()
	err = rows.Scan(&parsedInstanceID, &parsedAddr, &parsedSessionID, &parsedLocality)
	require.NoError(t, err)
	require.Equal(t, instance.InstanceID, parsedInstanceID)
	require.Equal(t, instance.SessionID, sqlliveness.SessionID(parsedSessionID.String))
	require.Equal(t, instance.InstanceAddr, parsedAddr.String)
	require.Equal(t, instance.Locality, locality)

	// Verify that the remaining entries are preallocated ones.
	i := 2
	for rows.Next() {
		err = rows.Scan(&parsedInstanceID, &parsedAddr, &parsedSessionID, &parsedLocality)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(i), parsedInstanceID)
		require.Empty(t, parsedSessionID.String)
		require.Empty(t, parsedAddr.String)
		require.Empty(t, parsedLocality.String)
		i++
	}
	require.NoError(t, rows.Err())
}

// TestConcurrentCreateAndRelease verifies that concurrent access to instancestorage
// to create and release SQL instance IDs works as expected.
func TestConcurrentCreateAndRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	clock := hlc.NewClock(timeutil.NewTestTimeSource(), base.DefaultMaxClockOffset)
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
	storage := instancestorage.NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage, s.ClusterSettings())
	instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, 1)

	const (
		runsPerWorker   = 100
		workers         = 100
		controllerSteps = 100
		addr            = "addr"
		expiration      = time.Minute
	)
	sessionID := makeSession()
	locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test-region"}}}
	sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)
	err := slStorage.Insert(ctx, sessionID, sessionExpiry)
	if err != nil {
		t.Fatal(err)
	}
	var (
		region = enum.One
		state  = struct {
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
			instance, err := storage.CreateInstance(ctx, sessionID, sessionExpiry, addr, locality)
			require.NoError(t, err)
			if len(state.freeInstances) > 0 {
				_, free := state.freeInstances[instance.InstanceID]
				// Confirm that a free id was repurposed.
				require.True(t, free)
				delete(state.freeInstances, instance.InstanceID)
			}
			state.liveInstances[instance.InstanceID] = struct{}{}
			if instance.InstanceID > state.maxInstanceID {
				state.maxInstanceID = instance.InstanceID
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
			require.NoError(t, storage.ReleaseInstanceID(ctx, region, i))
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
			instanceInfo, err := storage.GetInstanceDataForTest(ctx, region, i)
			if _, free := state.freeInstances[i]; free {
				require.Error(t, err)
				require.ErrorIs(t, err, sqlinstance.NonExistentInstanceError)
			} else {
				require.NoError(t, err)
				require.Equal(t, addr, instanceInfo.InstanceAddr)
				require.Equal(t, sessionID, instanceInfo.SessionID)
				require.Equal(t, locality, instanceInfo.Locality)
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

func TestReclaimLoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	clock := hlc.NewClock(timeutil.NewTestTimeSource(), base.DefaultMaxClockOffset)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	dbName := t.Name()
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := strings.Replace(systemschema.SQLInstancesTableSchema,
		`CREATE TABLE system.sql_instances`,
		`CREATE TABLE "`+dbName+`".sql_instances`, 1)
	tDB.Exec(t, schema)
	tableID := getTableID(t, tDB, dbName, "sql_instances")
	slStorage := slstorage.NewFakeStorage()
	storage := instancestorage.NewTestingStorage(kvDB, keys.SystemSQLCodec, tableID, slStorage, s.ClusterSettings())
	storage.TestingKnobs.JitteredIntervalFn = func(d time.Duration) time.Duration {
		// For deterministic tests.
		return d
	}
	const preallocatedCount = 5
	instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)

	// Use a custom time source for testing.
	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	ts := timeutil.NewManualTime(t0)

	// Expiration < ReclaimLoopInterval.
	const expiration = 5 * time.Hour
	sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

	ief := s.InternalExecutorFactory().(descs.TxnManager)
	err := storage.RunInstanceIDReclaimLoop(ctx, s.Stopper(), ts, ief, func() hlc.Timestamp {
		return sessionExpiry
	})
	require.NoError(t, err)

	reclaimGroupInterval := instancestorage.ReclaimLoopInterval.Get(&s.ClusterSettings().SV)

	// Ensure that no rows initially.
	instances, err := storage.GetAllInstancesDataForTest(ctx)
	require.NoError(t, err)
	require.Empty(t, instances)

	testutils.SucceedsSoon(t, func() error {
		// Wait for timer to be updated.
		if len(ts.Timers()) == 1 && ts.Timers()[0] == ts.Now().Add(reclaimGroupInterval) {
			return nil
		}
		return errors.New("waiting for timer to be updated")
	})

	// Advance the clock, and ensure that more rows are added.
	ts.Advance(reclaimGroupInterval)
	testutils.SucceedsSoon(t, func() error {
		instances, err = storage.GetAllInstancesDataForTest(ctx)
		if err != nil {
			return err
		}
		sortInstances(instances)
		if len(instances) == 0 {
			return errors.New("instances have not been generated yet")
		}
		return nil
	})

	require.Equal(t, preallocatedCount, len(instances))
	for id, instance := range instances {
		require.Equal(t, base.SQLInstanceID(id+1), instance.InstanceID)
		require.Empty(t, instance.InstanceAddr)
		require.Empty(t, instance.SessionID)
		require.Empty(t, instance.Locality)
	}

	// Consume two rows.
	region := enum.One
	instanceIDs := [...]base.SQLInstanceID{1, 2}
	addresses := [...]string{"addr1", "addr2"}
	sessionIDs := [...]sqlliveness.SessionID{makeSession(), makeSession()}
	localities := [...]roachpb.Locality{
		{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
		{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
	}
	for i, id := range instanceIDs {
		require.NoError(t, slStorage.Insert(ctx, sessionIDs[i], sessionExpiry))
		require.NoError(t, storage.CreateInstanceDataForTest(
			ctx,
			region,
			id,
			addresses[i],
			sessionIDs[i],
			sessionExpiry,
			localities[i],
		))
	}

	testutils.SucceedsSoon(t, func() error {
		// Wait for timer to be updated.
		if len(ts.Timers()) == 1 && ts.Timers()[0] == ts.Now().Add(reclaimGroupInterval) {
			return nil
		}
		return errors.New("waiting for timer to be updated")
	})

	// Advance the clock, and ensure that more rows are added.
	ts.Advance(reclaimGroupInterval)
	testutils.SucceedsSoon(t, func() error {
		instances, err = storage.GetAllInstancesDataForTest(ctx)
		if err != nil {
			return err
		}
		sortInstances(instances)
		if len(instances) == preallocatedCount {
			return errors.New("new instances have not been generated yet")
		}
		return nil
	})

	require.Equal(t, preallocatedCount+2, len(instances))
	for i, instance := range instances {
		require.Equal(t, base.SQLInstanceID(i+1), instance.InstanceID)
		switch i {
		case 0, 1:
			require.Equal(t, addresses[i], instance.InstanceAddr)
			require.Equal(t, sessionIDs[i], instance.SessionID)
			require.Equal(t, localities[i], instance.Locality)
		default:
			require.Empty(t, instance.InstanceAddr)
			require.Empty(t, instance.SessionID)
			require.Empty(t, instance.Locality)
		}
	}
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
