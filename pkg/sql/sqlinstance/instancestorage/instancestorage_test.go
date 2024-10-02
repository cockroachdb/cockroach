// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/sqllivenesstestutils"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeSession() *sqllivenesstestutils.FakeSession {
	sessionID, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
	if err != nil {
		panic(err)
	}
	return &sqllivenesstestutils.FakeSession{SessionID: sessionID}
}

// TestStorage verifies that instancestorage stores and retrieves SQL instance data correctly.
// Also, it verifies that released instance IDs are correctly updated within the database
// and reused for new SQL instances.
func TestStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	setup := func(t *testing.T) (
		*stop.Stopper, *instancestorage.Storage, *slstorage.FakeStorage, *hlc.Clock,
	) {
		dbName := t.Name()
		tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
		schema := instancestorage.GetTableSQLForDatabase(dbName)
		tDB.Exec(t, schema)
		table := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), dbName, "sql_instances")
		clock := hlc.NewClockForTesting(nil)
		stopper := stop.NewStopper()
		slStorage := slstorage.NewFakeStorage()
		f := s.RangeFeedFactory().(*rangefeed.Factory)
		storage := instancestorage.NewTestingStorage(kvDB, s.Codec(), table, slStorage, s.ClusterSettings(), s.Clock(), f, s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
		return stopper, storage, slStorage, clock
	}

	const preallocatedCount = 5
	instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, preallocatedCount)

	t.Run("create-instance-get-instance", func(t *testing.T) {
		stopper, storage, _, clock := setup(t)
		defer stopper.Stop(ctx)
		const id = base.SQLInstanceID(1)
		session := makeSession()
		const rpcAddr = "rpcAddr"
		const sqlAddr = "sqlAddr"
		locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test"}, {Key: "az", Value: "a"}}}
		binaryVersion := roachpb.Version{Major: 28, Minor: 4}
		const expiration = time.Minute
		{
			session.StartTS = clock.Now()
			session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
			instance, err := storage.CreateInstance(ctx, session, rpcAddr, sqlAddr, locality, binaryVersion)
			require.NoError(t, err)
			require.Equal(t, id, instance.InstanceID)
		}
	})

	t.Run("release-instance-get-all-instances", func(t *testing.T) {
		const expiration = time.Minute
		stopper, storage, slStorage, clock := setup(t)
		defer stopper.Stop(ctx)

		sessionStart := clock.Now()
		sessionExpiry := clock.Now().Add(expiration.Nanoseconds(), 0)

		makeInstance := func(id int) sqlinstance.InstanceInfo {
			return sqlinstance.InstanceInfo{
				Region:          enum.One,
				InstanceID:      base.SQLInstanceID(id),
				InstanceSQLAddr: fmt.Sprintf("sql-addr-%d", id),
				InstanceRPCAddr: fmt.Sprintf("rpc-addr-%d", id),
				SessionID:       makeSession().ID(),
				Locality:        roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: fmt.Sprintf("region-%d", id)}}},
				BinaryVersion:   roachpb.Version{Major: 22, Minor: int32(id)},
			}
		}

		createInstance := func(t *testing.T, instance sqlinstance.InstanceInfo) {
			t.Helper()

			alive, err := slStorage.IsAlive(ctx, instance.SessionID)
			require.NoError(t, err)
			if !alive {
				require.NoError(t, slStorage.Insert(ctx, instance.SessionID, sessionExpiry))
			}

			session := &sqllivenesstestutils.FakeSession{SessionID: instance.SessionID,
				StartTS: sessionStart,
				ExpTS:   sessionExpiry}
			created, err := storage.CreateInstance(ctx, session, instance.InstanceRPCAddr, instance.InstanceSQLAddr, instance.Locality, instance.BinaryVersion)
			require.NoError(t, err)

			require.Equal(t, instance, created)
		}

		equalInstance := func(t *testing.T, expect sqlinstance.InstanceInfo, actual sqlinstance.InstanceInfo) {
			require.Equal(t, expect.InstanceID, actual.InstanceID)
			require.Equal(t, expect.SessionID, actual.SessionID)
			require.Equal(t, expect.InstanceRPCAddr, actual.InstanceRPCAddr)
			require.Equal(t, expect.InstanceSQLAddr, actual.InstanceSQLAddr)
			require.Equal(t, expect.Locality, actual.Locality)
			require.Equal(t, expect.BinaryVersion, actual.BinaryVersion)
			require.Equal(t, expect.IsDraining, actual.IsDraining)
		}

		isAvailable := func(t *testing.T, instance sqlinstance.InstanceInfo, id base.SQLInstanceID) {
			require.Equal(t, sqlinstance.InstanceInfo{InstanceID: id, Region: enum.One}, instance)
		}

		var initialInstances []sqlinstance.InstanceInfo
		for i := 1; i <= 5; i++ {
			initialInstances = append(initialInstances, makeInstance(i))
		}

		// Create three instances and release one.
		for _, instance := range initialInstances[:3] {
			createInstance(t, instance)
		}

		// Verify all instances are returned by GetAllInstancesDataForTest.
		{
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			instancestorage.SortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount, len(instances))
			for _, i := range []int{0, 1, 2} {
				equalInstance(t, initialInstances[i], instances[i])
			}
			for _, i := range []int{3, 4} {
				isAvailable(t, instances[i], initialInstances[i].InstanceID)
			}
		}

		// Create two more instances.
		for _, instance := range initialInstances[3:] {
			createInstance(t, instance)
		}

		// Verify all instances are returned by GetAllInstancesDataForTest.
		{
			instances, err := storage.GetAllInstancesDataForTest(ctx)
			instancestorage.SortInstances(instances)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount, len(instances))
			for i := range instances {
				equalInstance(t, initialInstances[i], instances[i])
			}
		}

		// Release an instance and verify the instance is available.
		{
			toRelease := initialInstances[0]

			// Call ReleaseInstance twice to ensure it is idempotent.
			require.NoError(t, storage.ReleaseInstance(ctx, toRelease.SessionID, toRelease.InstanceID))
			require.NoError(t, storage.ReleaseInstance(ctx, toRelease.SessionID, toRelease.InstanceID))

			instances, err := storage.GetAllInstancesDataForTest(ctx)
			require.NoError(t, err)
			require.Equal(t, preallocatedCount, len(instances))
			instancestorage.SortInstances(instances)

			for i, instance := range instances {
				if i == 0 {
					isAvailable(t, instance, toRelease.InstanceID)
				} else {
					equalInstance(t, initialInstances[i], instance)
				}
			}

			// Re-allocate the instance.
			createInstance(t, initialInstances[0])
		}

		// Verify instance ID associated with an expired session gets reused.
		newInstance5 := makeInstance(1337)
		newInstance5.InstanceID = initialInstances[4].InstanceID
		{
			require.NoError(t, slStorage.Delete(ctx, initialInstances[4].SessionID))

			createInstance(t, newInstance5)

			instances, err := storage.GetAllInstancesDataForTest(ctx)
			require.NoError(t, err)
			instancestorage.SortInstances(instances)

			require.Equal(t, len(initialInstances), len(instances))
			for index, instance := range instances {
				expect := initialInstances[index]
				if index == 4 {
					expect = newInstance5
				}
				equalInstance(t, expect, instance)
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	clock := hlc.NewClockForTesting(nil)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	dbName := t.Name()
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := instancestorage.GetTableSQLForDatabase(dbName)
	tDB.Exec(t, schema)
	table := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), dbName, "sql_instances")
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	f := s.RangeFeedFactory().(*rangefeed.Factory)
	storage := instancestorage.NewTestingStorage(
		kvDB, s.Codec(), table, slstorage.NewFakeStorage(), s.ClusterSettings(), s.Clock(), f, s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
	const (
		tierStr         = "region=test1,zone=test2"
		expiration      = time.Minute
		expectedNumCols = 5
	)
	var locality roachpb.Locality
	var binaryVersion roachpb.Version
	require.NoError(t, locality.Set(tierStr))
	session := makeSession()
	session.StartTS = clock.Now()
	session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
	instance, err := storage.CreateInstance(
		ctx,
		session,
		"rpcAddr",
		"sqlAddr",
		locality,
		binaryVersion,
	)
	require.NoError(t, err)

	// Query the table through SQL and verify the query completes successfully.
	rows := tDB.Query(t, fmt.Sprintf("SELECT id, addr, sql_addr, session_id, locality FROM \"%s\".sql_instances", dbName))
	defer rows.Close()
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.Equal(t, expectedNumCols, len(columns))
	var parsedInstanceID base.SQLInstanceID
	var parsedSessionID gosql.NullString
	var parsedAddr gosql.NullString
	var parsedSqlAddr gosql.NullString
	var parsedLocality gosql.NullString
	if !assert.True(t, rows.Next()) {
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	err = rows.Scan(&parsedInstanceID, &parsedAddr, &parsedSqlAddr, &parsedSessionID, &parsedLocality)
	require.NoError(t, err)
	require.Equal(t, instance.InstanceID, parsedInstanceID)
	require.Equal(t, instance.SessionID, sqlliveness.SessionID(parsedSessionID.String))
	require.Equal(t, instance.InstanceRPCAddr, parsedAddr.String)
	require.Equal(t, instance.InstanceSQLAddr, parsedSqlAddr.String)
	require.Equal(t, instance.Locality, locality)

	// Verify that the remaining entries are preallocated ones.
	i := 2
	for rows.Next() {
		err = rows.Scan(&parsedInstanceID, &parsedAddr, &parsedSqlAddr, &parsedSessionID, &parsedLocality)
		require.NoError(t, err)
		require.Equal(t, base.SQLInstanceID(i), parsedInstanceID)
		require.Empty(t, parsedSessionID.String)
		require.Empty(t, parsedAddr.String)
		require.Empty(t, parsedSqlAddr.String)
		require.Empty(t, parsedLocality.String)
		i++
	}
	require.NoError(t, rows.Err())
}

func TestRefreshSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109410),
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "abc", Value: "xyz"},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	c1 := sqlutils.MakeSQLRunner(sqlDB)

	// Everything but the session should stay the same so observe the initial row.
	rowBeforeNoSession := c1.QueryStr(t, "SELECT id, addr, sql_addr, locality FROM system.sql_instances WHERE id = 1")
	require.Len(t, rowBeforeNoSession, 1)

	// This initial session should go away once we expire it below, but let's
	// verify it is there for starters and remember it.
	sess := c1.QueryStr(t, "SELECT encode(session_id, 'hex') FROM system.sql_instances WHERE id = 1")
	require.Len(t, sess, 1)
	require.Len(t, sess[0][0], 38)

	// First let's delete the instance AND expire the session; the instance should
	// reappear when a new session is acquired, with the new session.
	c1.ExecRowsAffected(t, 1, "DELETE FROM system.sql_instances WHERE session_id = decode($1, 'hex')", sess[0][0])
	c1.ExecRowsAffected(t, 1, "DELETE FROM system.sqlliveness WHERE session_id = decode($1, 'hex')", sess[0][0])

	// Wait until we see the right row appear.
	query := fmt.Sprintf(`SELECT count(*) FROM system.sql_instances WHERE id = 1 AND session_id <> decode('%s', 'hex')`, sess[0][0])
	c1.CheckQueryResultsRetry(t, query, [][]string{{"1"}})

	// Verify that everything else is the same after recreate.
	c1.CheckQueryResults(t, "SELECT id, addr, sql_addr, locality FROM system.sql_instances WHERE id = 1", rowBeforeNoSession)

	sess = c1.QueryStr(t, "SELECT encode(session_id, 'hex') FROM system.sql_instances WHERE id = 1")
	// Now let's just expire the session and leave the row; the instance row
	// should still become correct once it is updated with the new session.
	c1.ExecRowsAffected(t, 1, "DELETE FROM system.sqlliveness WHERE session_id = decode($1, 'hex')", sess[0][0])

	// Wait until we see the right row appear.
	query = fmt.Sprintf(`SELECT count(*) FROM system.sql_instances WHERE id = 1 AND session_id <> decode('%s', 'hex')`, sess[0][0])
	c1.CheckQueryResultsRetry(t, query, [][]string{{"1"}})

	// Verify everything else is still the same after update.
	c1.CheckQueryResults(t, "SELECT id, addr, sql_addr, locality FROM system.sql_instances WHERE id = 1", rowBeforeNoSession)

}

// TestConcurrentCreateAndRelease verifies that concurrent access to instancestorage
// to create and release SQL instance IDs works as expected.
func TestConcurrentCreateAndRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	clock := hlc.NewClockForTesting(nil)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	dbName := t.Name()
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := instancestorage.GetTableSQLForDatabase(dbName)
	tDB.Exec(t, schema)
	table := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), dbName, "sql_instances")
	stopper := stop.NewStopper()
	slStorage := slstorage.NewFakeStorage()
	defer stopper.Stop(ctx)
	f := s.RangeFeedFactory().(*rangefeed.Factory)
	storage := instancestorage.NewTestingStorage(kvDB, s.Codec(), table, slStorage, s.ClusterSettings(), s.Clock(), f, s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
	instancestorage.PreallocatedCount.Override(ctx, &s.ClusterSettings().SV, 1)

	const (
		runsPerWorker   = 100
		workers         = 100
		controllerSteps = 100
		rpcAddr         = "rpcAddr"
		sqlAddr         = "sqlAddr"
		expiration      = time.Minute
	)
	session := makeSession()
	session.StartTS = clock.Now()
	session.ExpTS = session.StartTS.Add(expiration.Nanoseconds(), 0)
	locality := roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "test-region"}}}
	binaryVersion := roachpb.Version{Major: 23, Minor: 4}
	err := slStorage.Insert(ctx, session.ID(), session.Expiration())
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
			session.ExpTS = clock.Now().Add(expiration.Nanoseconds(), 0)
			_, _, err = slStorage.Update(ctx, session.ID(), session.Expiration())
			if err != nil {
				t.Fatal(err)
			}
			instance, err := storage.CreateInstance(ctx, session, rpcAddr, sqlAddr, locality, binaryVersion)
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
			require.NoError(t, storage.ReleaseInstance(ctx, session.ID(), i))
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
			require.NoError(t, err)
			if _, free := state.freeInstances[i]; free {
				require.Empty(t, instanceInfo.InstanceRPCAddr)
				require.Empty(t, instanceInfo.InstanceSQLAddr)
				require.Empty(t, instanceInfo.SessionID)
				require.Empty(t, instanceInfo.Locality)
				require.Empty(t, instanceInfo.BinaryVersion)
			} else {
				require.Equal(t, rpcAddr, instanceInfo.InstanceRPCAddr)
				require.Equal(t, sqlAddr, instanceInfo.InstanceSQLAddr)
				require.Equal(t, session.ID(), instanceInfo.SessionID)
				require.Equal(t, locality, instanceInfo.Locality)
				require.Equal(t, binaryVersion, instanceInfo.BinaryVersion)
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
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	clock := hlc.NewClockForTesting(nil)
	tDB := sqlutils.MakeSQLRunner(sqlDB)
	dbName := t.Name()
	tDB.Exec(t, `CREATE DATABASE "`+dbName+`"`)
	schema := instancestorage.GetTableSQLForDatabase(dbName)
	tDB.Exec(t, schema)
	tableID := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), dbName, "sql_instances")
	slStorage := slstorage.NewFakeStorage()
	f := s.RangeFeedFactory().(*rangefeed.Factory)
	storage := instancestorage.NewTestingStorage(kvDB, s.Codec(), tableID, slStorage, s.ClusterSettings(), s.Clock(), f, s.SettingsWatcher().(*settingswatcher.SettingsWatcher))
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
	sessionStart := clock.Now()
	sessionExpiry := sessionStart.Add(expiration.Nanoseconds(), 0)

	db := s.InternalDB().(descs.DB)
	err := storage.RunInstanceIDReclaimLoop(ctx, s.AppStopper(), ts, db, func() hlc.Timestamp {
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
		instancestorage.SortInstances(instances)
		if len(instances) == 0 {
			return errors.New("instances have not been generated yet")
		}
		return nil
	})

	require.Equal(t, preallocatedCount, len(instances))
	for id, instance := range instances {
		require.Equal(t, base.SQLInstanceID(id+1), instance.InstanceID)
		require.Empty(t, instance.InstanceRPCAddr)
		require.Empty(t, instance.InstanceSQLAddr)
		require.Empty(t, instance.SessionID)
		require.Empty(t, instance.Locality)
		require.Empty(t, instance.BinaryVersion)
	}

	// Consume two rows.
	region := enum.One
	instanceIDs := [...]base.SQLInstanceID{1, 2}
	rpcAddresses := [...]string{"addr1", "addr2"}
	sqlAddresses := [...]string{"addr3", "addr4"}
	sessionIDs := [...]*sqllivenesstestutils.FakeSession{makeSession(), makeSession()}
	for _, id := range sessionIDs {
		id.StartTS = sessionStart
		id.ExpTS = sessionExpiry
	}
	localities := [...]roachpb.Locality{
		{Tiers: []roachpb.Tier{{Key: "region", Value: "region1"}}},
		{Tiers: []roachpb.Tier{{Key: "region", Value: "region2"}}},
	}
	binaryVersions := []roachpb.Version{
		{Major: 22, Minor: 2}, {Major: 23, Minor: 1},
	}

	for i, id := range instanceIDs {
		require.NoError(t, slStorage.Insert(ctx, sessionIDs[i].ID(), sessionExpiry))
		require.NoError(t, storage.CreateInstanceDataForTest(
			ctx,
			region,
			id,
			rpcAddresses[i],
			sqlAddresses[i],
			sessionIDs[i].ID(),
			sessionExpiry,
			localities[i],
			binaryVersions[i],
			/* encodeIsDraining */ true,
			/* isDraining */ false,
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
		instancestorage.SortInstances(instances)
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
			require.Equal(t, rpcAddresses[i], instance.InstanceRPCAddr)
			require.Equal(t, sqlAddresses[i], instance.InstanceSQLAddr)
			require.Equal(t, sessionIDs[i].ID(), instance.SessionID)
			require.Equal(t, localities[i], instance.Locality)
			require.Equal(t, binaryVersions[i], instance.BinaryVersion)
		default:
			require.Empty(t, instance.InstanceRPCAddr)
			require.Empty(t, instance.InstanceSQLAddr)
			require.Empty(t, instance.SessionID)
			require.Empty(t, instance.Locality)
			require.Empty(t, instance.BinaryVersion)
		}
	}
}
