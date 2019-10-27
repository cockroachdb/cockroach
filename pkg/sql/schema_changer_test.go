// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// asyncSchemaChangerDisabled can be used to disable asynchronous processing
// of schema changes.
func asyncSchemaChangerDisabled() error {
	return errors.New("async schema changer disabled")
}

func TestSchemaChangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	jobRegistry := s.JobRegistry().(*jobs.Registry)

	const dbDescID = keys.MinNonPredefinedUserDescID
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	var leaseDurationString string
	sqlRun.QueryRow(t, `SHOW CLUSTER SETTING schemachanger.lease.duration`).Scan(&leaseDurationString)
	leaseInterval, err := tree.ParseDInterval(leaseDurationString)
	if err != nil {
		t.Fatal(err)
	}
	leaseDuration := time.Duration(leaseInterval.Duration.Nanos())
	sqlRun.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`)

	var lease sqlbase.TableDescriptor_SchemaChangeLease
	var id = sqlbase.ID(dbDescID + 1)
	var node = roachpb.NodeID(2)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	cs := cluster.MakeTestingClusterSettings()
	u := cs.MakeUpdater()
	// Set to always expire the lease.
	if err := u.Set("schemachanger.lease.renew_fraction", "2.0", "f"); err != nil {
		t.Fatal(err)
	}
	changer := sql.NewSchemaChangerForTesting(
		id, 0, node, *kvDB, nil, jobRegistry,
		&execCfg, cs)

	ctx := context.TODO()

	// Acquire a lease.
	lease, err = changer.AcquireLease(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime, leaseDuration) {
		t.Fatalf("invalid expiration time: %s. now: %s",
			timeutil.Unix(0, lease.ExpirationTime), timeutil.Now())
	}

	// Acquiring another lease will fail.
	if _, err := changer.AcquireLease(ctx); !testutils.IsError(
		err, "an outstanding schema change lease exists",
	) {
		t.Fatal(err)
	}

	// Extend the lease.
	oldLease := lease
	if err := changer.ExtendLease(ctx, &lease); err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime, leaseDuration) {
		t.Fatalf("invalid expiration time: %s", timeutil.Unix(0, lease.ExpirationTime))
	}

	// The new lease is a brand new lease.
	if oldLease == lease {
		t.Fatalf("lease was not extended: %v", lease)
	}

	// Extending an old lease fails.
	if err := changer.ExtendLease(ctx, &oldLease); !testutils.IsError(
		err, "the schema change lease has expired") {
		t.Fatal(err)
	}

	// Releasing an old lease fails.
	if err := changer.ReleaseLease(ctx, oldLease); err == nil {
		t.Fatal("releasing a old lease succeeded")
	}

	// Release lease.
	if err := changer.ReleaseLease(ctx, lease); err != nil {
		t.Fatal(err)
	}

	// Extending the lease fails.
	if err := changer.ExtendLease(ctx, &lease); err == nil {
		t.Fatalf("was able to extend an already released lease: %d, %v", id, lease)
	}

	// acquiring the lease succeeds
	lease, err = changer.AcquireLease(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Reset to not expire the lease.
	if err := u.Set("schemachanger.lease.renew_fraction", "0.4", "f"); err != nil {
		t.Fatal(err)
	}
	oldLease = lease
	if err := changer.ExtendLease(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	// The old lease is renewed.
	if oldLease != lease {
		t.Fatalf("acquired new lease: %v, old lease: %v", lease, oldLease)
	}
}

func validExpirationTime(expirationTime int64, leaseDuration time.Duration) bool {
	now := timeutil.Now()
	return expirationTime > now.Add(leaseDuration/2).UnixNano() && expirationTime < now.Add(leaseDuration*3/2).UnixNano()
}

func TestSchemaChangeProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer sql.TestDisableTableLeases()()

	params, _ := tests.CreateTestServerParams()
	// Disable external processing of mutations.
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	var id = sqlbase.ID(keys.MinNonPredefinedUserDescID + 1 /* skip over DB ID */)
	var node = roachpb.NodeID(2)
	stopper := stop.NewStopper()
	cfg := base.NewLeaseManagerConfig()
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	leaseMgr := sql.NewLeaseManager(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		execCfg.NodeID,
		execCfg.DB,
		execCfg.Clock,
		execCfg.InternalExecutor,
		execCfg.Settings,
		sql.LeaseManagerTestingKnobs{},
		stopper,
		cfg,
	)
	jobRegistry := s.JobRegistry().(*jobs.Registry)
	defer stopper.Stop(context.TODO())
	changer := sql.NewSchemaChangerForTesting(
		id, 0, node, *kvDB, leaseMgr, jobRegistry, &execCfg, cluster.MakeTestingClusterSettings())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo(v));
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	expectedVersion := tableDesc.Version
	ctx := context.TODO()

	// Check that RunStateMachineBeforeBackfill doesn't do anything
	// if there are no mutations queued.
	if err := changer.RunStateMachineBeforeBackfill(ctx); err != nil {
		t.Fatal(err)
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Check that RunStateMachineBeforeBackfill functions properly.
	expectedVersion = tableDesc.Version
	// Make a copy of the index for use in a mutation.
	index := protoutil.Clone(&tableDesc.Indexes[0]).(*sqlbase.IndexDescriptor)
	index.Name = "bar"
	index.ID = tableDesc.NextIndexID
	tableDesc.NextIndexID++
	changer = sql.NewSchemaChangerForTesting(
		id, tableDesc.NextMutationID, node, *kvDB, leaseMgr, jobRegistry,
		&execCfg, cluster.MakeTestingClusterSettings(),
	)
	tableDesc.Mutations = append(tableDesc.Mutations, sqlbase.DescriptorMutation{
		Descriptor_: &sqlbase.DescriptorMutation_Index{Index: index},
		Direction:   sqlbase.DescriptorMutation_ADD,
		State:       sqlbase.DescriptorMutation_DELETE_ONLY,
		MutationID:  tableDesc.NextMutationID,
	})
	tableDesc.NextMutationID++

	// Run state machine in both directions.
	for _, direction := range []sqlbase.DescriptorMutation_Direction{sqlbase.DescriptorMutation_ADD, sqlbase.DescriptorMutation_DROP} {
		tableDesc.Mutations[0].Direction = direction
		expectedVersion++
		if err := kvDB.Put(
			ctx,
			sqlbase.MakeDescMetadataKey(tableDesc.ID),
			sqlbase.WrapDescriptor(tableDesc),
		); err != nil {
			t.Fatal(err)
		}
		// The expected end state.
		expectedState := sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY
		if direction == sqlbase.DescriptorMutation_DROP {
			expectedState = sqlbase.DescriptorMutation_DELETE_ONLY
		}
		// Run two times to ensure idempotency of operations.
		for i := 0; i < 2; i++ {
			if err := changer.RunStateMachineBeforeBackfill(ctx); err != nil {
				t.Fatal(err)
			}

			tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
			newVersion = tableDesc.Version
			if newVersion != expectedVersion {
				t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
			}
			state := tableDesc.Mutations[0].State
			if state != expectedState {
				t.Fatalf("bad state; e = %d, v = %d", expectedState, state)
			}
		}
	}
	// RunStateMachineBeforeBackfill() doesn't complete the schema change.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if len(tableDesc.Mutations) == 0 {
		t.Fatalf("table expected to have an outstanding schema change: %v", tableDesc)
	}
}

func TestAsyncSchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer sql.TestDisableTableLeases()()
	// Disable synchronous schema change execution so the asynchronous schema
	// changer executes all schema changes.
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// A long running schema change operation runs through
	// a state machine that increments the version by 3.
	expectedVersion := tableDesc.Version + 3

	// Run some schema change
	if _, err := sqlDB.Exec(`
CREATE INDEX foo ON t.test (v)
`); err != nil {
		t.Fatal(err)
	}

	retryOpts := retry.Options{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
		Multiplier:     2,
	}

	// Wait until index is created.
	for r := retry.Start(retryOpts); r.Next(); {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.Indexes) == 1 {
			break
		}
	}

	// Ensure that the indexes have been created.
	mTest := makeMutationTest(t, kvDB, sqlDB, tableDesc)
	indexQuery := `SELECT v FROM t.test@foo`
	mTest.CheckQueryResults(t, indexQuery, [][]string{{"b"}, {"d"}})

	// Ensure that the version has been incremented.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Apply a schema change that only sets the UpVersion bit.
	expectedVersion = newVersion + 1

	mTest.Exec(t, `ALTER INDEX t.test@foo RENAME TO ufo`)

	for r := retry.Start(retryOpts); r.Next(); {
		// Ensure that the version gets incremented.
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		name := tableDesc.Indexes[0].Name
		if name != "ufo" {
			t.Fatalf("bad index name %s", name)
		}
		newVersion = tableDesc.Version
		if newVersion == expectedVersion {
			break
		}
	}

	// Run many schema changes simultaneously and check
	// that they all get executed.
	count := 5
	for i := 0; i < count; i++ {
		mTest.Exec(t, fmt.Sprintf(`CREATE INDEX foo%d ON t.test (v)`, i))
	}
	// Wait until indexes are created.
	for r := retry.Start(retryOpts); r.Next(); {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.Indexes) == count+1 {
			break
		}
	}
	for i := 0; i < count; i++ {
		indexQuery := fmt.Sprintf(`SELECT v FROM t.test@foo%d`, i)
		mTest.CheckQueryResults(t, indexQuery, [][]string{{"b"}, {"d"}})
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

func getTableKeyCount(ctx context.Context, kvDB *client.DB) (int, error) {
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0)
	return len(kvs), err
}

func checkTableKeyCountExact(ctx context.Context, kvDB *client.DB, e int) error {
	if count, err := getTableKeyCount(ctx, kvDB); err != nil {
		return err
	} else if count != e {
		return errors.Errorf("expected %d key value pairs, but got %d", e, count)
	}
	return nil
}

// checkTableKeyCount returns the number of KVs in the DB, the multiple should be the
// number of columns.
func checkTableKeyCount(ctx context.Context, kvDB *client.DB, multiple int, maxValue int) error {
	return checkTableKeyCountExact(ctx, kvDB, multiple*(maxValue+1))
}

// Run a particular schema change and run some OLTP operations in parallel, as
// soon as the schema change starts executing its backfill.
func runSchemaChangeWithOperations(
	t *testing.T,
	sqlDB *gosql.DB,
	kvDB *client.DB,
	jobRegistry *jobs.Registry,
	schemaChange string,
	maxValue int,
	keyMultiple int,
	backfillNotification chan struct{},
	execCfg *sql.ExecutorConfig,
) {
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	// Run the schema change in a separate goroutine.

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		start := timeutil.Now()
		// Start schema change that eventually runs a backfill.
		if _, err := sqlDB.Exec(schemaChange); err != nil {
			t.Error(err)
		}
		t.Logf("schema change %s took %v", schemaChange, timeutil.Since(start))
		wg.Done()
	}()

	// Wait until the schema change backfill starts.
	<-backfillNotification

	// Run a variety of operations during the backfill.
	ctx := context.TODO()

	// Grabbing a schema change lease on the table will fail, disallowing
	// another schema change from being simultaneously executed.
	sc := sql.NewSchemaChangerForTesting(tableDesc.ID, 0, 0, *kvDB, nil, jobRegistry, execCfg, cluster.MakeTestingClusterSettings())
	if l, err := sc.AcquireLease(ctx); err == nil {
		t.Fatalf("schema change lease acquisition on table %d succeeded: %v", tableDesc.ID, l)
	}

	// Update some rows.
	var updatedKeys []int
	for i := 0; i < 10; i++ {
		k := rand.Intn(maxValue)
		v := maxValue + i + 1
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, v, k); err != nil {
			t.Error(err)
		}
		updatedKeys = append(updatedKeys, k)
	}

	// Reupdate updated values back to what they were before.
	for _, k := range updatedKeys {
		if rand.Float32() < 0.5 {
			if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, maxValue-k, k); err != nil {
				t.Error(err)
			}
		} else {
			if _, err := sqlDB.Exec(`UPSERT INTO t.test (k,v) VALUES ($1, $2)`, k, maxValue-k); err != nil {
				t.Error(err)
			}
		}
	}

	// Delete some rows.
	deleteStartKey := rand.Intn(maxValue - 10)
	for i := 0; i < 10; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, deleteStartKey+i); err != nil {
			t.Error(err)
		}
	}
	// Reinsert deleted rows.
	for i := 0; i < 10; i++ {
		k := deleteStartKey + i
		if rand.Float32() < 0.5 {
			if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, k, maxValue-k); err != nil {
				t.Error(err)
			}
		} else {
			if _, err := sqlDB.Exec(`UPSERT INTO t.test VALUES($1, $2)`, k, maxValue-k); err != nil {
				t.Error(err)
			}
		}
	}

	// Insert some new rows.
	numInserts := 10
	for i := 0; i < numInserts; i++ {
		k := maxValue + i + 1
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $1)`, k); err != nil {
			t.Error(err)
		}
	}

	wg.Wait() // for schema change to complete.

	// Verify the number of keys left behind in the table to validate schema
	// change operations.
	if err := checkTableKeyCount(ctx, kvDB, keyMultiple, maxValue+numInserts); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Delete the rows inserted.
	for i := 0; i < numInserts; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, maxValue+i+1); err != nil {
			t.Error(err)
		}
	}
}

// bulkInsertIntoTable fills up table t.test with (maxValue + 1) rows.
func bulkInsertIntoTable(sqlDB *gosql.DB, maxValue int) error {
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, maxValue-i)
	}
	_, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ","))
	return err
}

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestRaceWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// protects backfillNotification
	var mu syncutil.Mutex
	var backfillNotification chan struct{}

	const numNodes = 5
	var chunkSize int64 = 100
	var maxValue = 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows and a
		// correspondingly smaller chunk size.
		chunkSize = 5
		maxValue = 200
	}

	params, _ := tests.CreateTestServerParams()
	initBackfillNotification := func() chan struct{} {
		mu.Lock()
		defer mu.Unlock()
		backfillNotification = make(chan struct{})
		return backfillNotification
	}
	notifyBackfill := func() {
		mu.Lock()
		defer mu.Unlock()
		if backfillNotification != nil {
			// Close channel to notify that the backfill has started.
			close(backfillNotification)
			backfillNotification = nil
		}
	}
	// Disable asynchronous schema change execution.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				notifyBackfill()
				return nil
			},
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
	jobRegistry := tc.Server(0).JobRegistry().(*jobs.Registry)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
CREATE UNIQUE INDEX vidx ON t.test (v);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.TODO()

	// number of keys == 2 * number of rows; 1 column family and 1 index entry
	// for each row.
	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Run some schema changes with operations.

	// Add column with a check constraint.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		jobRegistry,
		"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4') CHECK (x >= 0)",
		maxValue,
		2,
		initBackfillNotification(),
		&execCfg)

	// Drop column.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		jobRegistry,
		"ALTER TABLE t.test DROP pi",
		maxValue,
		2,
		initBackfillNotification(),
		&execCfg)

	// Add index.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		jobRegistry,
		"CREATE UNIQUE INDEX foo ON t.test (v)",
		maxValue,
		3,
		initBackfillNotification(),
		&execCfg)

	// Add STORING index (that will have non-nil values).
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		jobRegistry,
		"CREATE INDEX bar ON t.test(k) STORING (v)",
		maxValue,
		4,
		initBackfillNotification(),
		&execCfg)

	// Verify that the index foo over v is consistent, and that column x has
	// been backfilled properly.
	rows, err := sqlDB.Query(`SELECT v, x from t.test@foo`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var val int
		var x float64
		if err := rows.Scan(&val, &x); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count != val {
			t.Errorf("e = %d, v = %d", count, val)
		}
		if x != 1.4 {
			t.Errorf("e = %f, v = %f", 1.4, x)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	eCount := maxValue + 1
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}
}

// Test that a table drop in the middle of a backfill works properly.
// The backfill will terminate in the middle, and the drop will
// successfully complete without deleting the data.
func TestDropWhileBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// protects backfillNotification
	var mu syncutil.Mutex
	backfillNotification := make(chan struct{})

	var partialBackfillDone atomic.Value
	partialBackfillDone.Store(false)
	const numNodes, chunkSize = 5, 100
	maxValue := 4000
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		// We expect this to also reduce the memory footprint of the test.
		maxValue = 200
	}
	params, _ := tests.CreateTestServerParams()
	notifyBackfill := func() {
		mu.Lock()
		defer mu.Unlock()
		if backfillNotification != nil {
			// Close channel to notify that the backfill has started.
			close(backfillNotification)
			backfillNotification = nil
		}
	}
	// Disable asynchronous schema change execution to allow synchronous path
	// to trigger start of backfill notification.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if partialBackfillDone.Load().(bool) {
					notifyBackfill()
					// Returning DeadlineExceeded will result in the
					// schema change being retried.
					return context.DeadlineExceeded
				}
				partialBackfillDone.Store(true)
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
CREATE UNIQUE INDEX vidx ON t.test (v);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.TODO()

	// number of keys == 2 * number of rows; 1 column family and 1 index entry
	// for each row.
	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	notification := backfillNotification
	// Run the schema change in a separate goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Start schema change that eventually runs a partial backfill.
		if _, err := sqlDB.Exec("CREATE UNIQUE INDEX bar ON t.test (v)"); err != nil && !testutils.IsError(err, "table is being dropped") {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until the schema change backfill is partially complete.
	<-notification

	if _, err := sqlDB.Exec("DROP TABLE t.test"); err != nil {
		t.Fatal(err)
	}

	// Wait until the schema change is done.
	wg.Wait()

	// Ensure that the table data hasn't been deleted.
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 2 * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
	// Check that the table descriptor exists so we know the data will
	// eventually be deleted.
	tbDescKey := sqlbase.MakeDescMetadataKey(tableDesc.ID)
	if gr, err := kvDB.Get(ctx, tbDescKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatalf("table descriptor doesn't exist after table is dropped: %q", tbDescKey)
	}
}

// Test that a schema change on encountering a permanent backfill error
// on a remote node terminates properly and returns the database to a
// proper state.
func TestBackfillErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes, chunkSize, maxValue = 5, 100, 4000
	params, _ := tests.CreateTestServerParams()

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Update v column on some rows to be the same so that the future
	// UNIQUE index we create on it fails.
	//
	// Pick a set of random rows because if we pick a deterministic set
	// we can't be sure they will end up on a remote node. We want this
	// test to fail if an error is not reported correctly on a local or
	// remote node and the randomness allows us to test both.
	const numUpdatedRows = 10
	for i := 0; i < numUpdatedRows; i++ {
		k := rand.Intn(maxValue - numUpdatedRows)
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, 1, k); err != nil {
			t.Error(err)
		}
	}

	// Split the table into multiple ranges.
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
CREATE UNIQUE INDEX vidx ON t.test (v);
`); !testutils.IsError(err, `violates unique constraint "vidx"`) {
		t.Fatalf("got err=%s", err)
	}

	// Index backfill errors at a non-deterministic chunk and the garbage
	// keys remain because the async schema changer for the rollback stays
	// disabled in order to assert the next errors. Therefore we do not check
	// the keycount from this operation and just check that the next failed
	// operations do not add more.
	keyCount, err := getTableKeyCount(ctx, kvDB)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	   ALTER TABLE t.test ADD COLUMN p DECIMAL NOT NULL DEFAULT (DECIMAL '1-3');
	   `); !testutils.IsError(err, `could not parse "1-3" as type decimal`) {
		t.Fatalf("got err=%s", err)
	}

	if err := checkTableKeyCountExact(ctx, kvDB, keyCount); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	ALTER TABLE t.test ADD COLUMN p DECIMAL NOT NULL;
	`); !testutils.IsError(err, `null value in column \"p\" violates not-null constraint`) {
		t.Fatalf("got err=%s", err)
	}

	if err := checkTableKeyCountExact(ctx, kvDB, keyCount); err != nil {
		t.Fatal(err)
	}
}

// Test aborting a schema change backfill transaction and check that the
// backfill is completed correctly. The backfill transaction is aborted at a
// time when it thinks it has processed all the rows of the table. Later,
// before the transaction is retried, the table is populated with more rows
// that a backfill chunk, requiring the backfill to forget that it is at the
// end of its processing and needs to continue on to process two more chunks
// of data.
func TestAbortSchemaChangeBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var backfillNotification, commandsDone chan struct{}
	var dontAbortBackfill uint32
	params, _ := tests.CreateTestServerParams()
	const maxValue = 100
	backfillCount := int64(0)
	retriedBackfill := int64(0)
	var retriedSpan roachpb.Span

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecQuickly:  true,
			BackfillChunkSize: maxValue,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				switch atomic.LoadInt64(&backfillCount) {
				case 0:
					// Keep track of the span provided with the first backfill
					// attempt.
					retriedSpan = sp
				case 1:
					// Ensure that the second backfill attempt provides the
					// same span as the first.
					if sp.EqualValue(retriedSpan) {
						atomic.AddInt64(&retriedBackfill, 1)
					}
				}
				return nil
			},
			RunAfterBackfillChunk: func() {
				atomic.AddInt64(&backfillCount, 1)
				if atomic.SwapUint32(&dontAbortBackfill, 1) == 1 {
					return
				}
				// Close channel to notify that the backfill has been
				// completed but hasn't yet committed.
				close(backfillNotification)
				// Receive signal that the commands that push the backfill
				// transaction have completed; The backfill will attempt
				// to commit and will abort.
				<-commandsDone
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	// Add a zone config for the table.
	if _, err := addImmediateGCZoneConfig(sqlDB, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	// Bulk insert enough rows to exceed the chunk size.
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, i)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	// The two drop cases (column and index) do not need to be tested here
	// because the INSERT down below will not insert an entry for a dropped
	// column or index, however, it's still nice to have the column drop
	// just in case INSERT gets messed up. The writes will never abort a
	// drop index because it uses ClearRange, so it is not tested.
	testCases := []struct {
		sql string
		// Each schema change adds/drops a schema element that affects the
		// number of keys representing a table row.
		expectedNumKeysPerRow int
	}{
		{"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4') CHECK (x >= 0)", 1},
		{"ALTER TABLE t.test DROP x", 1},
		{"CREATE UNIQUE INDEX foo ON t.test (v)", 2},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			// Delete two rows so that the table size is smaller than a backfill
			// chunk. The two values will be added later to make the table larger
			// than a backfill chunk after the schema change backfill is aborted.
			for i := 0; i < 2; i++ {
				if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, i); err != nil {
					t.Fatal(err)
				}
			}

			backfillNotification = make(chan struct{})
			commandsDone = make(chan struct{})
			atomic.StoreUint32(&dontAbortBackfill, 0)
			// Run the column schema change in a separate goroutine.
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				// Start schema change that eventually runs a backfill.
				if _, err := sqlDB.Exec(testCase.sql); err != nil {
					t.Error(err)
				}

				wg.Done()
			}()

			// Wait until the schema change backfill has finished writing its
			// intents.
			<-backfillNotification

			// Delete a row that will push the backfill transaction.
			if _, err := sqlDB.Exec(`
BEGIN TRANSACTION PRIORITY HIGH;
DELETE FROM t.test WHERE k = 2;
COMMIT;
			`); err != nil {
				t.Fatal(err)
			}

			// Add missing rows so that the table exceeds the size of a
			// backfill chunk.
			for i := 0; i < 3; i++ {
				if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, i, i); err != nil {
					t.Fatal(err)
				}
			}

			// Release backfill so that it can try to commit and in the
			// process discover that it was aborted.
			close(commandsDone)

			wg.Wait() // for schema change to complete

			ctx := context.TODO()

			// Verify the number of keys left behind in the table to validate
			// schema change operations.
			if err := checkTableKeyCount(
				ctx, kvDB, testCase.expectedNumKeysPerRow, maxValue,
			); err != nil {
				t.Fatal(err)
			}

			if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Add an index and check that it succeeds.
func addIndexSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *client.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("CREATE UNIQUE INDEX foo ON t.test (v)"); err != nil {
		t.Fatal(err)
	}

	// The schema change succeeded. Verify that the index foo over v is
	// consistent.
	rows, err := sqlDB.Query(`SELECT v from t.test@foo`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var val int
		if err := rows.Scan(&val); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count != val {
			t.Errorf("e = %d, v = %d", count, val)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := maxValue + 1; eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}
}

// Add a column with a check constraint and check that it succeeds.
func addColumnSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *client.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4') CHECK (x >= 0)"); err != nil {
		t.Fatal(err)
	}
	rows, err := sqlDB.Query(`SELECT x from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for ; rows.Next(); count++ {
		var val float64
		if err := rows.Scan(&val); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if e := 1.4; e != val {
			t.Errorf("e = %f, v = %f", e, val)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := maxValue + 1; eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}
}

// Drop a column and check that it succeeds.
func dropColumnSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *client.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("ALTER TABLE t.test DROP x"); err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}

}

// Drop an index and check that it succeeds.
func dropIndexSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *client.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("DROP INDEX t.test@foo CASCADE"); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(context.TODO(), kvDB, numKeysPerRow, maxValue); err != nil {
		t.Fatal(err)
	}
}

// TestDropColumn tests that dropped columns properly drop their Table's CHECK constraints,
// or an error occurs if a CHECK constraint is being added on it.
func TestDropColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
  k INT PRIMARY KEY,
  v INT CONSTRAINT check_v CHECK (v >= 0),
  a INT DEFAULT 0 CONSTRAINT check_av CHECK (a <= v),
  b INT DEFAULT 100 CONSTRAINT check_ab CHECK (b > a)
);
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if len(tableDesc.Checks) != 3 {
		t.Fatalf("Expected 3 checks but got %d ", len(tableDesc.Checks))
	}

	if _, err := sqlDB.Exec("ALTER TABLE t.test DROP v"); err != nil {
		t.Fatal(err)
	}

	// Re-read table descriptor.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	// Only check_ab should remain
	if len(tableDesc.Checks) != 1 {
		checkExprs := make([]string, 0)
		for i := range tableDesc.Checks {
			checkExprs = append(checkExprs, tableDesc.Checks[i].Expr)
		}
		t.Fatalf("Expected 1 check but got %d with CHECK expr %s ", len(tableDesc.Checks), strings.Join(checkExprs, ", "))
	}

	if tableDesc.Checks[0].Name != "check_ab" {
		t.Fatalf("Only check_ab should remain, got: %s ", tableDesc.Checks[0].Name)
	}

	// Test that a constraint being added prevents the column from being dropped.
	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec(`ALTER TABLE t.test ADD CONSTRAINT check_bk CHECK (b >= k)`); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec(`ALTER TABLE t.test DROP b`); !testutils.IsError(err,
		"referencing constraint \"check_bk\" in the middle of being added") {
		t.Fatalf("err = %+v", err)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Test schema changes are retried and complete properly. This also checks
// that a mutation checkpoint reduces the number of chunks operated on during
// a retry.
func TestSchemaChangeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	currChunk := 0
	seenSpan := roachpb.Span{}
	checkSpan := func(sp roachpb.Span) error {
		currChunk++
		// Fail somewhere in the middle.
		if currChunk == 3 {
			if rand.Intn(2) == 0 {
				return context.DeadlineExceeded
			} else {
				errAmbiguous := &roachpb.AmbiguousResultError{}
				return roachpb.NewError(errAmbiguous).GoError()
			}
		}
		if sp.Key != nil && seenSpan.Key != nil {
			// Check that the keys are never reevaluated
			if seenSpan.Key.Compare(sp.Key) >= 0 {
				t.Errorf("reprocessing span %s, already seen span %s", sp, seenSpan)
			}
			if !seenSpan.EndKey.Equal(sp.EndKey) {
				t.Errorf("different EndKey: span %s, already seen span %s", sp, seenSpan)
			}
		}
		seenSpan = sp
		return nil
	}

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Disable asynchronous schema change execution to allow
			// synchronous path to run schema changes.
			AsyncExecNotification:   asyncSchemaChangerDisabled,
			WriteCheckpointInterval: time.Nanosecond,
		},
		DistSQL: &execinfra.TestingKnobs{RunBeforeBackfillChunk: checkSpan},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	const maxValue = 2000
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	addIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	addColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2)
}

// Test schema changes are retried and complete properly when the table
// version changes. This also checks that a mutation checkpoint reduces
// the number of chunks operated on during a retry.
func TestSchemaChangeRetryOnVersionChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	var upTableVersion func()
	const maxValue = 2000
	currChunk := 0
	var numBackfills uint32
	seenSpan := roachpb.Span{}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				atomic.AddUint32(&numBackfills, 1)
				return nil
			},
			// Disable asynchronous schema change execution to allow
			// synchronous path to run schema changes.
			AsyncExecNotification:   asyncSchemaChangerDisabled,
			WriteCheckpointInterval: time.Nanosecond,
			BackfillChunkSize:       maxValue / 10,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				currChunk++
				// Fail somewhere in the middle.
				if currChunk == 3 {
					// Publish a new version of the table.
					upTableVersion()
				}
				if seenSpan.Key != nil {
					if !seenSpan.EndKey.Equal(sp.EndKey) {
						t.Errorf("different EndKey: span %s, already seen span %s", sp, seenSpan)
					}
				}
				seenSpan = sp
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	id := tableDesc.ID
	ctx := context.TODO()

	upTableVersion = func() {
		leaseMgr := s.LeaseManager().(*sql.LeaseManager)
		var version sqlbase.DescriptorVersion
		if _, err := leaseMgr.Publish(ctx, id, func(table *sqlbase.MutableTableDescriptor) error {
			// Publish nothing; only update the version.
			version = table.Version
			return nil
		}, nil); err != nil {
			t.Error(err)
		}
		// Grab a lease at the latest version so that we are confident
		// that all future leases will be taken at the latest version.
		table, _, err := leaseMgr.AcquireAndAssertMinVersion(ctx, s.Clock().Now(), id, version+1)
		if err != nil {
			t.Error(err)
		}
		if err := leaseMgr.Release(table); err != nil {
			t.Error(err)
		}
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	addIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2)
	if num := atomic.SwapUint32(&numBackfills, 0); num != 2 {
		t.Fatalf("expected %d backfills, but seen %d", 2, num)
	}

	currChunk = 0
	seenSpan = roachpb.Span{}
	addColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)
	if num := atomic.SwapUint32(&numBackfills, 0); num != 2 {
		t.Fatalf("expected %d backfills, but seen %d", 2, num)
	}

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)
	if num := atomic.SwapUint32(&numBackfills, 0); num != 2 {
		t.Fatalf("expected %d backfills, but seen %d", 2, num)
	}
}

// Test schema change purge failure doesn't leave DB in a bad state.
func TestSchemaChangePurgeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	var enableAsyncSchemaChanges uint32
	var attempts int32
	// attempt 1: write the first chunk of the index.
	// attempt 2: write the second chunk and hit a unique constraint
	// violation; purge the schema change.
	// attempt 3: return an error while purging the schema change.
	var expectedAttempts int32 = 3
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: func() error {
				if enable := atomic.LoadUint32(&enableAsyncSchemaChanges); enable == 0 {
					return errors.New("async schema changes are disabled")
				}
				return nil
			},
			// Speed up evaluation of async schema changes so that it
			// processes a purged schema change quickly.
			AsyncExecQuickly:  true,
			BackfillChunkSize: chunkSize,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				// Return a deadline exceeded error during the third attempt
				// which attempts to clean up the schema change.
				if atomic.AddInt32(&attempts, 1) == expectedAttempts {
					// Disable the async schema changer for assertions.
					atomic.StoreUint32(&enableAsyncSchemaChanges, 0)
					return context.DeadlineExceeded
				}
				return nil
			},
			// the backfiller flushes after every batch if RunAfterBackfillChunk is
			// non-nil so this noop fn means we can observe the partial-backfill that
			// would otherwise just be buffered.
			RunAfterBackfillChunk: func() {},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	const maxValue = chunkSize + 1
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Add a row with a duplicate value=0 which is the same
	// value as for the key maxValue.
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test VALUES ($1, $2)`, maxValue+1, 0,
	); err != nil {
		t.Fatal(err)
	}

	// A schema change that violates integrity constraints.
	if _, err := sqlDB.Exec(
		"CREATE UNIQUE INDEX foo ON t.test (v)",
	); !testutils.IsError(err, `violates unique constraint "foo"`) {
		t.Fatal(err)
	}

	// The index doesn't exist
	if _, err := sqlDB.Query(
		`SELECT v from t.test@foo`,
	); !testutils.IsError(err, "index .* not found") {
		t.Fatal(err)
	}

	// Allow async schema change purge to attempt backfill and error.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if _, err := addImmediateGCZoneConfig(sqlDB, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	// The deadline exceeded error in the schema change purge results in no
	// retry attempts of the purge.
	testutils.SucceedsSoon(t, func() error {
		if read := atomic.LoadInt32(&attempts); read != expectedAttempts {
			return errors.Errorf("%d retries, despite allowing only (schema change + reverse) = %d", read, expectedAttempts)
		}
		return nil
	})

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// There is still a DROP INDEX mutation waiting for GC.
	if e := 1; len(tableDesc.GCMutations) != e {
		t.Fatalf("the table has %d instead of %d GC mutations", len(tableDesc.GCMutations), e)
	} else if m := tableDesc.GCMutations[0]; m.IndexID != 2 && m.DropTime == 0 && m.JobID == 0 {
		t.Fatalf("unexpected GC mutation %v", m)
	}

	// There is still some garbage index data that needs to be purged. All the
	// rows from k = 0 to k = chunkSize - 1 have index values.
	numGarbageValues := chunkSize

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue+1+numGarbageValues); err != nil {
		t.Fatal(err)
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Enable async schema change processing to ensure that it cleans up the
	// above garbage left behind.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	testutils.SucceedsSoon(t, func() error {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.GCMutations) > 0 {
			return errors.Errorf("%d GC mutations remaining", len(tableDesc.GCMutations))
		}
		return nil
	})

	// No garbage left behind.
	numGarbageValues = 0
	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue+1+numGarbageValues); err != nil {
		t.Fatal(err)
	}

	// A new attempt cleans up a chunk of data.
	if attempts != expectedAttempts+1 {
		t.Fatalf("%d chunk ops, despite allowing only (schema change + reverse) = %d", attempts, expectedAttempts)
	}
}

// Test schema change failure after a backfill checkpoint has been written
// doesn't leave the DB in a bad state.
func TestSchemaChangeFailureAfterCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	attempts := 0
	// attempt 1: write two chunks of the column.
	// attempt 2: writing the third chunk returns a permanent failure
	// purge the schema change.
	expectedAttempts := 3
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
			// Aggressively checkpoint, so that a schema change
			// failure happens after a checkpoint has been written.
			WriteCheckpointInterval: time.Nanosecond,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Return a deadline exceeded error during the third attempt
				// which attempts to clean up the schema change.
				if attempts == expectedAttempts {
					return errors.New("permanent failure")
				}
				return nil
			},
		},
		// Disable backfill migrations so it doesn't interfere with the
		// backfill in this test.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	const maxValue = 4*chunkSize + 1
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(context.TODO(), kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	// A schema change that fails.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD column d INT DEFAULT 0 CREATE FAMILY F3, ADD CHECK (d >= 0)`); !testutils.IsError(err, `permanent failure`) {
		t.Fatalf("err = %s", err)
	}

	// No garbage left behind.
	if err := checkTableKeyCount(context.TODO(), kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	// A schema change that fails after the first mutation has completed. The
	// column is backfilled and the index backfill fails requiring the column
	// backfill to be rolled back.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test ADD column e INT DEFAULT 0 UNIQUE CREATE FAMILY F4, ADD CHECK (e >= 0)`,
	); !testutils.IsError(err, ` violates unique constraint`) {
		t.Fatalf("err = %s", err)
	}

	// No garbage left behind.
	if err := checkTableKeyCount(context.TODO(), kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	// Check that constraints are cleaned up.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if checks := tableDesc.AllActiveAndInactiveChecks(); len(checks) > 0 {
		t.Fatalf("found checks %+v", checks)
	}
}

// TestSchemaChangeReverseMutations tests that schema changes get reversed
// correctly when one of them violates a constraint.
func TestSchemaChangeReverseMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	// Disable synchronous schema change processing so that the mutations get
	// processed asynchronously.
	var enableAsyncSchemaChanges uint32
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: func() error {
				if enable := atomic.LoadUint32(&enableAsyncSchemaChanges); enable == 0 {
					return errors.New("async schema changes are disabled")
				}
				return nil
			},
			AsyncExecQuickly:  true,
			BackfillChunkSize: chunkSize,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// Create a k-v table.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT8);
`); err != nil {
		t.Fatal(err)
	}

	// Add some data
	const maxValue = chunkSize + 1
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	testCases := []struct {
		sql    string
		status jobs.Status
	}{
		// Create a column that is not NULL. This schema change doesn't return an
		// error only because we've turned off the synchronous execution path; it
		// will eventually fail when run by the asynchronous path.
		{`ALTER TABLE t.public.test ADD COLUMN a INT8 UNIQUE DEFAULT 0, ADD COLUMN c INT8`,
			jobs.StatusFailed},
		// Add an index over a column that will be purged. This index will
		// eventually not get added. The column aa will also be dropped as
		// a result.
		{`ALTER TABLE t.public.test ADD COLUMN aa INT8, ADD CONSTRAINT foo UNIQUE (a)`,
			jobs.StatusFailed},

		// The purge of column 'a' doesn't influence these schema changes.

		// Drop column 'v' moves along just fine.
		{`ALTER TABLE t.public.test DROP COLUMN v`,
			jobs.StatusSucceeded},
		// Add unique column 'b' moves along creating column b and the index on
		// it.
		{`ALTER TABLE t.public.test ADD COLUMN b INT8 UNIQUE`,
			jobs.StatusSucceeded},
		// #27033: Add a column followed by an index on the column.
		{`ALTER TABLE t.public.test ADD COLUMN d STRING NOT NULL DEFAULT 'something'`,
			jobs.StatusSucceeded},

		{`CREATE INDEX ON t.public.test (d)`,
			jobs.StatusSucceeded},

		// Add an index over a column 'c' that will be purged. This index will
		// eventually not get added. The column bb will also be dropped as
		// a result.
		{`ALTER TABLE t.public.test ADD COLUMN bb INT8, ADD CONSTRAINT bar UNIQUE (c)`,
			jobs.StatusFailed},
		// Cascading of purges. column 'c' -> column 'bb' -> constraint 'idx_bb'.
		{`ALTER TABLE t.public.test ADD CONSTRAINT idx_bb UNIQUE (bb)`,
			jobs.StatusFailed},
	}

	for _, tc := range testCases {
		if _, err := sqlDB.Exec(tc.sql); err != nil {
			t.Fatal(err)
		}
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if e := 13; e != len(tableDesc.Mutations) {
		t.Fatalf("e = %d, v = %d", e, len(tableDesc.Mutations))
	}

	// Enable async schema change processing for purged schema changes.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	expectedCols := []string{"k", "b", "d"}
	// Wait until all the mutations have been processed.
	testutils.SucceedsSoon(t, func() error {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.Mutations) > 0 {
			return errors.Errorf("%d mutations remaining", len(tableDesc.Mutations))
		}
		return nil
	})

	// Verify that t.public.test has the expected data. Read the table data while
	// ensuring that the correct table lease is in use.
	rows, err := sqlDB.Query(`SELECT * from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}

	// Ensure that sql is using the correct table lease.
	if len(cols) != len(expectedCols) {
		t.Fatalf("incorrect columns: %v, expected: %v", cols, expectedCols)
	}
	if cols[0] != expectedCols[0] || cols[1] != expectedCols[1] {
		t.Fatalf("incorrect columns: %v", cols)
	}

	// rows contains the data; verify that it's the right data.
	vals := make([]interface{}, len(expectedCols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	var count int64
	for ; rows.Next(); count++ {
		if err := rows.Scan(vals...); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		for j, v := range vals {
			switch j {
			case 0:
				if val := *v.(*interface{}); val != nil {
					switch k := val.(type) {
					case int64:
						if count != k {
							t.Errorf("k = %d, expected %d", k, count)
						}

					default:
						t.Errorf("error input of type %T", k)
					}
				} else {
					t.Error("received NULL value for column 'k'")
				}

			case 1:
				if val := *v.(*interface{}); val != nil {
					t.Error("received non NULL value for column 'b'")
				}

			case 2:
				if val := *v.(*interface{}); val == nil {
					t.Error("received NULL value for column 'd'")
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := int64(maxValue + 1); eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Check that the index on b eventually goes live even though a schema
	// change in front of it in the queue got purged.
	rows, err = sqlDB.Query(`SELECT * from t.test@test_b_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count = 0
	for ; rows.Next(); count++ {
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount := int64(maxValue + 1); eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Check that the index on c gets purged.
	if _, err := sqlDB.Query(`SELECT * from t.test@foo`); err == nil {
		t.Fatal("SELECT over index 'foo' works")
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Add immediate GC TTL to allow index creation purge to complete.
	if _, err := addImmediateGCZoneConfig(sqlDB, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.GCMutations) > 0 {
			return errors.Errorf("%d gc mutations remaining", len(tableDesc.GCMutations))
		}
		return nil
	})

	ctx := context.TODO()

	// Check that the number of k-v pairs is accurate.
	if err := checkTableKeyCount(ctx, kvDB, 3, maxValue); err != nil {
		t.Fatal(err)
	}

	// State of jobs table
	runner := sqlutils.SQLRunner{DB: sqlDB}
	for i, tc := range testCases {
		if err := jobutils.VerifySystemJob(t, &runner, i, jobspb.TypeSchemaChange, tc.status, jobs.Record{
			Username:    security.RootUser,
			Description: tc.sql,
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	jobRolledBack := 0
	jobID := jobutils.GetJobID(t, &runner, jobRolledBack)

	// Roll back job.
	if err := jobutils.VerifySystemJob(t, &runner, len(testCases), jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: fmt.Sprintf("ROLL BACK JOB %d: %s", jobID, testCases[jobRolledBack].sql),
		DescriptorIDs: sqlbase.IDs{
			tableDesc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}

}

// This test checks backward compatibility with old data that contains
// sentinel kv pairs at the start of each table row. Cockroachdb used
// to write table rows with sentinel values in the past. When a new column
// is added to such a table with the new column included in the same
// column family as the primary key columns, the sentinel kv pairs
// start representing this new column. This test checks that the sentinel
// values represent NULL column values, and that an UPDATE to such
// a column works correctly.
func TestParseSentinelValueWithNewColumnInSentinelFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
	k INT PRIMARY KEY,
	FAMILY F1 (k)
);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if tableDesc.Families[0].DefaultColumnID != 0 {
		t.Fatalf("default column id not set properly: %s", tableDesc)
	}

	// Add some data.
	const maxValue = 10
	inserts := make([]string, maxValue+1)
	for i := range inserts {
		inserts[i] = fmt.Sprintf(`(%d)`, i)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()

	// Convert table data created by the above INSERT into sentinel
	// values. This is done to make the table appear like it were
	// written in the past when cockroachdb used to write sentinel
	// values for each table row.
	startKey := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	kvs, err := kvDB.Scan(
		ctx,
		startKey,
		startKey.PrefixEnd(),
		maxValue+1)
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range kvs {
		value := roachpb.MakeValueFromBytes(nil)
		if err := kvDB.Put(ctx, kv.Key, &value); err != nil {
			t.Fatal(err)
		}
	}

	// Add a new column that gets added to column family 0,
	// updating DefaultColumnID.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN v INT FAMILY F1`); err != nil {
		t.Fatal(err)
	}
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if tableDesc.Families[0].DefaultColumnID != 2 {
		t.Fatalf("default column id not set properly: %s", tableDesc)
	}

	// Update one of the rows.
	const setKey = 5
	const setVal = maxValue - setKey
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, setVal, setKey); err != nil {
		t.Fatal(err)
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// The table contains the one updated value and remaining NULL values.
	rows, err := sqlDB.Query(`SELECT v from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	const eCount = maxValue + 1
	count := 0
	for ; rows.Next(); count++ {
		var val *int
		if err := rows.Scan(&val); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count == setKey {
			if val != nil {
				if setVal != *val {
					t.Errorf("value = %d, expected %d", *val, setVal)
				}
			} else {
				t.Error("received nil value for column 'v'")
			}
		} else if val != nil {
			t.Error("received non NULL value for column 'v'")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}
}

// This test checks whether a column can be added using the name of a column that has just been dropped.
func TestAddColumnDuringColumnDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Close channel to notify that the schema change has
					// been queued and the backfill has started.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT PRIMARY KEY NOT NULL,
    v INT NOT NULL
);
`); err != nil {
		t.Fatal(err)
	}

	if err := bulkInsertIntoTable(sqlDB, 1000); err != nil {
		t.Fatal(err)
	}
	// Run the column schema change in a separate goroutine.
	notification := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP column v;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notification
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD column v INT DEFAULT 0;`); !testutils.IsError(err, `column "v" being dropped, try again later`) {
		t.Fatal(err)
	}

	close(continueBackfillNotification)
	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// Test a DROP failure on a unique column. The rollback
// process might not be able to reconstruct the index and thus
// purges the rollback. For now this is considered acceptable.
func TestSchemaUniqueColumnDropFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const chunkSize = 200
	attempts := 0
	// DROP UNIQUE COLUMN is executed in two steps: drop index and drop column.
	// Dropping the index happens in a separate mutation job from the drop column
	// which does not perform backfilling (like adding indexes and add/drop
	// columns) and completes successfully. However, note that the testing knob
	// hooks are still run as if they were backfill attempts. The index truncation
	// happens as an asynchronous change after the index descriptor is removed,
	// and will be run after the GC TTL is passed and there are no pending
	// synchronous mutations. Therefore, the first two backfill attempts are from
	// the column drop. This part of the change errors during backfilling the
	// second chunk.
	const expectedColumnBackfillAttempts = 2
	const maxValue = (expectedColumnBackfillAttempts/2+1)*chunkSize + 1
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
			// Aggressively checkpoint, so that a schema change
			// failure happens after a checkpoint has been written.
			WriteCheckpointInterval: time.Nanosecond,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Return a deadline exceeded error while dropping
				// the column after the index has been dropped.
				if attempts == expectedColumnBackfillAttempts {
					return errors.New("permanent failure")
				}
				return nil
			},
		},
		// Disable backfill migrations so it doesn't interfere with the
		// backfill in this test.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT UNIQUE DEFAULT 23 CREATE FAMILY F3);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(context.TODO(), kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}

	// A schema change that fails.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP column v`); !testutils.IsError(err, `permanent failure`) {
		t.Fatalf("err = %s", err)
	}

	// The index is not regenerated.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if len(tableDesc.Indexes) > 0 {
		t.Fatalf("indexes %+v", tableDesc.Indexes)
	}

	// Unfortunately this is the same failure present when an index drop
	// fails, so the rollback never completes and leaves orphaned kvs.
	// TODO(erik): Ignore errors or individually drop indexes in
	// DELETE_AND_WRITE_ONLY which failed during the creation backfill
	// as a rollback from a drop.
	if e := 1; e != len(tableDesc.Columns) {
		t.Fatalf("e = %d, v = %d, columns = %+v", e, len(tableDesc.Columns), tableDesc.Columns)
	} else if tableDesc.Columns[0].Name != "k" {
		t.Fatalf("columns %+v", tableDesc.Columns)
	} else if len(tableDesc.Mutations) != 2 {
		t.Fatalf("mutations %+v", tableDesc.Mutations)
	}
}

// Test CRUD operations can read NULL values for NOT NULL columns
// in the middle of a column backfill.
func TestCRUDWhileColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	backfillNotification := make(chan bool)

	backfillCompleteNotification := make(chan bool)
	continueSchemaChangeNotification := make(chan bool)

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if backfillNotification != nil {
					// Close channel to notify that the schema change has
					// been queued and the backfill has started.
					close(backfillNotification)
					backfillNotification = nil
					<-continueSchemaChangeNotification
				}
				return nil
			},
			RunAfterBackfillChunk: func() {
				if backfillCompleteNotification != nil {
					// Close channel to notify that the schema change
					// backfill is complete and not finalized.
					close(backfillCompleteNotification)
					backfillCompleteNotification = nil
					<-continueSchemaChangeNotification
				}
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT8 NOT NULL,
    v INT8,
    length INT8 NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (k),
    INDEX v_idx (v),
    FAMILY "primary" (k, v, length)
);
INSERT INTO t.test (k, v, length) VALUES (0, 1, 1);
INSERT INTO t.test (k, v, length) VALUES (1, 2, 1);
INSERT INTO t.test (k, v, length) VALUES (2, 3, 1);
`); err != nil {
		t.Fatal(err)
	}

	// Run the column schema change in a separate goroutine.
	notification := backfillNotification
	doneNotification := backfillCompleteNotification
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD id INT8 NOT NULL DEFAULT 2, ADD u INT8 NOT NULL AS (v+1) STORED;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until the first mutation has processed through the state machine
	// and has started backfilling.
	<-notification

	go func() {
		// Create a column that uses the above column in an expression.
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD z INT8 AS (k + id) STORED;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until both mutations are queued up.
	testutils.SucceedsSoon(t, func() error {
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if l := len(tableDesc.Mutations); l != 3 {
			return errors.Errorf("number of mutations = %d", l)
		}
		return nil
	})

	// UPDATE the row using the secondary index.
	if _, err := sqlDB.Exec(`UPDATE t.test SET length = 27000 WHERE v = 1`); err != nil {
		t.Error(err)
	}

	// UPDATE the row using the primary index.
	if _, err := sqlDB.Exec(`UPDATE t.test SET length = 27001 WHERE k = 1`); err != nil {
		t.Error(err)
	}

	// Use UPSERT instead of UPDATE.
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, length) VALUES (2, 3, 27000)`); err != nil {
		t.Error(err)
	}

	// UPSERT inserts a new row.
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, length) VALUES (3, 4, 27000)`); err != nil {
		t.Error(err)
	}

	// INSERT inserts a new row.
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, length) VALUES (4, 5, 270)`); err != nil {
		t.Error(err)
	}

	// DELETE.
	if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 1`); err != nil {
		t.Error(err)
	}

	// DELETE using the secondary index.
	if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE v = 4`); err != nil {
		t.Error(err)
	}

	// Ensure that the newly added column cannot be supplied with any values.
	if _, err := sqlDB.Exec(`UPDATE t.test SET id = 27000 WHERE k = 2`); !testutils.IsError(err,
		`column "id" does not exist`) && !testutils.IsError(err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET id = NULL WHERE k = 2`); !testutils.IsError(err,
		`column "id" does not exist`) && !testutils.IsError(err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, id) VALUES (2, 3, 234)`); !testutils.IsError(
		err, `column "id" does not exist`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`UPSERT INTO t.test(k, v, id) VALUES (2, 3, NULL)`); !testutils.IsError(
		err, `column "id" does not exist`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, id) VALUES (4, 5, 270)`); !testutils.IsError(
		err, `column "id" does not exist`) && !testutils.IsError(
		err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, id) VALUES (4, 5, NULL)`); !testutils.IsError(
		err, `column "id" does not exist`) && !testutils.IsError(
		err, `column "id" is being backfilled`) {
		t.Errorf("err = %+v", err)
	}

	// Use column in an expression.
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test (k, v, length) VALUES (2, 1, 3) ON CONFLICT (k) DO UPDATE SET (k, v, length) = (id + 1, 1, 3)`,
	); err != nil {
		t.Error(err)
	}

	// SHOW CREATE TABLE doesn't show new columns.
	row := sqlDB.QueryRow(`SHOW CREATE TABLE t.test`)
	var scanName, create string
	if err := row.Scan(&scanName, &create); err != nil {
		t.Fatal(err)
	}
	if scanName != `t.public.test` {
		t.Fatalf("expected table name %s, got %s", `test`, scanName)
	}
	expect := `CREATE TABLE test (
	k INT8 NOT NULL,
	v INT8 NULL,
	length INT8 NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (k ASC),
	INDEX v_idx (v ASC),
	FAMILY "primary" (k, v, length)
)`
	if create != expect {
		t.Fatalf("got: %s\nexpected: %s", create, expect)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if l := len(tableDesc.Mutations); l != 3 {
		t.Fatalf("number of mutations = %d", l)
	}

	continueSchemaChangeNotification <- true

	<-doneNotification

	expectedErr := "\"u\" violates not-null constraint"
	if _, err := sqlDB.Exec(`INSERT INTO t.test(k, v, length) VALUES (5, NULL, 8)`); !testutils.IsError(err, expectedErr) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`UPDATE t.test SET v = NULL WHERE k = 0`); !testutils.IsError(err, expectedErr) {
		t.Fatal(err)
	}

	close(continueSchemaChangeNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
	// Check data!
	rows, err := sqlDB.Query(`SELECT k, v, length, id, u, z FROM t.test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	expected := [][]int{
		{0, 1, 27000, 2, 2, 2},
		{3, 1, 3, 2, 2, 5},
		{4, 5, 270, 2, 6, 6},
	}
	count := 0
	for ; rows.Next(); count++ {
		var i1, i2, i3, i4, i5, i6 *int
		if err := rows.Scan(&i1, &i2, &i3, &i4, &i5, &i6); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		row := fmt.Sprintf("%d %d %d %d %d %d", *i1, *i2, *i3, *i4, *i5, *i6)
		exp := expected[count]
		expRow := fmt.Sprintf("%d %d %d %d %d %d", exp[0], exp[1], exp[2], exp[3], exp[4], exp[5])
		if row != expRow {
			t.Errorf("expected %q but read %q", expRow, row)
		}
	}
	if err := rows.Err(); err != nil {
		t.Error(err)
	} else if count != 3 {
		t.Errorf("expected 3 rows but read %d", count)
	}
}

// Test that a schema change backfill that completes on a
// backfill chunk boundary works correctly. A backfill is done
// by scanning a table in chunks and backfilling the schema
// element for each chunk. Normally the last chunk is smaller
// than the other chunks (configured chunk size), but it can
// sometimes be equal in size. This test deliberately runs a
// schema change where the last chunk size is equal to the
// configured chunk size.
func TestBackfillCompletesOnChunkBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numNodes = 5
	const chunkSize = 100
	// The number of rows in the table is a multiple of chunkSize.
	// [0...maxValue], so that the backfill processing ends on
	// a chunk boundary.
	const maxValue = 3*chunkSize - 1
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
 CREATE DATABASE t;
 CREATE TABLE t.test (k INT8 PRIMARY KEY, v INT8, pi DECIMAL DEFAULT (DECIMAL '3.14'));
 CREATE UNIQUE INDEX vidx ON t.test (v);
 `); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	// Run some schema changes.
	testCases := []struct {
		sql           string
		numKeysPerRow int
	}{
		{sql: "ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')", numKeysPerRow: 2},
		{sql: "ALTER TABLE t.test DROP pi", numKeysPerRow: 2},
		{sql: "CREATE UNIQUE INDEX foo ON t.test (v)", numKeysPerRow: 3},
		{sql: "DROP INDEX t.test@vidx CASCADE", numKeysPerRow: 3},
	}

	for _, tc := range testCases {
		t.Run(tc.sql, func(t *testing.T) {
			// Start schema change that eventually runs a backfill.
			if _, err := sqlDB.Exec(tc.sql); err != nil {
				t.Error(err)
			}

			ctx := context.TODO()

			// Verify the number of keys left behind in the table to
			// validate schema change operations.
			if err := checkTableKeyCount(ctx, kvDB, tc.numKeysPerRow, maxValue); err != nil {
				t.Fatal(err)
			}

			if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSchemaChangeInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('a', 'b');
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name        string
		firstStmt   string
		secondStmt  string
		expectedErr string
	}{
		// DROP TABLE followed by CREATE TABLE case.
		{
			name:        `drop-create`,
			firstStmt:   `DROP TABLE t.kv`,
			secondStmt:  `CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR)`,
			expectedErr: `relation "kv" already exists`,
		},
		// schema change followed by another statement works.
		{
			name:        `createindex-insert`,
			firstStmt:   `CREATE INDEX foo ON t.kv (v)`,
			secondStmt:  `INSERT INTO t.kv VALUES ('c', 'd')`,
			expectedErr: ``,
		},
		// CREATE TABLE followed by INSERT works.
		{
			name:        `createtable-insert`,
			firstStmt:   `CREATE TABLE t.origin (k CHAR PRIMARY KEY, v CHAR);`,
			secondStmt:  `INSERT INTO t.origin VALUES ('c', 'd')`,
			expectedErr: ``},
		// Support multiple schema changes for ORMs: #15269
		// Support insert into another table after schema changes: #15297
		{
			name:        `multiple-schema-change`,
			firstStmt:   `CREATE TABLE t.orm1 (k CHAR PRIMARY KEY, v CHAR); CREATE TABLE t.orm2 (k CHAR PRIMARY KEY, v CHAR);`,
			secondStmt:  `CREATE INDEX foo ON t.orm1 (v); CREATE INDEX foo ON t.orm2 (v); INSERT INTO t.origin VALUES ('e', 'f')`,
			expectedErr: ``,
		},
		// schema change at the end of a transaction that has written.
		{
			name:        `insert-create`,
			firstStmt:   `INSERT INTO t.kv VALUES ('e', 'f')`,
			secondStmt:  `CREATE INDEX foo2 ON t.kv (v)`,
			expectedErr: `schema change statement cannot follow a statement that has written in the same transaction`,
		},
		// schema change at the end of a read only transaction.
		{
			name:        `select-create`,
			firstStmt:   `SELECT * FROM t.kv`,
			secondStmt:  `CREATE INDEX bar ON t.kv (v)`,
			expectedErr: ``,
		},
		{
			name:        `index-on-add-col`,
			firstStmt:   `ALTER TABLE t.kv ADD i INT`,
			secondStmt:  `CREATE INDEX foobar ON t.kv (i)`,
			expectedErr: ``,
		},
		{
			name:        `check-on-add-col`,
			firstStmt:   `ALTER TABLE t.kv ADD j INT`,
			secondStmt:  `ALTER TABLE t.kv ADD CONSTRAINT ck_j CHECK (j >= 0)`,
			expectedErr: ``,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			tx, err := sqlDB.Begin()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := tx.Exec(testCase.firstStmt); err != nil {
				t.Fatal(err)
			}

			_, err = tx.Exec(testCase.secondStmt)

			if testCase.expectedErr != "" {
				// Can't commit after ALTER errored, so we ROLLBACK.
				if rollbackErr := tx.Rollback(); rollbackErr != nil {
					t.Fatal(rollbackErr)
				}

				if !testutils.IsError(err, testCase.expectedErr) {
					t.Fatalf("different error than expected: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}

				if err := sqlutils.RunScrub(sqlDB, "t", "kv"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSecondaryIndexWithOldStoringEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TABLE d.t (
  k INT PRIMARY KEY,
  a INT,
  b INT,
  INDEX i (a) STORING (b),
  UNIQUE INDEX u (a) STORING (b)
);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "d", "t")
	// Verify that this descriptor uses the new STORING encoding. Overwrite it
	// with one that uses the old encoding.
	for i, index := range tableDesc.Indexes {
		if len(index.ExtraColumnIDs) != 1 {
			t.Fatalf("ExtraColumnIDs not set properly: %s", tableDesc)
		}
		if len(index.StoreColumnIDs) != 1 {
			t.Fatalf("StoreColumnIDs not set properly: %s", tableDesc)
		}
		index.ExtraColumnIDs = append(index.ExtraColumnIDs, index.StoreColumnIDs...)
		index.StoreColumnIDs = nil
		tableDesc.Indexes[i] = index
	}
	if err := kvDB.Put(
		context.TODO(),
		sqlbase.MakeDescMetadataKey(tableDesc.GetID()),
		sqlbase.WrapDescriptor(tableDesc),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO d.t VALUES (11, 1, 2);`); err != nil {
		t.Fatal(err)
	}
	// Force another ID allocation to ensure that the old encoding persists.
	if _, err := sqlDB.Exec(`ALTER TABLE d.t ADD COLUMN c INT;`); err != nil {
		t.Fatal(err)
	}
	// Ensure that the decoder sees the old encoding.
	for indexName, expExplainRow := range map[string]string{
		"i": "fetched: /t/i/1/11/2 -> NULL",
		"u": "fetched: /t/u/1 -> /11/2",
	} {
		t.Run("index scan", func(t *testing.T) {
			if _, err := sqlDB.Exec(fmt.Sprintf(`SET tracing = on,kv; SELECT k, a, b FROM d.t@%s; SET tracing = off`, indexName)); err != nil {
				t.Fatal(err)
			}

			rows, err := sqlDB.Query(
				`SELECT message FROM [SHOW KV TRACE FOR SESSION] ` +
					`WHERE message LIKE 'fetched:%'`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for ; rows.Next(); count++ {
				var msg string
				if err := rows.Scan(&msg); err != nil {
					t.Errorf("row %d scan failed: %s", count, err)
					continue
				}
				if msg != expExplainRow {
					t.Errorf("expected %q but read %q", expExplainRow, msg)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			} else if count != 1 {
				t.Errorf("expected one row but read %d", count)
			}
		})
		t.Run("data scan", func(t *testing.T) {
			rows, err := sqlDB.Query(fmt.Sprintf(`SELECT k, a, b FROM d.t@%s;`, indexName))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for ; rows.Next(); count++ {
				var i1, i2, i3 *int
				if err := rows.Scan(&i1, &i2, &i3); err != nil {
					t.Errorf("row %d scan failed: %s", count, err)
					continue
				}
				row := fmt.Sprintf("%d %d %d", *i1, *i2, *i3)
				const expRow = "11 1 2"
				if row != expRow {
					t.Errorf("expected %q but read %q", expRow, row)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			} else if count != 1 {
				t.Errorf("expected one row but read %d", count)
			}

			if err := sqlutils.RunScrub(sqlDB, "d", "t"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Test that a backfill is executed with an EvalContext generated on the
// gateway. We assert that by checking that the same timestamp is used by all
// the backfilled columns.
func TestSchemaChangeEvalContext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numNodes = 3
	const chunkSize = 200
	const maxValue = 5000
	params, _ := tests.CreateTestServerParams()
	// Disable asynchronous schema change execution.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Split the table into multiple ranges.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	var sps []sql.SplitPoint
	for i := 1; i <= numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / numNodes * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	testCases := []struct {
		sql    string
		column string
	}{
		{"ALTER TABLE t.test ADD COLUMN x TIMESTAMP DEFAULT current_timestamp;", "x"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {

			if _, err := sqlDB.Exec(testCase.sql); err != nil {
				t.Fatal(err)
			}

			rows, err := sqlDB.Query(fmt.Sprintf(`SELECT DISTINCT %s from t.test`, testCase.column))
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("read the wrong number of rows: e = %d, v = %d", 1, count)
			}

		})
	}
}

// Tests that a schema change that is queued behind another schema change
// is executed through the synchronous execution path properly even if it
// gets to run before the first schema change.
func TestSchemaChangeCompletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	var notifySchemaChange chan struct{}
	var restartSchemaChange chan struct{}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				notify := notifySchemaChange
				restart := restartSchemaChange
				if notify != nil {
					close(notify)
					<-restart
				}
			},
			// Turn off asynchronous schema change manager.
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Add some data
	const maxValue = 200
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Do not execute the first schema change so that the second schema
	// change gets queued up behind it. The second schema change will be
	// given the green signal to execute before the first one.
	var wg sync.WaitGroup
	wg.Add(2)
	notifySchemaChange = make(chan struct{})
	restartSchemaChange = make(chan struct{})
	restart := restartSchemaChange
	go func() {
		if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX foo ON t.test (v)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notifySchemaChange

	notifySchemaChange = make(chan struct{})
	restartSchemaChange = make(chan struct{})
	go func() {
		if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX bar ON t.test (v)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notifySchemaChange
	// Allow second schema change to execute.
	close(restartSchemaChange)

	// Allow first schema change to execute after a bit.
	time.Sleep(time.Millisecond)
	close(restart)

	// Check that both schema changes have completed.
	wg.Wait()
	if err := checkTableKeyCount(ctx, kvDB, 3, maxValue); err != nil {
		t.Fatal(err)
	}

	// The notify schema change channel must be nil-ed out, or else
	// running scrub will cause it to trigger again on an already closed
	// channel when we run another statement.
	notifySchemaChange = nil
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// Test that a table TRUNCATE leaves the database in the correct state
// for the asynchronous schema changer to eventually execute it.
func TestTruncateInternals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const maxValue = 2000
	params, _ := tests.CreateTestServerParams()
	// Disable schema changes.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecNotification: asyncSchemaChangerDisabled,
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
		},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Add a zone config.
	cfg := zonepb.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("TRUNCATE TABLE t.test"); err != nil {
		t.Error(err)
	}

	// Check that SQL thinks the table is empty.
	if err := checkTableKeyCount(ctx, kvDB, 0, 0); err != nil {
		t.Fatal(err)
	}

	newTableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if newTableDesc.Adding() {
		t.Fatalf("bad state = %s", newTableDesc.State)
	}
	if err := zoneExists(sqlDB, &cfg, newTableDesc.ID); err != nil {
		t.Fatal(err)
	}

	// Ensure that the table data hasn't been deleted.
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := maxValue + 1; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
	// Check that the table descriptor exists so we know the data will
	// eventually be deleted.
	var droppedDesc *sqlbase.TableDescriptor
	if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		var err error
		droppedDesc, err = sqlbase.GetTableDescFromID(ctx, txn, tableDesc.ID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if droppedDesc == nil {
		t.Fatalf("table descriptor doesn't exist after table is truncated: %d", tableDesc.ID)
	}
	if !droppedDesc.Dropped() {
		t.Fatalf("bad state = %s", droppedDesc.State)
	}

	// Job still running, waiting for GC.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusWaitingGC, jobs.Record{
		Username:    security.RootUser,
		Description: "TRUNCATE TABLE t.public.test",
		DescriptorIDs: sqlbase.IDs{
			tableDesc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that a table truncation completes properly.
func TestTruncateCompletion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const maxValue = 2000
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			AsyncExecQuickly: true,
		},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	ctx := context.TODO()
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.pi (d DECIMAL PRIMARY KEY);
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL REFERENCES t.pi (d) DEFAULT (DECIMAL '3.14'));
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`INSERT INTO t.pi VALUES (3.14)`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Add a zone config.
	var cfg zonepb.ZoneConfig
	cfg, err := addImmediateGCZoneConfig(sqlDB, tableDesc.ID)
	if err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec("TRUNCATE TABLE t.test"); err != nil {
		t.Error(err)
	}

	// Check that SQL thinks the table is empty.
	if err := checkTableKeyCount(ctx, kvDB, 0, 0); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Ensure that the FK property still holds.
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test VALUES ($1 , $2, $3)`, maxValue+2, maxValue+2, 3.15,
	); !testutils.IsError(err, "foreign key violation|violates foreign key") {
		t.Fatalf("err = %v", err)
	}

	newTableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if newTableDesc.Adding() {
		t.Fatalf("bad state = %s", newTableDesc.State)
	}
	if err := zoneExists(sqlDB, &cfg, newTableDesc.ID); err != nil {
		t.Fatal(err)
	}

	// Wait until the older descriptor has been deleted.
	testutils.SucceedsSoon(t, func() error {
		if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			var err error
			_, err = sqlbase.GetTableDescFromID(ctx, txn, tableDesc.ID)
			return err
		}); err != nil {
			if err == sqlbase.ErrDescriptorNotFound {
				return nil
			}
			return err
		}
		return errors.Errorf("table descriptor exists after table is truncated: %d", tableDesc.ID)
	})

	if err := zoneExists(sqlDB, nil, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	// Ensure that the table data has been deleted.
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 0; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	fkTableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "pi")
	tablePrefix = roachpb.Key(keys.MakeTablePrefix(uint32(fkTableDesc.ID)))
	tableEnd = tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 1; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Ensure that the job is marked as succeeded.
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: "TRUNCATE TABLE t.public.test",
		DescriptorIDs: sqlbase.IDs{
			tableDesc.ID,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Test TRUNCATE during a column backfill.
func TestTruncateWhileColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	backfillNotification := make(chan struct{})
	backfillCount := int64(0)
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		// Runs schema changes asynchronously.
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecQuickly: true,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				switch atomic.LoadInt64(&backfillCount) {
				case 3:
					// Notify in the middle of a backfill.
					if backfillNotification != nil {
						close(backfillNotification)
						backfillNotification = nil
					}
					// Never complete the backfill.
					return context.DeadlineExceeded
				default:
					atomic.AddInt64(&backfillCount, 1)
				}
				return nil
			},
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	const maxValue = 5000
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	notify := backfillNotification

	const add_column = `ALTER TABLE t.public.test ADD COLUMN x DECIMAL NOT NULL DEFAULT 1.4::DECIMAL, ADD CHECK (x >= 0)`
	if _, err := sqlDB.Exec(add_column); err != nil {
		t.Fatal(err)
	}

	const drop_column = `ALTER TABLE t.public.test DROP COLUMN v`
	if _, err := sqlDB.Exec(drop_column); err != nil {
		t.Fatal(err)
	}

	// Check that an outstanding schema change exists.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	oldID := tableDesc.ID
	if lenMutations := len(tableDesc.Mutations); lenMutations != 3 {
		t.Fatalf("%d outstanding schema change", lenMutations)
	}

	// Run TRUNCATE.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-notify
		if _, err := sqlDB.Exec("TRUNCATE TABLE t.test"); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	wg.Wait()

	// The new table is truncated.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 0; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Col "x" is public and col "v" is dropped.
	if num := len(tableDesc.Mutations); num > 0 {
		t.Fatalf("%d outstanding mutation", num)
	}
	if lenCols := len(tableDesc.Columns); lenCols != 2 {
		t.Fatalf("%d columns", lenCols)
	}
	if k, x := tableDesc.Columns[0].Name, tableDesc.Columns[1].Name; k != "k" && x != "x" {
		t.Fatalf("columns %q, %q in descriptor", k, x)
	}
	if checks := tableDesc.AllActiveAndInactiveChecks(); len(checks) != 1 {
		t.Fatalf("expected 1 check, found %d", len(checks))
	}

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	if err := jobutils.VerifySystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: add_column,
		DescriptorIDs: sqlbase.IDs{
			oldID,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := jobutils.VerifySystemJob(t, sqlRun, 1, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
		Username:    security.RootUser,
		Description: drop_column,
		DescriptorIDs: sqlbase.IDs{
			oldID,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that, when DDL statements are run in a transaction, their errors are
// received as the results of the commit statement.
func TestSchemaChangeErrorOnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
INSERT INTO t.test (k, v) VALUES (1, 99), (2, 99);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// This schema change is invalid because of the duplicate v, but its error is
	// only reported later.
	if _, err := tx.Exec("ALTER TABLE t.test ADD CONSTRAINT v_unique UNIQUE (v)"); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); !testutils.IsError(
		err, `duplicate key value`,
	) {
		t.Fatal(err)
	}
}

// TestIndexBackfillAfterGC verifies that if a GC is done after an index
// backfill has started, it will move past the error and complete.
func TestIndexBackfillAfterGC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var tc serverutils.TestClusterInterface
	ctx := context.Background()
	runGC := func(sp roachpb.Span) error {
		if tc == nil {
			return nil
		}
		gcr := roachpb.GCRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(sp),
			Threshold:     tc.Server(0).Clock().Now(),
		}
		_, err := client.SendWrapped(ctx, tc.Server(0).DistSenderI().(*kv.DistSender), &gcr)
		if err != nil {
			panic(err)
		}
		return nil
	}

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if fn := runGC; fn != nil {
					runGC = nil
					return fn(sp)
				}
				return nil
			},
		},
	}

	tc = serverutils.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(context.TODO())
	db := tc.ServerConn(0)
	kvDB := tc.Server(0).DB()
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE t`)
	sqlDB.Exec(t, `CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'))`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES (1, 1)`)
	if _, err := db.Exec(`CREATE UNIQUE INDEX foo ON t.test (v)`); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(context.TODO(), kvDB, 2, 0); err != nil {
		t.Fatal(err)
	}
}

// TestAddComputedColumn verifies that while a column backfill is happening
// for a computed column, INSERTs and UPDATEs for that column are correct.
func TestAddComputedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var db *gosql.DB
	done := false
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if db == nil || done {
					return nil
				}
				done = true
				if _, err := db.Exec(`INSERT INTO t.test VALUES (10)`); err != nil {
					panic(err)
				}
				if _, err := db.Exec(`UPDATE t.test SET a = a + 1 WHERE a < 10`); err != nil {
					panic(err)
				}
				return nil
			},
		},
	}

	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(context.TODO())
	db = tc.ServerConn(0)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE t`)
	sqlDB.Exec(t, `CREATE TABLE t.test (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO t.test VALUES (1)`)
	sqlDB.Exec(t, `ALTER TABLE t.test ADD COLUMN b INT AS (a + 5) STORED`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM t.test ORDER BY a`, [][]string{{"2", "7"}, {"10", "15"}})
}

func TestSchemaChangeAfterCreateInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// The schema change below can occasionally take more than
	// 5 seconds and gets pushed by the closed timestamp mechanism
	// in the read timestamp cache. Setting the closed timestamp
	// target duration to a higher value.
	// TODO(vivek): Remove the need to do this by removing the use of
	// txn.CommitTimestamp() in schema changes.
	if _, err := sqlDB.Exec(`
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '20s'
`); err != nil {
		t.Fatal(err)
	}

	// A large enough value that the backfills run as part of the
	// schema change run in many chunks.
	var maxValue = 4001
	if util.RaceEnabled {
		// Race builds are a lot slower, so use a smaller number of rows.
		maxValue = 200
	}

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`CREATE TABLE t.testing (k INT PRIMARY KEY, v INT, INDEX foo(v), CONSTRAINT ck_k CHECK (k >= 0));`); err != nil {
		t.Fatal(err)
	}

	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, maxValue-i)
	}

	if _, err := tx.Exec(`INSERT INTO t.testing VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`ALTER TABLE t.testing RENAME TO t.test`); err != nil {
		t.Fatal(err)
	}

	// Run schema changes that execute Column, Check and Index backfills.
	if _, err := tx.Exec(`
ALTER TABLE t.test ADD COLUMN c INT AS (v + 4) STORED, ADD COLUMN d INT DEFAULT 23, ADD CONSTRAINT bar UNIQUE (c), DROP CONSTRAINT ck_k, ADD CONSTRAINT ck_c CHECK (c >= 4)
`); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`DROP INDEX t.test@foo`); err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(context.TODO(), kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}

	// Verify that the index bar over is consistent, and that columns c, d
	// have been backfilled properly.
	rows, err := sqlDB.Query(`SELECT c, d from t.test@bar`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var c int
		var d int
		if err := rows.Scan(&c, &d); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count+4 != c {
			t.Errorf("e = %d, v = %d", count, c)
		}
		if d != 23 {
			t.Errorf("e = %d, v = %d", 23, d)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	eCount := maxValue + 1
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Constraint ck_k dropped, ck_c public.
	if _, err := sqlDB.Exec(fmt.Sprintf("INSERT INTO t.test (k, v) VALUES (-1, %d)", maxValue+10)); err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("INSERT INTO t.test (k, v) VALUES (%d, -1)", maxValue+10)
	if _, err := sqlDB.Exec(q); !testutils.IsError(err,
		`failed to satisfy CHECK constraint \(c >= 4\)`) {
		t.Fatalf("err = %+v", err)
	}

	// The descriptor version hasn't changed.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if tableDesc.Version != 1 {
		t.Fatalf("invalid version = %d", tableDesc.Version)
	}
}

// TestCancelSchemaChange tests that a CANCEL JOB run midway through column
// and index backfills is canceled.
func TestCancelSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		numNodes = 3
		maxValue = 100
	)

	var sqlDB *sqlutils.SQLRunner
	var db *gosql.DB
	params, _ := tests.CreateTestServerParams()
	doCancel := false
	var enableAsyncSchemaChanges uint32
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			WriteCheckpointInterval: time.Nanosecond, // checkpoint after every chunk.
			AsyncExecNotification: func() error {
				if enable := atomic.LoadUint32(&enableAsyncSchemaChanges); enable == 0 {
					return errors.New("async schema changes are disabled")
				}
				return nil
			},
			AsyncExecQuickly:  true,
			BackfillChunkSize: 10,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if !doCancel {
					return nil
				}
				if _, err := db.Exec(`CANCEL JOB (
					SELECT job_id FROM [SHOW JOBS]
					WHERE
						job_type = 'SCHEMA CHANGE' AND
						status = $1 AND
						description NOT LIKE 'ROLL BACK%'
				)`, jobs.StatusRunning); err != nil {
					panic(err)
				}
				return nil
			},
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      params,
	})
	defer tc.Stopper().Stop(context.TODO())
	db = tc.ServerConn(0)
	kvDB := tc.Server(0).DB()
	sqlDB = sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
	`)

	// Bulk insert.
	if err := bulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	// Split the table into multiple ranges.
	var sps []sql.SplitPoint
	const numSplits = numNodes * 2
	for i := 1; i <= numSplits-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i % numNodes, Vals: []interface{}{maxValue / numSplits * i}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	ctx := context.TODO()
	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		sql string
		// Set to true if this schema change is to be canceled.
		cancel bool
		// Set to true if the rollback returns in a running, waiting status.
		isGC bool
	}{
		{`ALTER TABLE t.public.test ADD COLUMN x DECIMAL DEFAULT 1.4::DECIMAL CREATE FAMILY f2, ADD CHECK (x >= 0)`,
			true, false},
		{`CREATE INDEX foo ON t.public.test (v)`,
			true, true},
		{`ALTER TABLE t.public.test ADD COLUMN x DECIMAL DEFAULT 1.2::DECIMAL CREATE FAMILY f3, ADD CHECK (x >= 0)`,
			false, false},
		{`CREATE INDEX foo ON t.public.test (v)`,
			false, true},
	}

	idx := 0
	for _, tc := range testCases {
		doCancel = tc.cancel
		if doCancel {
			if _, err := db.Exec(tc.sql); !testutils.IsError(err, "job canceled") {
				t.Fatalf("unexpected %v", err)
			}
			if err := jobutils.VerifySystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, jobs.StatusCanceled, jobs.Record{
				Username:    security.RootUser,
				Description: tc.sql,
				DescriptorIDs: sqlbase.IDs{
					tableDesc.ID,
				},
			}); err != nil {
				t.Fatal(err)
			}
			jobID := jobutils.GetJobID(t, sqlDB, idx)
			idx++
			jobRecord := jobs.Record{
				Username:    security.RootUser,
				Description: fmt.Sprintf("ROLL BACK JOB %d: %s", jobID, tc.sql),
				DescriptorIDs: sqlbase.IDs{
					tableDesc.ID,
				},
			}
			var err error
			if tc.isGC {
				err = jobutils.VerifyRunningSystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, sql.RunningStatusWaitingGC, jobRecord)
			} else {
				err = jobutils.VerifySystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobRecord)
			}
			if err != nil {
				t.Fatal(err)
			}
		} else {
			sqlDB.Exec(t, tc.sql)
			if err := jobutils.VerifySystemJob(t, sqlDB, idx, jobspb.TypeSchemaChange, jobs.StatusSucceeded, jobs.Record{
				Username:    security.RootUser,
				Description: tc.sql,
				DescriptorIDs: sqlbase.IDs{
					tableDesc.ID,
				},
			}); err != nil {
				t.Fatal(err)
			}
		}
		idx++
	}

	// Verify that the index foo over v is consistent, and that column x has
	// been backfilled properly.
	rows, err := db.Query(`SELECT v, x from t.test@foo`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	count := 0
	for ; rows.Next(); count++ {
		var val int
		var x float64
		if err := rows.Scan(&val, &x); err != nil {
			t.Errorf("row %d scan failed: %s", count, err)
			continue
		}
		if count != val {
			t.Errorf("e = %d, v = %d", count, val)
		}
		if x != 1.2 {
			t.Errorf("e = %f, v = %f", 1.2, x)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	eCount := maxValue + 1
	if eCount != count {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", eCount, count)
	}

	// Verify that the data from the canceled CREATE INDEX is cleaned up.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)
	if _, err := addImmediateGCZoneConfig(db, tableDesc.ID); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		return checkTableKeyCount(ctx, kvDB, 3, maxValue)
	})

	// Check that constraints are cleaned up.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if checks := tableDesc.AllActiveAndInactiveChecks(); len(checks) != 1 {
		t.Fatalf("expected 1 check, found %+v", checks)
	}
}

// This test checks that when a transaction containing schema changes
// needs to be retried it gets retried internal to cockroach. This test
// currently fails because a schema changeg transaction is not retried.
func TestSchemaChangeRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const numNodes = 3

	params, _ := tests.CreateTestServerParams()

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	sqlDB := tc.ServerConn(0)

	if _, err := sqlDB.Exec(`
 CREATE DATABASE t;
 CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
 `); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// The timestamp of the transaction is guaranteed to be fixed after
	// this statement.
	if _, err := tx.Exec(`
		CREATE TABLE t.another (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
		`); err != nil {
		t.Fatal(err)
	}

	otherSQLDB := tc.ServerConn(1)

	// Read schema on another node that picks a later timestamp.
	rows, err := otherSQLDB.Query("SELECT * FROM t.test")
	if err != nil {
		t.Fatal(err)
	}
	rows.Close()

	if _, err := tx.Exec(`
		CREATE UNIQUE INDEX vidx ON t.test (v);
		`); err != nil {
		t.Fatal(err)
	}

	// The transaction should get pushed and commit without an error.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestCancelSchemaChangeContext tests that a canceled context on
// the session with a schema change after the schema change transaction
// has committed will not indefinitely retry executing the post schema
// execution transactions using a canceled context. The schema
// change will give up and ultimately be executed to completion through
// the asynchronous schema changer.
func TestCancelSchemaChangeContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxValue = 100
	notifyBackfill := make(chan struct{})
	cancelSessionDone := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	seenContextCancel := false
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if notify := notifyBackfill; notify != nil {
					notifyBackfill = nil
					close(notify)
					<-cancelSessionDone
				}
				return nil
			},
			OnError: func(err error) {
				if err == context.Canceled && !seenContextCancel {
					seenContextCancel = true
					return
				}
				t.Errorf("saw unexpected error: %+v", err)
			},
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	`)

	// Bulk insert.
	if err := bulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()
	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	notification := notifyBackfill

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx := context.TODO()
		// When using db.Exec(), CANCEL SESSION below will result in the
		// database client retrying the request on another connection.
		// Use a connection here so when the session gets canceled; a
		// connection failure is returned.
		// TODO(vivek): It's likely we need to vendor lib/pq#422 and check
		// that this is unnecessary.
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Error(err)
		}
		if _, err := conn.ExecContext(
			ctx, `CREATE INDEX foo ON t.public.test (v)`); err != driver.ErrBadConn {
			t.Errorf("unexpected err = %+v", err)
		}
	}()

	<-notification

	if _, err := db.Exec(`
CANCEL SESSIONS (SELECT session_id FROM [SHOW SESSIONS] WHERE last_active_query LIKE 'CREATE INDEX%')
`); err != nil {
		t.Error(err)
	}

	close(cancelSessionDone)

	wg.Wait()

	if !seenContextCancel {
		t.Fatal("didnt see context cancel error")
	}
}

func TestSchemaChangeGRPCError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxValue = 100
	params, _ := tests.CreateTestServerParams()
	seenNodeUnavailable := false
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if !seenNodeUnavailable {
					seenNodeUnavailable = true
					return errors.Errorf("node unavailable")
				}
				return nil
			},
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	`)

	// Bulk insert.
	if err := bulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()
	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`CREATE INDEX foo ON t.public.test (v)`); err != nil {
		t.Fatal(err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
}

// TestBlockedSchemaChange tests whether a schema change that
// has no data backfill processing will be blocked by a schema
// change that is holding the schema change lease while backfill
// processing.
func TestBlockedSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const maxValue = 100
	notifyBackfill := make(chan struct{})
	tableRenameDone := make(chan struct{})

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				if notify := notifyBackfill; notify != nil {
					notifyBackfill = nil
					close(notify)
					<-tableRenameDone
				}
				return nil
			},
		},
	}
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
	`)

	// Bulk insert.
	if err := bulkInsertIntoTable(db, maxValue); err != nil {
		t.Fatal(err)
	}

	ctx := context.TODO()
	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	notification := notifyBackfill

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := db.Exec(`CREATE INDEX foo ON t.public.test (v)`); err != nil {
			t.Error(err)
		}
	}()

	<-notification

	if _, err := db.Exec(`ALTER TABLE t.test RENAME TO t.newtest`); err != nil {
		t.Fatal(err)
	}

	close(tableRenameDone)

	if _, err := db.Query(`SELECT x from t.test`); !testutils.IsError(err, `relation "t.test" does not exist`) {
		t.Fatalf("err = %+v", err)
	}

	wg.Wait()
}

// Tests index backfill validation step by purposely deleting an index
// value during the index backfill and checking that the validation
// fails.
func TestIndexBackfillValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const maxValue = 1000
	backfillCount := int64(0)
	var db *client.DB
	var tableDesc *sqlbase.TableDescriptor
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: maxValue / 5,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunAfterBackfillChunk: func() {
				count := atomic.AddInt64(&backfillCount, 1)
				if count == 2 {
					// drop an index value before validation.
					keyEnc := keys.MakeTablePrefix(uint32(tableDesc.ID))
					key := roachpb.Key(encoding.EncodeUvarintAscending(keyEnc, uint64(tableDesc.NextIndexID)))
					kv, err := db.Scan(context.TODO(), key, key.PrefixEnd(), 1)
					if err != nil {
						t.Error(err)
					}
					if err := db.Del(context.TODO(), kv[0].Key); err != nil {
						t.Error(err)
					}
				}
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	db = kvDB
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Bulk insert enough rows to exceed the chunk size.
	inserts := make([]string, maxValue+1)
	for i := 0; i < maxValue+1; i++ {
		inserts[i] = fmt.Sprintf(`(%d, %d)`, i, i)
	}
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ` + strings.Join(inserts, ",")); err != nil {
		t.Fatal(err)
	}

	// Start schema change that eventually runs a backfill.
	if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX foo ON t.test (v)`); !testutils.IsError(
		err, fmt.Sprintf("%d entries, expected %d violates unique constraint", maxValue, maxValue+1),
	) {
		t.Fatal(err)
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if len(tableDesc.Indexes) > 0 || len(tableDesc.Mutations) > 0 {
		t.Fatalf("descriptor broken %d, %d", len(tableDesc.Indexes), len(tableDesc.Mutations))
	}
}

// Tests inverted index backfill validation step by purposely deleting an index
// value during the index backfill and checking that the validation fails.
func TestInvertedIndexBackfillValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const maxValue = 1000
	backfillCount := int64(0)
	var db *client.DB
	var tableDesc *sqlbase.TableDescriptor
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: maxValue / 5,
		},
		DistSQL: &execinfra.TestingKnobs{
			RunAfterBackfillChunk: func() {
				count := atomic.AddInt64(&backfillCount, 1)
				if count == 2 {
					// drop an index value before validation.
					keyEnc := keys.MakeTablePrefix(uint32(tableDesc.ID))
					key := roachpb.Key(encoding.EncodeUvarintAscending(keyEnc, uint64(tableDesc.NextIndexID)))
					kv, err := db.Scan(context.TODO(), key, key.PrefixEnd(), 1)
					if err != nil {
						t.Error(err)
					}
					if err := db.Del(context.TODO(), kv[0].Key); err != nil {
						t.Error(err)
					}
				}
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	db = kvDB
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v JSON);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	// Insert enough rows to exceed the chunk size.
	for i := 0; i < maxValue+1; i++ {
		jsonVal, err := json.Random(20, r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2)`, i, jsonVal.String()); err != nil {
			t.Fatal(err)
		}
	}

	// Start schema change that eventually runs a backfill.
	if _, err := sqlDB.Exec(`CREATE INVERTED INDEX foo ON t.test (v)`); !testutils.IsError(
		err, "validation of index foo failed",
	) {
		t.Fatal(err)
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if len(tableDesc.Indexes) > 0 || len(tableDesc.Mutations) > 0 {
		t.Fatalf("descriptor broken %d, %d", len(tableDesc.Indexes), len(tableDesc.Mutations))
	}
}

// Test multiple index backfills (for forward and inverted indexes) from the
// same transaction.
func TestMultipleIndexBackfills(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	const maxValue = 1000
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: maxValue / 5,
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (a INT, b INT, c JSON, d JSON);
`); err != nil {
		t.Fatal(err)
	}

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	// Insert enough rows to exceed the chunk size.
	for i := 0; i < maxValue+1; i++ {
		jsonVal1, err := json.Random(20, r)
		if err != nil {
			t.Fatal(err)
		}
		jsonVal2, err := json.Random(20, r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2, $3, $4)`, i, i, jsonVal1.String(), jsonVal2.String()); err != nil {
			t.Fatal(err)
		}
	}

	// Start schema changes.
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INDEX idx_a ON t.test (a)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INDEX idx_b ON t.test (b)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INVERTED INDEX idx_c ON t.test (c)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`CREATE INVERTED INDEX idx_d ON t.test (d)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateStatsAfterSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func(oldRefreshInterval, oldAsOf time.Duration) {
		stats.DefaultRefreshInterval = oldRefreshInterval
		stats.DefaultAsOfTime = oldAsOf
	}(stats.DefaultRefreshInterval, stats.DefaultAsOfTime)
	stats.DefaultRefreshInterval = time.Millisecond
	stats.DefaultAsOfTime = time.Microsecond

	server, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(context.TODO())
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)

	sqlRun.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=false`)

	sqlRun.Exec(t, `
		CREATE DATABASE t;
		CREATE TABLE t.test (k INT PRIMARY KEY, v CHAR, w CHAR);`)

	sqlRun.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled=true`)

	// Add an index.
	sqlRun.Exec(t, `CREATE INDEX foo ON t.test (w)`)

	// Verify that statistics have been created for the new index (note that
	// column w is ordered before column v, since index columns are added first).
	sqlRun.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE t.test]`,
		[][]string{
			{"__auto__", "{k}", "0", "0", "0"},
			{"__auto__", "{w}", "0", "0", "0"},
			{"__auto__", "{v}", "0", "0", "0"},
		})

	// Add a column.
	sqlRun.Exec(t, `ALTER TABLE t.test ADD COLUMN x INT`)

	// Verify that statistics have been created for the new column.
	sqlRun.CheckQueryResultsRetry(t,
		`SELECT statistics_name, column_names, row_count, distinct_count, null_count
	  FROM [SHOW STATISTICS FOR TABLE t.test] ORDER BY column_names::STRING`,
		[][]string{
			{"__auto__", "{k}", "0", "0", "0"},
			{"__auto__", "{k}", "0", "0", "0"},
			{"__auto__", "{v}", "0", "0", "0"},
			{"__auto__", "{v}", "0", "0", "0"},
			{"__auto__", "{w}", "0", "0", "0"},
			{"__auto__", "{w}", "0", "0", "0"},
			{"__auto__", "{x}", "0", "0", "0"},
		})
}

func TestTableValidityWhileAddingFK(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.child (a INT PRIMARY KEY, b INT, INDEX (b));
CREATE TABLE t.parent (a INT PRIMARY KEY);
`); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.child ADD FOREIGN KEY (b) REFERENCES t.child (a), ADD FOREIGN KEY (a) REFERENCES t.parent (a)`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.child`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.parent`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.child`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.parent`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "child"); err != nil {
		t.Fatal(err)
	}
	if err := sqlutils.RunScrub(sqlDB, "t", "parent"); err != nil {
		t.Fatal(err)
	}
}

// TestWritesWithChecksBeforeDefaultColumnBackfill tests that when a check on a
// column being added references a different public column, writes to the public
// column ignore the constraint before the backfill for the non-public column
// begins. See #35258.
func TestWritesWithChecksBeforeDefaultColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT PRIMARY KEY NOT NULL,
    v INT NOT NULL
);
`); err != nil {
		t.Fatal(err)
	}

	if err := bulkInsertIntoTable(sqlDB, 1000); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN a INT DEFAULT 0, ADD CHECK (a < v AND a IS NOT NULL)`)
		if !testutils.IsError(err, `validation of CHECK "\(a < v\) AND \(a IS NOT NULL\)" failed on row: k=1003, v=-1003, a=0`) {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1001, 1001)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 100 WHERE v < 100`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1002, 1002)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 200 WHERE v < 200`); err != nil {
		t.Fatal(err)
	}
	// Final insert violates the constraint
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1003, -1003)`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

// TestWritesWithChecksBeforeComputedColumnBackfill tests that when a check on a
// column being added references a different public column, writes to the public
// column ignore the constraint before the backfill for the non-public column
// begins. See #35258.
func TestWritesWithChecksBeforeComputedColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()

	publishWriteNotification := make(chan struct{})
	continuePublishWriteNotification := make(chan struct{})

	backfillNotification := make(chan struct{})
	continueBackfillNotification := make(chan struct{})

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforePublishWriteAndDelete: func() {
				if publishWriteNotification != nil {
					// Notify before the delete and write only state is published.
					close(publishWriteNotification)
					publishWriteNotification = nil
					<-continuePublishWriteNotification
				}
			},
			RunBeforeBackfill: func() error {
				if backfillNotification != nil {
					// Notify before the backfill begins.
					close(backfillNotification)
					backfillNotification = nil
					<-continueBackfillNotification
				}
				return nil
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT PRIMARY KEY NOT NULL,
    v INT NOT NULL
);
`); err != nil {
		t.Fatal(err)
	}

	if err := bulkInsertIntoTable(sqlDB, 1000); err != nil {
		t.Fatal(err)
	}

	n1 := publishWriteNotification
	n2 := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		_, err := sqlDB.Exec(`ALTER TABLE t.test ADD COLUMN a INT AS (v - 1) STORED, ADD CHECK (a < v AND a > -1000 AND a IS NOT NULL)`)
		if !testutils.IsError(err, `validation of CHECK "\(\(a < v\) AND \(a > -1000\)\) AND \(a IS NOT NULL\)" failed on row: k=1003, v=-1003, a=-1004`) {
			t.Error(err)
		}
		wg.Done()
	}()

	<-n1
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1001, 1001)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 100 WHERE v < 100`); err != nil {
		t.Fatal(err)
	}
	close(continuePublishWriteNotification)

	<-n2
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1002, 1002)`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`UPDATE t.test SET v = 200 WHERE v < 200`); err != nil {
		t.Fatal(err)
	}
	// Final insert violates the constraint
	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES (1003, -1003)`); err != nil {
		t.Fatal(err)
	}
	close(continueBackfillNotification)

	wg.Wait()

	if err := sqlutils.RunScrub(sqlDB, "t", "test"); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaChangeJobRunningStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	var scNotification chan struct{}
	var runBeforeBackfill func() error
	var runBeforeBackfillChunk func() error
	var runBeforeIndexValidation func() error
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(_ sql.TestingSchemaChangerCollection) {
				if scNotification != nil {
					notify := scNotification
					scNotification = nil
					// Notify that the schema change is about to run and
					// so the job has been created in the jobs table.
					close(notify)
				}
			},
			RunBeforeBackfill: func() error {
				return runBeforeBackfill()
			},
			RunBeforeIndexValidation: func() error {
				return runBeforeIndexValidation()
			},
		},
		DistSQL: &execinfra.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				return runBeforeBackfillChunk()
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
INSERT INTO t.test (k, v) VALUES (1, 99), (2, 100);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Acquire a lease on the table to block the schema changer.
	if _, err := tx.Exec("SELECT count(*) FROM t.test"); err != nil {
		t.Fatal(err)
	}

	scNotification = make(chan struct{})
	notify := scNotification

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := sqlDB.Exec("CREATE INDEX idx_v ON t.test(v)"); err != nil {
			t.Error(err)
		}
	}()

	<-notify

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	testutils.SucceedsSoon(t, func() error {
		return jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusDeleteOnly, jobs.Record{
			Username:    security.RootUser,
			Description: "CREATE INDEX idx_v ON t.public.test (v)",
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	})

	runBeforeBackfill = func() error {
		return jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusDeleteAndWriteOnly, jobs.Record{
			Username:    security.RootUser,
			Description: "CREATE INDEX idx_v ON t.public.test (v)",
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	}

	runBeforeBackfillChunk = func() error {
		return jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusBackfill, jobs.Record{
			Username:    security.RootUser,
			Description: "CREATE INDEX idx_v ON t.public.test (v)",
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	}

	runBeforeIndexValidation = func() error {
		return jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusValidation, jobs.Record{
			Username:    security.RootUser,
			Description: "CREATE INDEX idx_v ON t.public.test (v)",
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	}

	// release the lease on the table to allow the schema change to move forward.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestIntentRaceWithIndexBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var readyToBackfill, canStartBackfill, backfillProgressing chan struct{}

	const numNodes = 1
	var maxValue = 2000

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: 100,
			RunBeforeBackfill: func() error {
				select {
				case <-readyToBackfill:
				default:
					close(readyToBackfill)
					<-canStartBackfill
				}
				return nil
			},
		},
		DistSQL: &execinfra.TestingKnobs{
			RunAfterBackfillChunk: func() {
				select {
				case <-backfillProgressing:
				default:
					close(backfillProgressing)
				}
			},
		},
	}

	tc := serverutils.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs:      params,
		})
	defer tc.Stopper().Stop(context.TODO())
	kvDB := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)

	ctx, cancel := context.WithCancel(context.TODO())

	readyToBackfill = make(chan struct{})
	canStartBackfill = make(chan struct{})
	backfillProgressing = make(chan struct{})

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	var sps []sql.SplitPoint
	for i := 1; i < numNodes-1; i++ {
		sps = append(sps, sql.SplitPoint{TargetNodeIdx: i, Vals: []interface{}{maxValue / 2}})
	}
	sql.SplitTable(t, tc, tableDesc, sps)

	bg := ctxgroup.WithContext(ctx)
	bg.Go(func() error {
		if _, err := sqlDB.ExecContext(ctx, "CREATE UNIQUE INDEX ON t.test(v)"); err != nil {
			cancel()
			return err
		}
		return nil
	})

	// Wait until the schema change backfill starts.
	select {
	case <-readyToBackfill:
	case <-ctx.Done():
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE t.test SET v = $2 WHERE k = $1`, maxValue-1, maxValue-1); err != nil {
		t.Error(err)
	}
	if _, err := tx.Exec(`DELETE FROM t.test WHERE k = $1`, maxValue-1); err != nil {
		t.Error(err)
	}

	close(canStartBackfill)

	bg.Go(func() error {
		// We need to give the schema change time in which it could progress and end
		// up writing between our intent and its write before we rollback and the
		// intent is cleaned up. At the same time, we need to rollback so that a
		// correct schema change -- which waits for any intents -- will eventually
		// proceed and not block the test forever.
		time.Sleep(50 * time.Millisecond)
		return tx.Rollback()
	})

	select {
	case <-backfillProgressing:
	case <-ctx.Done():
	}

	rows, err := sqlDB.Query(`
	SELECT t.range_id, t.start_key_pretty, t.status, t.detail
	FROM
	crdb_internal.check_consistency(false, '', '') as t
	WHERE t.status NOT IN ('RANGE_CONSISTENT', 'RANGE_INDETERMINATE', 'RANGE_CONSISTENT_STATS_ESTIMATED')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var rangeID int32
		var prettyKey, status, detail string
		if err := rows.Scan(&rangeID, &prettyKey, &status, &detail); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("r%d (%s) is inconsistent: %s %s", rangeID, prettyKey, status, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if err := bg.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaChangeJobRunningStatusValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	var runBeforeConstraintValidation func() error
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeConstraintValidation: func() error {
				return runBeforeConstraintValidation()
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
INSERT INTO t.test (k, v) VALUES (1, 99), (2, 100);
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	runBeforeConstraintValidation = func() error {
		return jobutils.VerifyRunningSystemJob(t, sqlRun, 0, jobspb.TypeSchemaChange, sql.RunningStatusValidation, jobs.Record{
			Username:    security.RootUser,
			Description: "ALTER TABLE t.public.test ADD COLUMN a INT8 AS (v - 1) STORED, ADD CHECK ((a < v) AND (a IS NOT NULL))",
			DescriptorIDs: sqlbase.IDs{
				tableDesc.ID,
			},
		})
	}

	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test ADD COLUMN a INT AS (v - 1) STORED, ADD CHECK (a < v AND a IS NOT NULL)`,
	); err != nil {
		t.Fatal(err)
	}
}

// TestFKReferencesAddedOnlyOnceOnRetry verifies that if ALTER TABLE ADD FOREIGN
// KEY is retried, both the FK reference and backreference (on another table)
// are only added once. This is addressed by #38377.
func TestFKReferencesAddedOnlyOnceOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	var runBeforeConstraintValidation func() error
	errorReturned := false
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeConstraintValidation: func() error {
				return runBeforeConstraintValidation()
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE TABLE t.test2 (k INT, INDEX (k));
`); err != nil {
		t.Fatal(err)
	}

	// After FK forward references and backreferences are installed, and before
	// the validation query is run, return an error so that the schema change
	// has to be retried. The error is only returned on the first try.
	runBeforeConstraintValidation = func() error {
		if !errorReturned {
			errorReturned = true
			return context.DeadlineExceeded

		}
		return nil
	}
	if _, err := sqlDB.Exec(`
ALTER TABLE t.test2 ADD FOREIGN KEY (k) REFERENCES t.test;
`); err != nil {
		t.Fatal(err)
	}

	// Table descriptor validation failures, resulting from, e.g., broken or
	// duplicated backreferences, are returned by SHOW CONSTRAINTS.
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.test`); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Query(`SHOW CONSTRAINTS FROM t.test2`); err != nil {
		t.Fatal(err)
	}
}
