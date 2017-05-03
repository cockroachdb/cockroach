// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql_test

import (
	gosql "database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// asyncSchemaChangerDisabled can be used to disable asynchronous processing
// of schema changes.
func asyncSchemaChangerDisabled() error {
	return errors.New("async schema changer disabled")
}

func TestSchemaChangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	// Set MinSchemaChangeLeaseDuration to always expire the lease.
	minLeaseDuration := sql.MinSchemaChangeLeaseDuration
	sql.MinSchemaChangeLeaseDuration = 2 * sql.SchemaChangeLeaseDuration
	defer func() {
		sql.MinSchemaChangeLeaseDuration = minLeaseDuration
	}()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	var lease sqlbase.TableDescriptor_SchemaChangeLease
	var id = sqlbase.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	changer := sql.NewSchemaChangerForTesting(id, 0, node, *kvDB, nil)

	ctx := context.TODO()

	// Acquire a lease.
	lease, err := changer.AcquireLease(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, lease.ExpirationTime))
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

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, lease.ExpirationTime))
	}

	// The new lease is a brand new lease.
	if oldLease == lease {
		t.Fatalf("lease was not extended: %v", lease)
	}

	// Extending an old lease fails.
	if err := changer.ExtendLease(ctx, &oldLease); !testutils.IsError(err, "table: .* has lease") {
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

	// Set MinSchemaChangeLeaseDuration to not expire the lease.
	sql.MinSchemaChangeLeaseDuration = minLeaseDuration
	oldLease = lease
	if err := changer.ExtendLease(ctx, &lease); err != nil {
		t.Fatal(err)
	}
	// The old lease is renewed.
	if oldLease != lease {
		t.Fatalf("acquired new lease: %v, old lease: %v", lease, oldLease)
	}
}

func validExpirationTime(expirationTime int64) bool {
	now := timeutil.Now()
	return expirationTime > now.Add(sql.LeaseDuration/2).UnixNano() && expirationTime < now.Add(sql.LeaseDuration*3/2).UnixNano()
}

func TestSchemaChangeProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer sql.TestDisableTableLeases()()

	params, _ := createTestServerParams()
	// Disable external processing of mutations.
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	var id = sqlbase.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	stopper := stop.NewStopper()
	leaseMgr := sql.NewLeaseManager(
		&base.NodeIDContainer{},
		*kvDB,
		hlc.NewClock(hlc.UnixNano, time.Nanosecond),
		sql.LeaseManagerTestingKnobs{},
		stopper,
		&sql.MemoryMetrics{},
	)
	defer stopper.Stop(context.TODO())
	changer := sql.NewSchemaChangerForTesting(id, 0, node, *kvDB, leaseMgr)

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

	desc, err := changer.MaybeIncrementVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tableDesc = desc.GetTable()
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Check that MaybeIncrementVersion increments the version
	// correctly.
	expectedVersion++
	tableDesc.UpVersion = true
	if err := kvDB.Put(
		ctx,
		sqlbase.MakeDescMetadataKey(tableDesc.ID),
		sqlbase.WrapDescriptor(tableDesc),
	); err != nil {
		t.Fatal(err)
	}

	desc, err = changer.MaybeIncrementVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tableDesc = desc.GetTable()
	savedTableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	newVersion = tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version in returned desc; e = %d, v = %d", expectedVersion, newVersion)
	}
	newVersion = savedTableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version in saved desc; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Check that RunStateMachineBeforeBackfill doesn't do anything
	// if there are no mutations queued.
	if err := changer.RunStateMachineBeforeBackfill(ctx); err != nil {
		t.Fatal(err)
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	newVersion = tableDesc.Version
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
	changer = sql.NewSchemaChangerForTesting(id, tableDesc.NextMutationID, node, *kvDB, leaseMgr)
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
		expectedState := sqlbase.DescriptorMutation_WRITE_ONLY
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
	params, _ := createTestServerParams()
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
	mTest.CheckQueryResults(indexQuery, [][]string{{"b"}, {"d"}})

	// Ensure that the version has been incremented.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Apply a schema change that only sets the UpVersion bit.
	expectedVersion = newVersion + 1

	mTest.Exec(`ALTER INDEX t.test@foo RENAME TO ufo`)

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
		mTest.Exec(fmt.Sprintf(`CREATE INDEX foo%d ON t.test (v)`, i))
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
		mTest.CheckQueryResults(indexQuery, [][]string{{"b"}, {"d"}})
	}
}

func checkTableKeyCount(ctx context.Context, kvDB *client.DB, multiple int, maxValue int) error {
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		return err
	} else if e := multiple * (maxValue + 1); len(kvs) != e {
		return errors.Errorf("expected %d key value pairs, but got %d", e, len(kvs))
	}
	return nil
}

// Run a particular schema change and run some OLTP operations in parallel, as
// soon as the schema change starts executing its backfill.
func runSchemaChangeWithOperations(
	t *testing.T,
	sqlDB *gosql.DB,
	kvDB *client.DB,
	schemaChange string,
	maxValue int,
	keyMultiple int,
	backfillNotification chan struct{},
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
	sc := sql.NewSchemaChangerForTesting(tableDesc.ID, 0, 0, *kvDB, nil)
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
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $1 WHERE k = $2`, maxValue-k, k); err != nil {
			t.Error(err)
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
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, k, maxValue-k); err != nil {
			t.Error(err)
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

	var partialBackfillDone atomic.Value
	partialBackfillDone.Store(false)
	var partialBackfill bool
	const numNodes, chunkSize, maxValue = 5, 100, 4000
	params, _ := createTestServerParams()
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
	// Disable asynchronous schema change execution to allow synchronous path
	// to trigger start of backfill notification.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if !partialBackfill {
					notifyBackfill()
				}
				return nil
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     chunkSize,
		},
		DistSQL: &distsqlrun.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if partialBackfill {
					if partialBackfillDone.Load().(bool) {
						notifyBackfill()
						// Returning DeadlineExceeded will result in the
						// schema change being retried.
						return context.DeadlineExceeded
					}
					partialBackfillDone.Store(true)
				} else {
					notifyBackfill()
				}
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
	kvDB := tc.Server(0).KVClient().(*client.DB)
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
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		sql.SplitTable(t, tc, tableDesc, i, maxValue/numNodes*i)
	}

	ctx := context.TODO()

	// number of keys == 2 * number of rows; 1 column family and 1 index entry
	// for each row.
	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}

	// Run some schema changes with operations.

	// Add column.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')",
		maxValue,
		2,
		initBackfillNotification())

	// Drop column.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test DROP pi",
		maxValue,
		2,
		initBackfillNotification())

	// Add index.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"CREATE UNIQUE INDEX foo ON t.test (v)",
		maxValue,
		3,
		initBackfillNotification())

	// Drop index.
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"DROP INDEX t.test@vidx",
		maxValue,
		2,
		initBackfillNotification())

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
		if 1.4 != x {
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

	// Verify that a table delete in the middle of a backfill works properly.
	// The backfill will terminate in the middle, and the delete will
	// successfully delete all the table data.
	//
	// This test could be made its own test but is placed here to speed up the
	// testing.

	notification := initBackfillNotification()
	partialBackfill = true
	// Run the schema change in a separate goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Start schema change that eventually runs a partial backfill.
		if _, err := sqlDB.Exec("CREATE UNIQUE INDEX bar ON t.test (v)"); err != nil {
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

	// Ensure that the table data has been deleted.
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(ctx, tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 0; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
}

// Test that a schema change on encountering a permanent backfill error
// on a remote node terminates properly and returns the database to a
// proper state.
func TestBackfillErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes, chunkSize, maxValue = 5, 100, 4000
	params, _ := createTestServerParams()

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
	kvDB := tc.Server(0).KVClient().(*client.DB)
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
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		sql.SplitTable(t, tc, tableDesc, i, maxValue/numNodes*i)
	}

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
CREATE UNIQUE INDEX vidx ON t.test (v);
`); !testutils.IsError(err, `duplicate key value \(v\)=\(1\) violates unique constraint "vidx"`) {
		t.Fatalf("got err=%s", err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	   ALTER TABLE t.test ADD COLUMN p DECIMAL NOT NULL DEFAULT (DECIMAL '1-3');
	   `); !testutils.IsError(err, `could not parse '1-3' as type decimal`) {
		t.Fatalf("got err=%s", err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
	ALTER TABLE t.test ADD COLUMN p DECIMAL NOT NULL;
	`); !testutils.IsError(err, `null value in column \"p\" violates not-null constraint`) {
		t.Fatalf("got err=%s", err)
	}

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue); err != nil {
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
	params, _ := createTestServerParams()
	const maxValue = 100
	backfillCount := int64(0)
	retriedBackfill := int64(0)
	var retriedSpan roachpb.Span

	// Disable asynchronous schema change execution to allow synchronous path
	// to trigger start of backfill notification.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				switch atomic.LoadInt64(&backfillCount) {
				case 0:
					// Keep track of the span provided with the first backfill
					// attempt.
					retriedSpan = sp
				case 1:
					// Ensure that the second backfill attempt provides the
					// same span as the first.
					if sp.Equal(retriedSpan) {
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
			AsyncExecNotification: asyncSchemaChangerDisabled,
			BackfillChunkSize:     maxValue,
		},
		DistSQL: &distsqlrun.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				switch atomic.LoadInt64(&backfillCount) {
				case 0:
					// Keep track of the span provided with the first backfill
					// attempt.
					retriedSpan = sp
				case 1:
					// Ensure that the second backfill attempt provides the
					// same span as the first.
					if sp.Equal(retriedSpan) {
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
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
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
	// column or index, however, it's still nice to have them just in case
	// INSERT gets messed up.
	testCases := []struct {
		sql string
		// Each schema change adds/drops a schema element that affects the
		// number of keys representing a table row.
		expectedNumKeysPerRow int
	}{
		{"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')", 1},
		{"ALTER TABLE t.test DROP x", 1},
		{"CREATE UNIQUE INDEX foo ON t.test (v)", 2},
		{"DROP INDEX t.test@foo", 1},
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

// Add a column and check that it succeeds.
func addColumnSchemaChange(
	t *testing.T, sqlDB *gosql.DB, kvDB *client.DB, maxValue int, numKeysPerRow int,
) {
	if _, err := sqlDB.Exec("ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')"); err != nil {
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

// Test schema changes are retried and complete properly. This also checks
// that a mutation checkpoint reduces the number of chunks operated on during
// a retry.
func TestSchemaChangeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	currChunk := 0
	seenSpan := roachpb.Span{}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				currChunk++
				// Fail somewhere in the middle.
				if currChunk == 3 {
					return context.DeadlineExceeded
				}
				if seenSpan.Key != nil {
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
			},
			// Disable asynchronous schema change execution to allow
			// synchronous path to run schema changes.
			AsyncExecNotification:   asyncSchemaChangerDisabled,
			WriteCheckpointInterval: time.Nanosecond,
		},
		DistSQL: &distsqlrun.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				currChunk++
				// Fail somewhere in the middle.
				if currChunk == 3 {
					return context.DeadlineExceeded
				}
				if seenSpan.Key != nil {
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

	addIndexSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	addColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)

	currChunk = 0
	seenSpan = roachpb.Span{}
	dropColumnSchemaChange(t, sqlDB, kvDB, maxValue, 2)
}

// Test schema changes are retried and complete properly when the table
// version changes. This also checks that a mutation checkpoint reduces
// the number of chunks operated on during a retry.
func TestSchemaChangeRetryOnVersionChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	var upTableVersion func()
	currChunk := 0
	var numBackfills uint32
	seenSpan := roachpb.Span{}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfill: func() error {
				atomic.AddUint32(&numBackfills, 1)
				return nil
			},
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
			// Disable asynchronous schema change execution to allow
			// synchronous path to run schema changes.
			AsyncExecNotification:   asyncSchemaChangerDisabled,
			WriteCheckpointInterval: time.Nanosecond,
		},
		DistSQL: &distsqlrun.TestingKnobs{
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
		if _, err := leaseMgr.Publish(ctx, id, func(table *sqlbase.TableDescriptor) error {
			// Publish nothing; only update the version.
			version = table.Version
			return nil
		}, nil); err != nil {
			t.Error(err)
		}
		// Grab a lease at the latest version so that we are confident
		// that all future leases will be taken at the latest version.
		if err := kvDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			lease, err := leaseMgr.Acquire(ctx, txn, id, version+1)
			if err != nil {
				return err
			}
			return leaseMgr.Release(lease)
		}); err != nil {
			t.Error(err)
		}
	}

	// Bulk insert.
	maxValue := 5000
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
	params, _ := createTestServerParams()
	const chunkSize = 200
	// Disable the async schema changer.
	var enableAsyncSchemaChanges uint32
	attempts := 0
	// attempt 1: write the first chunk of the index.
	// attempt 2: write the second chunk and hit a unique constraint
	// violation; purge the schema change.
	// attempt 3: return an error while purging the schema change.
	expectedAttempts := 3
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Return a deadline exceeded error during the third attempt
				// which attempts to clean up the schema change.
				if attempts == expectedAttempts {
					return context.DeadlineExceeded
				}
				return nil
			},
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
		DistSQL: &distsqlrun.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Return a deadline exceeded error during the third attempt
				// which attempts to clean up the schema change.
				if attempts == expectedAttempts {
					return context.DeadlineExceeded
				}
				return nil
			},
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

	// Add a row with a duplicate value for v
	if _, err := sqlDB.Exec(
		`INSERT INTO t.test VALUES ($1, $2)`, maxValue+1, maxValue,
	); err != nil {
		t.Fatal(err)
	}

	// A schema change that violates integrity constraints.
	if _, err := sqlDB.Exec(
		"CREATE UNIQUE INDEX foo ON t.test (v)",
	); !testutils.IsError(err, "violates unique constraint") {
		t.Fatal(err)
	}
	// The deadline exceeded error in the schema change purge results in no
	// retry attempts of the purge.
	if attempts != expectedAttempts {
		t.Fatalf("%d retries, despite allowing only (schema change + reverse) = %d", attempts, expectedAttempts)
	}

	// The index doesn't exist
	if _, err := sqlDB.Query(
		`SELECT v from t.test@foo`,
	); !testutils.IsError(err, "index .* not found") {
		t.Fatal(err)
	}

	// Read table descriptor.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// There is still a mutation hanging off of it.
	if e := 1; len(tableDesc.Mutations) != e {
		t.Fatalf("the table has %d instead of %d mutations", len(tableDesc.Mutations), e)
	}
	// The mutation is for a DROP.
	if tableDesc.Mutations[0].Direction != sqlbase.DescriptorMutation_DROP {
		t.Fatalf("the table has mutation %v instead of a DROP", tableDesc.Mutations[0])
	}

	// There is still some garbage index data that needs to be purged. All the
	// rows from k = 0 to k = maxValue have index values. The k = maxValue + 1
	// row with the conflict doesn't contain an index value.
	numGarbageValues := chunkSize

	ctx := context.TODO()

	if err := checkTableKeyCount(ctx, kvDB, 1, maxValue+1+numGarbageValues); err != nil {
		t.Fatal(err)
	}

	// Enable async schema change processing to ensure that it cleans up the
	// above garbage left behind.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	testutils.SucceedsSoon(t, func() error {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.Mutations) > 0 {
			return errors.Errorf("%d mutations remaining", len(tableDesc.Mutations))
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

// TestSchemaChangeReverseMutations tests that schema changes get reversed
// correctly when one of them violates a constraint.
func TestSchemaChangeReverseMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
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
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// Create a k-v table.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Add some data
	const maxValue = chunkSize + 1
	if err := bulkInsertIntoTable(sqlDB, maxValue); err != nil {
		t.Fatal(err)
	}

	// Create a column that is not NULL. This schema change doesn't return an
	// error only because we've turned off the synchronous execution path; it
	// will eventually fail when run by the asynchronous path.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD a INT NOT NULL, ADD c INT`); err != nil {
		t.Fatal(err)
	}

	// Add an index over a column that will be purged. This index will
	// eventually not get added.
	if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX idx_a ON t.test (a)`); err != nil {
		t.Fatal(err)
	}

	// The purge of column 'a' doesn't influence these schema changes.

	// Drop column 'v' moves along just fine. The constraint 'foo' will not be
	// enforced because c is not added.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test DROP v, ADD CONSTRAINT foo UNIQUE (c)`,
	); err != nil {
		t.Fatal(err)
	}

	// Add unique column 'b' moves along creating column b and the index on
	// it.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD b INT UNIQUE`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	if e := 7; e != len(tableDesc.Mutations) {
		t.Fatalf("e = %d, v = %d", e, len(tableDesc.Mutations))
	}

	// Enable async schema change processing.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	// Wait until all the mutations have been processed.
	var rows *gosql.Rows
	expectedCols := []string{"k", "b"}
	testutils.SucceedsSoon(t, func() error {
		// Read table descriptor.
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.Mutations) > 0 {
			return errors.Errorf("%d mutations remaining", len(tableDesc.Mutations))
		}

		// Verify that t.test has the expected data. Read the table data while
		// ensuring that the correct table lease is in use.
		var err error
		rows, err = sqlDB.Query(`SELECT * from t.test`)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that sql is using the correct table lease.
		if len(cols) != len(expectedCols) {
			defer rows.Close()
			return errors.Errorf("incorrect columns: %v, expected: %v", cols, expectedCols)
		}
		if cols[0] != expectedCols[0] || cols[1] != expectedCols[1] {
			t.Fatalf("incorrect columns: %v", cols)
		}
		return nil
	})

	defer rows.Close()
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
			if j == 0 {
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
					t.Error("received nil value for column 'k'")
				}
			} else {
				if val := *v.(*interface{}); val != nil {
					t.Error("received non NULL value for column 'b'")
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
	rows, err := sqlDB.Query(`SELECT * from t.test@test_b_key`)
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
	if _, err = sqlDB.Query(`SELECT * from t.test@foo`); err == nil {
		t.Fatal("SELECT over index 'foo' works")
	}

	ctx := context.TODO()

	// Check that the number of k-v pairs is accurate.
	if err := checkTableKeyCount(ctx, kvDB, 2, maxValue); err != nil {
		t.Fatal(err)
	}
}

// This test checks backward compatibility with old data that contains
// sentinel k:v pairs at the start of each table row. Cockroachdb used
// to write table rows with sentinel values in the past. When a new column
// is added to such a table with the new column included in the same
// column family as the primary key columns, the sentinel k:v pairs
// start representing this new column. This test checks that the sentinel
// values represent NULL column values, and that an UPDATE to such
// a column works correctly.
func TestParseSentinelValueWithNewColumnInSentinelFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
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

// Test an UPDATE using a primary and a secondary index in the middle
// of a column backfill.
func TestUpdateDuringColumnBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	backfillNotification := make(chan bool)
	continueBackfillNotification := make(chan bool)
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		DistSQL: &distsqlrun.TestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
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
	}
	server, sqlDB, _ := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
    k INT NOT NULL,
    v INT NOT NULL,
    length INT NOT NULL,
    CONSTRAINT "primary" PRIMARY KEY (k),
    INDEX v_idx (v),
    FAMILY "primary" (k, v, length)
);
INSERT INTO t.test (k, v, length) VALUES (0, 1, 1);
`); err != nil {
		t.Fatal(err)
	}

	// Run the column schema change in a separate goroutine.
	notification := backfillNotification
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD id int NOT NULL DEFAULT 0;`); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-notification

	// UPDATE the row using the secondary index.
	if _, err := sqlDB.Exec(`UPDATE t.test SET length = 27000 WHERE v = 1`); err != nil {
		t.Error(err)
	}

	// UPDATE the row using the primary index.
	if _, err := sqlDB.Exec(`UPDATE t.test SET length = 27001 WHERE k = 0`); err != nil {
		t.Error(err)
	}

	close(continueBackfillNotification)

	wg.Wait()
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
	params, _ := createTestServerParams()
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
	kvDB := tc.Server(0).KVClient().(*client.DB)
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
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		sql.SplitTable(t, tc, tableDesc, i, maxValue/numNodes*i)
	}

	// Run some schema changes.
	testCases := []struct {
		sql           string
		numKeysPerRow int
	}{
		{sql: "ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')", numKeysPerRow: 2},
		{sql: "ALTER TABLE t.test DROP pi", numKeysPerRow: 2},
		{sql: "CREATE UNIQUE INDEX foo ON t.test (v)", numKeysPerRow: 3},
		{sql: "DROP INDEX t.test@vidx", numKeysPerRow: 2},
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
		})
	}
}

func TestSchemaChangeInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
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
		{`drop-create`, `DROP TABLE t.kv`, `CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR)`,
			`relation "kv" already exists`},
		// schema change followed by another statement works.
		{`createindex-insert`, `CREATE INDEX foo ON t.kv (v)`, `INSERT INTO t.kv VALUES ('c', 'd')`,
			``},
		// CREATE TABLE followed by INSERT works.
		{`createtable-insert`, `CREATE TABLE t.origin (k CHAR PRIMARY KEY, v CHAR);`,
			`INSERT INTO t.origin VALUES ('c', 'd')`, ``},
		// Support multiple schema changes for ORMs: #15269
		// Support insert into another table after schema changes: #15297
		{`multiple-schema-change`,
			`CREATE TABLE t.orm1 (k CHAR PRIMARY KEY, v CHAR); CREATE TABLE t.orm2 (k CHAR PRIMARY KEY, v CHAR);`,
			`CREATE INDEX foo ON t.orm1 (v); CREATE INDEX foo ON t.orm2 (v); INSERT INTO t.origin VALUES ('e', 'f')`,
			``},
		// schema change at the end of a transaction that has written.
		{`insert-create`, `INSERT INTO t.kv VALUES ('e', 'f')`, `CREATE INDEX foo ON t.kv (v)`,
			`schema change statement cannot follow a statement that has written in the same transaction`},
		// schema change at the end of a read only transaction.
		{`select-create`, `SELECT * FROM t.kv`, `CREATE INDEX bar ON t.kv (v)`, ``},
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
			}
		})
	}
}

func TestSecondaryIndexWithOldStoringEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
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
		"i": "0 /t/i/1/11/2 NULL ROW",
		"u": "0 /t/u/1 /11/2 ROW",
	} {
		{
			rows, err := sqlDB.Query(fmt.Sprintf(`EXPLAIN (DEBUG) SELECT k, a, b FROM d.t@%s;`, indexName))
			if err != nil {
				t.Error(err)
				continue
			}
			defer rows.Close()
			count := 0
			for ; rows.Next(); count++ {
				var i1 *int
				var t2, t3, t4 *string
				if err := rows.Scan(&i1, &t2, &t3, &t4); err != nil {
					t.Errorf("row %d scan failed: %s", count, err)
					continue
				}
				row := fmt.Sprintf("%d %s %s %s", *i1, *t2, *t3, *t4)
				if row != expExplainRow {
					t.Errorf("expected %q but read %q", expExplainRow, row)
				}
			}
			if err := rows.Err(); err != nil {
				t.Error(err)
			} else if count != 1 {
				t.Errorf("expected one row but read %d", count)
			}
		}
		{
			rows, err := sqlDB.Query(fmt.Sprintf(`SELECT k, a, b FROM d.t@%s;`, indexName))
			if err != nil {
				t.Error(err)
				continue
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
		}
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
	params, _ := createTestServerParams()
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
	kvDB := tc.Server(0).KVClient().(*client.DB)
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
	// SplitTable moves the right range, so we split things back to front
	// in order to move less data.
	for i := numNodes - 1; i > 0; i-- {
		sql.SplitTable(t, tc, tableDesc, i, maxValue/numNodes*i)
	}

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
