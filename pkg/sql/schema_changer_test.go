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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	csql "github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
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
	defer s.Stopper().Stop()
	// Set MinSchemaChangeLeaseDuration to always expire the lease.
	minLeaseDuration := csql.MinSchemaChangeLeaseDuration
	csql.MinSchemaChangeLeaseDuration = 2 * csql.SchemaChangeLeaseDuration
	defer func() {
		csql.MinSchemaChangeLeaseDuration = minLeaseDuration
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
	changer := csql.NewSchemaChangerForTesting(id, 0, node, *kvDB, nil)

	// Acquire a lease.
	lease, err := changer.AcquireLease()
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, lease.ExpirationTime))
	}

	// Acquiring another lease will fail.
	if _, err := changer.AcquireLease(); !testutils.IsError(
		err, "an outstanding schema change lease exists",
	) {
		t.Fatal(err)
	}

	// Extend the lease.
	newLease, err := changer.ExtendLease(lease)
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(newLease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, newLease.ExpirationTime))
	}

	// The new lease is a brand new lease.
	if newLease == lease {
		t.Fatalf("lease was not extended: %v", lease)
	}

	// Extending an old lease fails.
	if _, err := changer.ExtendLease(lease); !testutils.IsError(err, "table: .* has lease") {
		t.Fatal(err)
	}

	// Releasing an old lease fails.
	err = changer.ReleaseLease(lease)
	if err == nil {
		t.Fatal("releasing a old lease succeeded")
	}

	// Release lease.
	err = changer.ReleaseLease(newLease)
	if err != nil {
		t.Fatal(err)
	}

	// Extending the lease fails.
	_, err = changer.ExtendLease(newLease)
	if err == nil {
		t.Fatalf("was able to extend an already released lease: %d, %v", id, lease)
	}

	// acquiring the lease succeeds
	lease, err = changer.AcquireLease()
	if err != nil {
		t.Fatal(err)
	}

	// Set MinSchemaChangeLeaseDuration to not expire the lease.
	csql.MinSchemaChangeLeaseDuration = minLeaseDuration
	newLease, err = changer.ExtendLease(lease)
	if err != nil {
		t.Fatal(err)
	}
	// The old lease is renewed.
	if newLease != lease {
		t.Fatalf("acquired new lease: %v, old lease: %v", newLease, lease)
	}
}

func validExpirationTime(expirationTime int64) bool {
	now := timeutil.Now()
	return expirationTime > now.Add(csql.LeaseDuration/2).UnixNano() && expirationTime < now.Add(csql.LeaseDuration*3/2).UnixNano()
}

func TestSchemaChangeProcess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()

	params, _ := createTestServerParams()
	// Disable external processing of mutations.
	params.Knobs.SQLSchemaChanger = &csql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	var id = sqlbase.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	stopper := stop.NewStopper()
	leaseMgr := csql.NewLeaseManager(&base.NodeIDContainer{}, *kvDB, hlc.NewClock(hlc.UnixNano),
		csql.LeaseManagerTestingKnobs{}, stopper, &csql.MemoryMetrics{})
	defer stopper.Stop()
	changer := csql.NewSchemaChangerForTesting(id, 0, node, *kvDB, leaseMgr)

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

	desc, err := changer.MaybeIncrementVersion()
	if err != nil {
		t.Fatal(err)
	}
	tableDesc = desc.GetTable()
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}
	isDone, err := changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if !isDone {
		t.Fatalf("table expected to not have an outstanding schema change: %v", tableDesc)
	}

	// Check that MaybeIncrementVersion increments the version
	// correctly.
	expectedVersion++
	tableDesc.UpVersion = true
	if err := kvDB.Put(
		context.TODO(),
		sqlbase.MakeDescMetadataKey(tableDesc.ID),
		sqlbase.WrapDescriptor(tableDesc),
	); err != nil {
		t.Fatal(err)
	}
	isDone, err = changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if isDone {
		t.Fatalf("table expected to have an outstanding schema change: %v", desc.GetTable())
	}
	desc, err = changer.MaybeIncrementVersion()
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
	isDone, err = changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if !isDone {
		t.Fatalf("table expected to not have an outstanding schema change: %v", tableDesc)
	}

	// Check that RunStateMachineBeforeBackfill doesn't do anything
	// if there are no mutations queued.
	if err := changer.RunStateMachineBeforeBackfill(); err != nil {
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
	changer = csql.NewSchemaChangerForTesting(id, tableDesc.NextMutationID, node, *kvDB, leaseMgr)
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
			context.TODO(),
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
			if err := changer.RunStateMachineBeforeBackfill(); err != nil {
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
	isDone, err = changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if isDone {
		t.Fatalf("table expected to have an outstanding schema change: %v", tableDesc)
	}

}

func TestAsyncSchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable synchronous schema change execution so the asynchronous schema
	// changer executes all schema changes.
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &csql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc csql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
	mTest := mutationTest{
		T:         t,
		kvDB:      kvDB,
		sqlDB:     sqlDB,
		tableDesc: tableDesc,
	}
	indexQuery := `SELECT v FROM t.test@foo`
	_ = mTest.checkQueryResponse(indexQuery, [][]string{{"b"}, {"d"}})

	// Ensure that the version has been incremented.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
	newVersion := tableDesc.Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Apply a schema change that only sets the UpVersion bit.
	expectedVersion = newVersion + 1

	if _, err := sqlDB.Exec(`
ALTER INDEX t.test@foo RENAME TO ufo
`); err != nil {
		t.Fatal(err)
	}

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
		cmd := fmt.Sprintf(`CREATE INDEX foo%d ON t.test (v)`, i)
		if _, err := sqlDB.Exec(cmd); err != nil {
			t.Fatal(err)
		}
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
		_ = mTest.checkQueryResponse(indexQuery, [][]string{{"b"}, {"d"}})
	}
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
	backfillNotification chan bool,
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

	// Grabbing a schema change lease on the table will fail, disallowing
	// another schema change from being simultaneously executed.
	sc := csql.NewSchemaChangerForTesting(tableDesc.ID, 0, 0, *kvDB, nil)
	if l, err := sc.AcquireLease(); err == nil {
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
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := keyMultiple * (maxValue + numInserts + 1); len(kvs) != e {
		for _, kv := range kvs {
			t.Errorf("key %s, value %s", kv.Key, kv.Value)
		}
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Delete the rows inserted.
	for i := 0; i < numInserts; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, maxValue+i+1); err != nil {
			t.Error(err)
		}
	}
}

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestRaceWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var backfillNotification chan bool
	params, _ := createTestServerParams()
	// Disable asynchronous schema change execution to allow synchronous path
	// to trigger start of backfill notification.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &csql.SchemaChangerTestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				if backfillNotification != nil {
					// Close channel to notify that the backfill has started.
					close(backfillNotification)
					backfillNotification = nil
				}
				return nil
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, pi DECIMAL DEFAULT (DECIMAL '3.14'));
CREATE UNIQUE INDEX vidx ON t.test (v);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	maxValue := 4000
	insert := fmt.Sprintf(`INSERT INTO t.test VALUES (%d, %d)`, 0, maxValue)
	for i := 1; i <= maxValue; i++ {
		insert += fmt.Sprintf(` ,(%d, %d)`, i, maxValue-i)
	}
	if _, err := sqlDB.Exec(insert); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	// number of keys == 3 * number of rows; 2 column families and 1 index entry
	// for each row.
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 3 * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Run some schema changes with operations.

	// Add column.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')",
		maxValue,
		4,
		backfillNotification)

	// Drop column.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test DROP pi",
		maxValue,
		3,
		backfillNotification)

	// Add index.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"CREATE UNIQUE INDEX foo ON t.test (v)",
		maxValue,
		4,
		backfillNotification)

	// Drop index.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"DROP INDEX t.test@vidx",
		maxValue,
		3,
		backfillNotification)

	// Verify that the index foo over v is consistent, and that column x has
	// been backfilled properly.
	rows, err := sqlDB.Query(`SELECT v, x from t.test@foo`)
	if err != nil {
		t.Fatal(err)
	}

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

	notification := make(chan bool)
	backfillNotification = notification
	// Run the schema change in a separate goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// Start schema change that eventually runs a backfill.
		if _, err := sqlDB.Exec("CREATE UNIQUE INDEX bar ON t.test (v)"); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	// Wait until the schema change backfill starts.
	<-notification

	// Wait for a short bit to ensure that the backfill has likely progressed
	// and written some data, but not long enough that the backfill has
	// completed.
	time.Sleep(10 * time.Millisecond)

	if _, err := sqlDB.Exec("DROP TABLE t.test"); err != nil {
		t.Fatal(err)
	}

	// Wait until the schema change is done.
	wg.Wait()

	// Ensure that the table data has been deleted.
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if len(kvs) != 0 {
		t.Fatalf("expected %d key value pairs, but got %d", 0, len(kvs))
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
		SQLExecutor: &csql.ExecutorTestingKnobs{
			// Fix the priority to guarantee that a high priority transaction
			// pushes a lower priority one.
			FixTxnPriority: true,
		},
		SQLSchemaChanger: &csql.SchemaChangerTestingKnobs{
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
			// Set the backfill chunk size for all the backfill operations.
			BackfillChunkSize: maxValue,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

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
		{"ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')", 2},
		{"ALTER TABLE t.test DROP x", 1},
		{"CREATE UNIQUE INDEX foo ON t.test (v)", 2},
		{"DROP INDEX t.test@foo", 1},
	}

	for i, testCase := range testCases {
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

			// Backfill retry happened.
			if count, e := atomic.SwapInt64(&retriedBackfill, 0), int64(1); count != e {
				t.Fatalf("expected = %d, found = %d", e, count)
			}
			// 1 failed + 2 retried backfill chunks.
			expectNumBackfills := int64(3)
			if i == len(testCases)-1 {
				// The DROP INDEX case: The above INSERTs do not add any index
				// entries for the inserted rows, so the index remains smaller
				// than a backfill chunk and is dropped in a single retried
				// backfill chunk.
				expectNumBackfills = 2
			}
			if count := atomic.SwapInt64(&backfillCount, 0); count != expectNumBackfills {
				t.Fatalf("expected = %d, found = %d", expectNumBackfills, count)
			}

			// Verify the number of keys left behind in the table to validate
			// schema change operations.
			tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
			tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
			tableEnd := tablePrefix.PrefixEnd()
			if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
				t.Fatal(err)
			} else if e := testCase.expectedNumKeysPerRow * (maxValue + 1); len(kvs) != e {
				t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
			}
		})
	}
}

// Test schema changes are retried and complete properly. This also checks
// that a mutation checkpoint reduces the number of chunks operated on during
// a retry.
func TestSchemaChangeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	attempts := 0
	seenSpan := roachpb.Span{}
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &csql.SchemaChangerTestingKnobs{
			RunBeforeBackfillChunk: func(sp roachpb.Span) error {
				attempts++
				// Fail somewhere in the middle.
				if attempts == 3 {
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
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	maxValue := 5000
	insert := fmt.Sprintf(`INSERT INTO t.test VALUES (%d, %d)`, 0, maxValue)
	for i := 1; i <= maxValue; i++ {
		insert += fmt.Sprintf(` ,(%d, %d)`, i, maxValue-i)
	}
	if _, err := sqlDB.Exec(insert); err != nil {
		t.Fatal(err)
	}

	// Add an index and check that it succeeds.
	if _, err := sqlDB.Exec("CREATE UNIQUE INDEX foo ON t.test (v)"); err != nil {
		t.Fatal(err)
	}

	// The schema change succeeded. Verify that the index foo over v is
	// consistent.
	rows, err := sqlDB.Query(`SELECT v from t.test@foo`)
	if err != nil {
		t.Fatal(err)
	}

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

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	numKeysPerRow := 2
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := numKeysPerRow * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Add a column and check that it works.
	attempts = 0
	seenSpan = roachpb.Span{}
	if _, err := sqlDB.Exec("ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4')"); err != nil {
		t.Fatal(err)
	}
	rows, err = sqlDB.Query(`SELECT x from t.test`)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
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
	numKeysPerRow += 1
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := numKeysPerRow * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Delete a column and check that it works.
	attempts = 0
	seenSpan = roachpb.Span{}
	if _, err := sqlDB.Exec("ALTER TABLE t.test DROP x"); err != nil {
		t.Fatal(err)
	}
	numKeysPerRow -= 1
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := numKeysPerRow * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
}

// Test schema change purge failure doesn't leave DB in a bad state.
func TestSchemaChangePurgeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	// Disable the async schema changer.
	var enableAsyncSchemaChanges uint32
	attempts := 0
	// attempt 1: write the first chunk of the index.
	// attempt 2: write the second chunk and hit a unique constraint
	// violation; purge the schema change.
	// attempt 3: return an error while purging the schema change.
	expectedAttempts := 3
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &csql.SchemaChangerTestingKnobs{
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
			AsyncExecQuickly: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	maxValue := csql.IndexBackfillChunkSize + 1
	insert := fmt.Sprintf(`INSERT INTO t.test VALUES (%d, %d)`, 0, maxValue)
	for i := 1; i <= maxValue; i++ {
		insert += fmt.Sprintf(` ,(%d, %d)`, i, maxValue-i)
	}
	if _, err := sqlDB.Exec(insert); err != nil {
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
	numGarbageValues := csql.IndexBackfillChunkSize
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 1*(maxValue+2) + numGarbageValues; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Enable async schema change processing to ensure that it cleans up the
	// above garbage left behind.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	util.SucceedsSoon(t, func() error {
		tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")
		if len(tableDesc.Mutations) > 0 {
			return errors.Errorf("%d mutations remaining", len(tableDesc.Mutations))
		}
		return nil
	})

	// No garbage left behind.
	numGarbageValues = 0
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 1*(maxValue+2) + numGarbageValues; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
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
	// Disable synchronous schema change processing so that the mutations get
	// processed asynchronously.
	var enableAsyncSchemaChanges uint32
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &csql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc csql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: func() error {
				if enable := atomic.LoadUint32(&enableAsyncSchemaChanges); enable == 0 {
					return errors.New("async schema changes are disabled")
				}
				return nil
			},
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	// Create a k-v table.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Add some data
	maxValue := csql.IndexBackfillChunkSize + 1
	insert := fmt.Sprintf(`INSERT INTO t.test VALUES (%d, %d)`, 0, maxValue)
	for i := 1; i <= maxValue; i++ {
		insert += fmt.Sprintf(` ,(%d, %d)`, i, maxValue-i)
	}
	if _, err := sqlDB.Exec(insert); err != nil {
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
	util.SucceedsSoon(t, func() error {
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
			return errors.Errorf("incorrect columns: %v, expected: %v", cols, expectedCols)
		}
		if cols[0] != expectedCols[0] || cols[1] != expectedCols[1] {
			t.Fatalf("incorrect columns: %v", cols)
		}
		return nil
	})

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

	// Check that the number of k-v pairs is accurate.
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(context.TODO(), tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 2 * (maxValue + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
}
