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
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

// schemaChangeManagerDisabled can be used to disable asynchronous processing
// of schema changes.
func schemaChangeManagerDisabled() error {
	return errors.New("async schema changes are disabled")
}

func TestSchemaChangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	var lease sqlbase.TableDescriptor_SchemaChangeLease
	var id = sqlbase.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	db := server.DB()
	changer := csql.NewSchemaChangerForTesting(id, 0, node, *db, nil)

	// Acquire a lease.
	lease, err := changer.AcquireLease()
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, lease.ExpirationTime))
	}

	// Acquiring another lease will fail.
	if newLease, err := changer.AcquireLease(); err == nil {
		t.Fatalf("acquired new lease: %v, while unexpired lease exists: %v", newLease, lease)
	}

	// Extend the lease.
	newLease, err := changer.ExtendLease(lease)
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(newLease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, newLease.ExpirationTime))
	}

	// Extending an old lease fails.
	_, err = changer.ExtendLease(lease)
	if err == nil {
		t.Fatal("extending an old lease succeeded")
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
	// Disable external processing of mutations.
	ctx, _ := createTestServerContext()
	ctx.TestingKnobs.SQLSchemaChangeManager = &csql.SchemaChangeManagerTestingKnobs{
		AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
	}
	server, sqlDB, kvDB := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)
	var id = sqlbase.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	db := server.DB()
	leaseMgr := csql.NewLeaseManager(0, *db, hlc.NewClock(hlc.UnixNano), csql.LeaseManagerTestingKnobs{})
	changer := csql.NewSchemaChangerForTesting(id, 0, node, *db, leaseMgr)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo(v));
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	desc := &sqlbase.Descriptor{}

	// Check that MaybeIncrementVersion doesn't increment the version
	// when the up_version bit is not set.
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	expectedVersion := desc.GetTable().Version

	desc, err = changer.MaybeIncrementVersion()
	if err != nil {
		t.Fatal(err)
	}
	newVersion := desc.GetTable().Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}
	isDone, err := changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if !isDone {
		t.Fatalf("table expected to not have an outstanding schema change: %v", desc.GetTable())
	}

	// Check that MaybeIncrementVersion increments the version
	// correctly.
	expectedVersion++
	desc.GetTable().UpVersion = true
	if err := kvDB.Put(descKey, desc); err != nil {
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
	savedDesc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(descKey, savedDesc); err != nil {
		t.Fatal(err)
	}
	newVersion = desc.GetTable().Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version in returned desc; e = %d, v = %d", expectedVersion, newVersion)
	}
	newVersion = savedDesc.GetTable().Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version in saved desc; e = %d, v = %d", expectedVersion, newVersion)
	}
	isDone, err = changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if !isDone {
		t.Fatalf("table expected to not have an outstanding schema change: %v", desc.GetTable())
	}

	// Check that RunStateMachineBeforeBackfill doesn't do anything
	// if there are no mutations queued.
	if err := changer.RunStateMachineBeforeBackfill(); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	newVersion = desc.GetTable().Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Check that RunStateMachineBeforeBackfill functions properly.
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	table := desc.GetTable()
	expectedVersion = table.Version
	// Make a copy of the index for use in a mutation.
	index := protoutil.Clone(&table.Indexes[0]).(*sqlbase.IndexDescriptor)
	index.Name = "bar"
	index.ID = table.NextIndexID
	table.NextIndexID++
	changer = csql.NewSchemaChangerForTesting(id, table.NextMutationID, node, *db, leaseMgr)
	table.Mutations = append(table.Mutations, sqlbase.DescriptorMutation{
		Descriptor_: &sqlbase.DescriptorMutation_Index{Index: index},
		Direction:   sqlbase.DescriptorMutation_ADD,
		State:       sqlbase.DescriptorMutation_DELETE_ONLY,
		MutationID:  table.NextMutationID,
	})
	table.NextMutationID++

	// Run state machine in both directions.
	for _, direction := range []sqlbase.DescriptorMutation_Direction{sqlbase.DescriptorMutation_ADD, sqlbase.DescriptorMutation_DROP} {
		table.Mutations[0].Direction = direction
		expectedVersion++
		if err := kvDB.Put(descKey, desc); err != nil {
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
			if err := kvDB.GetProto(descKey, desc); err != nil {
				t.Fatal(err)
			}
			table = desc.GetTable()
			newVersion = table.Version
			if newVersion != expectedVersion {
				t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
			}
			state := table.Mutations[0].State
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
		t.Fatalf("table expected to have an outstanding schema change: %v", desc.GetTable())
	}

}

func TestAsyncSchemaChanger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable synchronous schema change execution so the asynchronous schema
	// changer executes all schema changes.
	ctx, _ := createTestServerContext()
	ctx.TestingKnobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SyncSchemaChangersFilter: func(tscc csql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
		},
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecQuickly: true,
		},
	}

	server, sqlDB, kvDB := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	// A long running schema change operation runs through
	// a state machine that increments the version by 3.
	expectedVersion := desc.GetTable().Version + 3

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
		if err := kvDB.GetProto(descKey, desc); err != nil {
			t.Fatal(err)
		}
		if len(desc.GetTable().Indexes) == 1 {
			break
		}
	}

	// Ensure that the indexes have been created.
	mTest := mutationTest{
		T:       t,
		kvDB:    kvDB,
		sqlDB:   sqlDB,
		descKey: descKey,
		desc:    desc,
	}
	indexQuery := `SELECT v FROM t.test@foo`
	_ = mTest.checkQueryResponse(indexQuery, [][]string{{"b"}, {"d"}})

	// Ensure that the version has been incremented.
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	newVersion := desc.GetTable().Version
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
		if err := kvDB.GetProto(descKey, desc); err != nil {
			t.Fatal(err)
		}
		name := desc.GetTable().Indexes[0].Name
		if name != "ufo" {
			t.Fatalf("bad index name %s", name)
		}
		newVersion = desc.GetTable().Version
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
		if err := kvDB.GetProto(descKey, desc); err != nil {
			t.Fatal(err)
		}
		if len(desc.GetTable().Indexes) == count+1 {
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
	descKey roachpb.Key,
	backfillNotification chan bool,
) {
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	tableDesc := desc.GetTable()

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
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $2 WHERE k = $1`, k, v); err != nil {
			t.Fatal(err)
		}
		updatedKeys = append(updatedKeys, k)
	}

	// Reupdate updated values back to what they were before.
	for _, k := range updatedKeys {
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = $2 WHERE k = $1`, k, maxValue-k); err != nil {
			t.Fatal(err)
		}
	}

	// Delete some rows.
	deleteStartKey := rand.Intn(maxValue - 10)
	for i := 0; i < 10; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, deleteStartKey+i); err != nil {
			t.Fatal(err)
		}
	}
	// Reinsert deleted rows.
	for i := 0; i < 10; i++ {
		k := deleteStartKey + i
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, k, maxValue-k); err != nil {
			t.Fatal(err)
		}
	}

	// Insert some new rows.
	numInserts := 10
	for i := 0; i < numInserts; i++ {
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, maxValue+i+1, maxValue+i+1); err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait() // for schema change to complete.

	// Verify the number of keys left behind in the table to validate schema
	// change operations.
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	if kvs, err := kvDB.Scan(tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := keyMultiple * (maxValue + numInserts + 1); len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Delete the rows inserted.
	for i := 0; i < numInserts; i++ {
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = $1`, maxValue+i+1); err != nil {
			t.Fatal(err)
		}
	}
}

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestRaceWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Disable asynchronous schema change execution to allow synchronous path
	// to trigger start of backfill notification.
	var backfillNotification chan bool
	ctx, _ := createTestServerContext()
	ctx.TestingKnobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SchemaChangersStartBackfillNotification: func() error {
				if backfillNotification != nil {
					// Close channel to notify that the backfill has started.
					close(backfillNotification)
				}
				return nil
			},
		},
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
		},
	}
	server, sqlDB, kvDB := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

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
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	tableDesc := desc.GetTable()
	tablePrefix := roachpb.Key(keys.MakeTablePrefix(uint32(tableDesc.ID)))
	tableEnd := tablePrefix.PrefixEnd()
	// number of keys == 4 * number of rows; 3 columns and 1 index entry for
	// each row.
	if kvs, err := kvDB.Scan(tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 4 * (maxValue + 1); len(kvs) != e {
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
		5,
		descKey,
		backfillNotification)

	// Drop column.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"ALTER TABLE t.test DROP pi",
		maxValue,
		4,
		descKey,
		backfillNotification)

	// Add index.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"CREATE UNIQUE INDEX foo ON t.test (v)",
		maxValue,
		5,
		descKey,
		backfillNotification)

	// Drop index.
	backfillNotification = make(chan bool)
	runSchemaChangeWithOperations(
		t,
		sqlDB,
		kvDB,
		"DROP INDEX t.test@vidx",
		maxValue,
		4,
		descKey,
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

	backfillNotification = make(chan bool)
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
	<-backfillNotification

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
	if kvs, err := kvDB.Scan(tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if len(kvs) != 0 {
		t.Fatalf("expected %d key value pairs, but got %d", 0, len(kvs))
	}
}

// Test schema changes are retried and complete properly
func TestSchemaChangeRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	attempts := 0
	ctx, _ := createTestServerContext()
	ctx.TestingKnobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SchemaChangersStartBackfillNotification: func() error {
				attempts++
				// Return a deadline exceeded error once.
				if attempts == 1 {
					return errors.New("context deadline exceeded")
				}
				return nil
			},
		},
		// Disable asynchronous schema change execution to allow synchronous
		// path to run schema changes.
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
		},
	}
	server, sqlDB, _ := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	maxValue := 10
	insert := fmt.Sprintf(`INSERT INTO t.test VALUES (%d, %d)`, 0, maxValue)
	for i := 1; i <= maxValue; i++ {
		insert += fmt.Sprintf(` ,(%d, %d)`, i, maxValue-i)
	}
	if _, err := sqlDB.Exec(insert); err != nil {
		t.Fatal(err)
	}

	// Run a schema change.
	if _, err := sqlDB.Exec("CREATE UNIQUE INDEX foo ON t.test (v)"); err != nil {
		t.Fatal(err)
	}
	// The schema change retried once.
	if e := 2; attempts != e {
		t.Fatalf("there were %d instead of %d attempts", attempts, e)
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
}

// Test schema change purge failure doesn't leave DB in a bad state.
func TestSchemaChangePurgeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Disable the async schema changer.
	var enableAsyncSchemaChanges uint32

	attempts := 0
	ctx, _ := createTestServerContext()
	ctx.TestingKnobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SchemaChangersStartBackfillNotification: func() error {
				attempts++
				// Return a deadline exceeded error during the second attempt
				// which attempts to clean up the schema change.
				if attempts == 2 {
					return errors.New("context deadline exceeded")
				}
				return nil
			},
		},
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecNotification: func() error {
				if enable := atomic.LoadUint32(&enableAsyncSchemaChanges); enable == 0 {
					return errors.New("async schema changes are disabled")
				}
				return nil
			},
			// Speed up evaluation of async schema changes so that it
			// processes a purged schema change quickly.
			AsyncSchemaChangerExecQuickly: true,
		},
	}
	server, sqlDB, kvDB := setupWithContext(t, ctx)
	defer cleanup(server, sqlDB)

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
	if e := 2; attempts != e {
		t.Fatalf("%d retries, despite allowing only (schema change + reverse) = %d", attempts, e)
	}

	// The index doesn't exist
	if _, err := sqlDB.Query(
		`SELECT v from t.test@foo`,
	); !testutils.IsError(err, "index .* not found") {
		t.Fatal(err)
	}

	// Read table descriptor.
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	tableDesc := desc.GetTable()

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
	if kvs, err := kvDB.Scan(tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 2*(maxValue+2) + numGarbageValues; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}

	// Enable async schema change processing to ensure that it cleans up the
	// above garbage left behind.
	atomic.StoreUint32(&enableAsyncSchemaChanges, 1)

	util.SucceedsSoon(t, func() error {
		if err := kvDB.GetProto(descKey, desc); err != nil {
			t.Fatal(err)
		}
		tableDesc := desc.GetTable()
		if len(tableDesc.Mutations) > 0 {
			return util.Errorf("%d mutations remaining", len(tableDesc.Mutations))
		}
		return nil
	})

	// No garbage left behind.
	numGarbageValues = 0
	if kvs, err := kvDB.Scan(tablePrefix, tableEnd, 0); err != nil {
		t.Fatal(err)
	} else if e := 2*(maxValue+2) + numGarbageValues; len(kvs) != e {
		t.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
}
