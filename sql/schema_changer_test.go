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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

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

	var lease csql.TableDescriptor_SchemaChangeLease
	var id = csql.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	db := server.DB()
	changer := csql.NewSchemaChangerForTesting(id, 0, node, *db, nil)

	// Acquire a lease.
	lease, pErr := changer.AcquireLease()
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, lease.ExpirationTime))
	}

	// Acquiring another lease will fail.
	var newLease csql.TableDescriptor_SchemaChangeLease
	newLease, pErr = changer.AcquireLease()
	if pErr == nil {
		t.Fatalf("acquired new lease: %v, while unexpired lease exists: %v", newLease, lease)
	}

	// Extend the lease.
	newLease, pErr = changer.ExtendLease(lease)
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !validExpirationTime(newLease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(0, newLease.ExpirationTime))
	}

	// Extending an old lease fails.
	_, pErr = changer.ExtendLease(lease)
	if pErr == nil {
		t.Fatal("extending an old lease succeeded")
	}

	// Releasing an old lease fails.
	err := changer.ReleaseLease(lease)
	if err == nil {
		t.Fatal("releasing a old lease succeeded")
	}

	// Release lease.
	err = changer.ReleaseLease(newLease)
	if err != nil {
		t.Fatal(err)
	}

	// Extending the lease fails.
	_, pErr = changer.ExtendLease(newLease)
	if pErr == nil {
		t.Fatalf("was able to extend an already released lease: %d, %v", id, lease)
	}

	// acquiring the lease succeeds
	lease, pErr = changer.AcquireLease()
	if pErr != nil {
		t.Fatal(pErr)
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
	defer csql.TestDisableAsyncSchemaChangeExec()()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)
	var id = csql.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	db := server.DB()
	leaseMgr := csql.NewLeaseManager(0, *db, hlc.NewClock(hlc.UnixNano))
	changer := csql.NewSchemaChangerForTesting(id, 0, node, *db, leaseMgr)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo(v));
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	nameKey := csql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, pErr := kvDB.Get(nameKey)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := csql.MakeDescMetadataKey(csql.ID(gr.ValueInt()))
	desc := &csql.Descriptor{}

	// Check that MaybeIncrementVersion doesn't increment the version
	// when the up_version bit is not set.
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	expectedVersion := desc.GetTable().Version

	if pErr := changer.MaybeIncrementVersion(); pErr != nil {
		t.Fatal(pErr)
	}
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
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
	if pErr := kvDB.Put(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	isDone, err = changer.IsDone()
	if err != nil {
		t.Fatal(err)
	}
	if isDone {
		t.Fatalf("table expected to have an outstanding schema change: %v", desc.GetTable())
	}
	if pErr := changer.MaybeIncrementVersion(); pErr != nil {
		t.Fatal(pErr)
	}
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	newVersion = desc.GetTable().Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
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
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	newVersion = desc.GetTable().Version
	if newVersion != expectedVersion {
		t.Fatalf("bad version; e = %d, v = %d", expectedVersion, newVersion)
	}

	// Check that RunStateMachineBeforeBackfill functions properly.
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	table := desc.GetTable()
	expectedVersion = table.Version
	// Make a copy of the index for use in a mutation.
	index := protoutil.Clone(&table.Indexes[0]).(*csql.IndexDescriptor)
	index.Name = "bar"
	index.ID = table.NextIndexID
	table.NextIndexID++
	changer = csql.NewSchemaChangerForTesting(id, table.NextMutationID, node, *db, leaseMgr)
	table.Mutations = append(table.Mutations, csql.DescriptorMutation{
		Descriptor_: &csql.DescriptorMutation_Index{Index: index},
		Direction:   csql.DescriptorMutation_ADD,
		State:       csql.DescriptorMutation_DELETE_ONLY,
		MutationID:  table.NextMutationID,
	})
	table.NextMutationID++

	// Run state machine in both directions.
	for _, direction := range []csql.DescriptorMutation_Direction{csql.DescriptorMutation_ADD, csql.DescriptorMutation_DROP} {
		table.Mutations[0].Direction = direction
		expectedVersion++
		if pErr := kvDB.Put(descKey, desc); pErr != nil {
			t.Fatal(pErr)
		}
		// The expected end state.
		expectedState := csql.DescriptorMutation_WRITE_ONLY
		if direction == csql.DescriptorMutation_DROP {
			expectedState = csql.DescriptorMutation_DELETE_ONLY
		}
		// Run two times to ensure idempotency of operations.
		for i := 0; i < 2; i++ {
			if err := changer.RunStateMachineBeforeBackfill(); err != nil {
				t.Fatal(err)
			}
			if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
				t.Fatal(pErr)
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
	// Disable synchronous schema change execution so
	// the asynchronous schema changer executes all
	// schema changes.
	defer csql.TestDisableSyncSchemaChangeExec()()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.test VALUES ('a', 'b'), ('c', 'd');
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	nameKey := csql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := csql.MakeDescMetadataKey(csql.ID(gr.ValueInt()))
	desc := &csql.Descriptor{}
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

// Test schema change backfills are not affected by various operations
// that run simultaneously.
func TestRaceWithBackfill(t *testing.T) {
	defer leaktest.AfterTest(t)()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
`); err != nil {
		t.Fatal(err)
	}

	// Bulk insert.
	max_value := 2000
	insert := fmt.Sprintf(`INSERT INTO t.test VALUES (%d, %d)`, 0, max_value)
	for i := 1; i <= max_value; i++ {
		insert += fmt.Sprintf(` ,(%d, %d)`, i, max_value-i)
	}
	if _, err := sqlDB.Exec(insert); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor for version.
	nameKey := csql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
	gr, pErr := kvDB.Get(nameKey)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := csql.MakeDescMetadataKey(csql.ID(gr.ValueInt()))
	desc := &csql.Descriptor{}
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	version := desc.GetTable().Version

	// Run the schema changes in a separate goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		start := timeutil.Now()
		// Start schema change that eventually runs a number of backfills.
		if _, err := sqlDB.Exec(`
BEGIN;
ALTER TABLE t.test ADD COLUMN x DECIMAL DEFAULT (DECIMAL '1.4');
CREATE UNIQUE INDEX foo ON t.test (v);
END;
`); err != nil {
			t.Error(err)
		}
		t.Logf("schema changes took %v", time.Since(start))
		wg.Done()
	}()

	// Wait until the backfills for the schema changes above have
	// started.
	util.SucceedsSoon(t, func() error {
		if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
			t.Fatal(pErr)
		}
		if desc.GetTable().Version == version+2 {
			// Version upgrade has happened, backfills can proceed.
			return nil
		}
		return errors.New("version not updated")
	})

	// TODO(vivek): uncomment these inserts when #5817 is fixed.
	// Insert some new rows in the table while the backfills are running.
	//num_values := 5
	//for i := 0; i < num_values; i++ {
	//	t.Logf("inserting a new value into the table")
	//	if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES($1, $2)`, max_value+i+1, max_value+i+1); err != nil {
	//		t.Fatal(err)
	//	}
	//}

	// Renaming an index in the middle of the backfills will not affect
	// the backfills because the above schema changes have the schema change
	// lease on the table. All future schema changes have to wait in line for
	// the lease.
	if _, err := sqlDB.Exec(`
ALTER INDEX t.test@foo RENAME TO bar;
`); err != nil {
		t.Fatal(err)
	}

	// Wait until schema changes have completed.
	wg.Wait()

	// Verify that the index over v is consistent.
	rows, err := sqlDB.Query(`SELECT v from t.test@bar`)
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for ; rows.Next(); i++ {
		var val int
		if err := rows.Scan(&val); err != nil {
			t.Fatal(err)
		}
		if i != val {
			t.Errorf("e = %d, v = %d", i, val)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if max_value+1 != i {
		t.Fatalf("read the wrong number of rows: e = %d, v = %d", max_value+1, i)
	}
}
