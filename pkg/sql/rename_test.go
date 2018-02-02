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

package sql

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TestRenameTable tests the table descriptor changes during
// a rename operation.
func TestRenameTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	counter := int64(keys.MaxReservedDescID)

	counter++
	oldDBID := sqlbase.ID(counter)
	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// Create table in 'test'.
	counter++
	oldName := "foo"
	if _, err := db.Exec(`CREATE TABLE test.public.foo (k INT PRIMARY KEY, v int)`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor.
	desc := &sqlbase.Descriptor{}
	tableDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(counter))
	if err := kvDB.GetProto(context.TODO(), tableDescKey, desc); err != nil {
		t.Fatal(err)
	}
	tableDesc := desc.GetTable()
	if tableDesc.Name != oldName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", oldName, tableDesc)
	}
	if tableDesc.ParentID != oldDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", oldDBID, tableDesc)
	}

	// Create database test2.
	counter++
	newDBID := sqlbase.ID(counter)
	if _, err := db.Exec(`CREATE DATABASE test2`); err != nil {
		t.Fatal(err)
	}

	// Move table to test2 and change its name as well.
	newName := "bar"
	if _, err := db.Exec(`ALTER TABLE test.public.foo RENAME TO test2.public.bar`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor again.
	if err := kvDB.GetProto(context.TODO(), tableDescKey, desc); err != nil {
		t.Fatal(err)
	}
	tableDesc = desc.GetTable()
	if tableDesc.Name != newName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", newName, tableDesc)
	}
	if tableDesc.ParentID != newDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", newDBID, tableDesc)
	}
}

// isRenamed tests if a descriptor is updated by gossip to the specified name
// and version.
func isRenamed(
	tableID sqlbase.ID,
	expectedName string,
	expectedVersion sqlbase.DescriptorVersion,
	cfg config.SystemConfig,
) bool {
	descKey := sqlbase.MakeDescMetadataKey(tableID)
	val := cfg.GetValue(descKey)
	if val == nil {
		return false
	}
	var descriptor sqlbase.Descriptor
	if err := val.GetProto(&descriptor); err != nil {
		panic("unable to unmarshal table descriptor")
	}
	table := descriptor.GetTable()
	return table.Name == expectedName && table.Version == expectedVersion
}

// Test that a SQL txn that resolved a name can keep resolving that name during
// its lifetime even after the table has been renamed.
// Also tests that the name of a renamed table cannot be reused until everybody
// has stopped using it. Otherwise, we'd have different transactions in the
// systems using a single name for different tables.
// Also tests that the old name cannot be used by node that doesn't have a lease
// on the old version even while the name mapping still exists.
func TestTxnCanStillResolveOldName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var lmKnobs LeaseManagerTestingKnobs
	// renameUnblocked is used to block the rename schema change until the test
	// doesn't need the old name->id mapping to exist anymore.
	renameUnblocked := make(chan interface{})
	serverParams := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &SchemaChangerTestingKnobs{
				OldNamesDrainedNotification: func() {
					<-renameUnblocked
				},
			},
			SQLLeaseManager: &lmKnobs,
		}}
	var mu syncutil.Mutex
	var waitTableID sqlbase.ID
	// renamed is used to block until the node cannot get leases with the original
	// table name. It will be signaled once the table has been renamed and the update
	// about the new name has been processed. Moreover, not only does an update to
	// the name needs to have been received, but the version of the descriptor needs to
	// have also been incremented in order to guarantee that the node cannot get
	// leases using the old name (an update with the new name but the original
	// version is ignored by the leasing refresh mechanism).
	renamed := make(chan interface{})
	lmKnobs.TestingLeasesRefreshedEvent =
		func(cfg config.SystemConfig) {
			mu.Lock()
			defer mu.Unlock()
			if waitTableID != 0 {
				if isRenamed(waitTableID, "t2", 2, cfg) {
					close(renamed)
					waitTableID = 0
				}
			}
		}
	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.TODO())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.public.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	mu.Lock()
	waitTableID = tableDesc.ID
	mu.Unlock()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Run a command to make the transaction resolves the table name.
	if _, err := txn.Exec("SELECT * FROM test.public.t"); err != nil {
		t.Fatal(err)
	}

	// Concurrently, rename the table.
	threadDone := make(chan error)
	go func() {
		// The ALTER will commit and signal the main thread through `renamed`, but
		// the schema changer will remain blocked by the lease on the "t" version
		// held by the txn started above.
		_, err := db.Exec("ALTER TABLE test.public.t RENAME TO test.public.t2")
		threadDone <- err
	}()
	defer func() {
		close(renameUnblocked)
		// Block until the thread doing the rename has finished, so the test can clean
		// up. It needed to wait for the transaction to release its lease.
		if err := <-threadDone; err != nil {
			t.Fatal(err)
		}
	}()

	// Block until the LeaseManager has processed the gossip update.
	<-renamed

	// Run another command in the transaction and make sure that we can still
	// resolve the table name.
	if _, err := txn.Exec("SELECT * FROM test.public.t"); err != nil {
		t.Fatal(err)
	}

	// Check that the name cannot be reused while somebody still has a lease on
	// the old one (the mechanism for ensuring this is that the entry for the old
	// name is not deleted from the database until the async schema changer checks
	// that there's no more leases on the old version).
	if _, err := db.Exec("CREATE TABLE test.public.t (a INT PRIMARY KEY)"); !testutils.IsError(
		err, `relation "t" already exists`) {
		t.Fatal(err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Check that the old name is not usable outside of the transaction now
	// that the node doesn't have a lease on it anymore (committing the txn
	// should have released the lease on the version of the descriptor with the
	// old name), even thoudh the name mapping still exists.
	lease := s.LeaseManager().(*LeaseManager).tableNames.get(tableDesc.ID, "t", s.Clock().Now())
	if lease != nil {
		t.Fatalf(`still have lease on "t"`)
	}
	if _, err := db.Exec("SELECT * FROM test.public.t"); !testutils.IsError(
		err, `relation "test.public.t" does not exist`) {
		t.Fatal(err)
	}
}

// Test that a txn doing a rename can use the new name immediately.
// It can also use the old name if it took a lease on it before the rename, for
// better or worse.
func TestTxnCanUseNewNameAfterRename(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.public.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure we take a lease on the version called "t".
	if _, err := db.Exec("SELECT * FROM test.public.t"); err != nil {
		t.Fatal(err)
	}
	{
		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := txn.Exec("ALTER TABLE test.public.t RENAME TO test.public.t2"); err != nil {
			t.Fatal(err)
		}
		// Check that we can use the new name.
		if _, err := txn.Exec("SELECT * FROM test.public.t2"); err != nil {
			t.Fatal(err)
		}

		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	{
		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := txn.Exec("ALTER TABLE test.public.t2 RENAME TO test.public.t"); err != nil {
			t.Fatal(err)
		}
		// Check that we can use the new name.
		if _, err := txn.Exec("SELECT * FROM test.public.t"); err != nil {
			t.Fatal(err)
		}
		// Check that we cannot use the old name.
		if _, err := txn.Exec(`
SELECT * FROM test.public.t2
`); !testutils.IsError(err, "relation \"test.public.t2\" does not exist") {
			t.Fatalf("err = %v", err)
		}
		if err := txn.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

// Check that we properly cleanup all the temporary names when performing a
// series of renames in a transaction.
func TestSeriesOfRenames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.public.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ALTER TABLE test.public.t RENAME TO test.public.t2"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ALTER TABLE test.public.t2 RENAME TO test.public.t3"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ALTER TABLE test.public.t3 RENAME TO test.public.t4"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Check that the temp names have been properly cleaned up by creating tables
	// with those names.
	if _, err := db.Exec("CREATE TABLE test.public.t (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE test.public.t2 (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE test.public.t3 (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
}

// Tests that a RENAME while a name is being drained will result in the
// table version being incremented again, implying that all old names
// are drained correctly. The new RENAME will succeed with
// all old names drained.
func TestRenameDuringDrainingName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// two channels that signal the start of the second rename
	// and the end of the second rename.
	startRename := make(chan interface{})
	finishRename := make(chan interface{})
	serverParams := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLSchemaChanger: &SchemaChangerTestingKnobs{
				OldNamesDrainedNotification: func() {
					if startRename != nil {
						// Run second rename.
						start := startRename
						startRename = nil
						close(start)
						<-finishRename
					}
				},
				SyncFilter: func(tscc TestingSchemaChangerCollection) {
					// Clear the schema changer for the second RENAME so
					// that we can be sure that the first schema changer
					// runs both schema changes.
					if startRename == nil {
						tscc.ClearSchemaChangers()
					}
				},
			},
		}}

	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.TODO())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.public.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	// The expected version will be the result of two increments for the
	// two schema changes and one increment for signaling of the completion
	// of the drain.
	expectedVersion := tableDesc.Version + 3

	// Concurrently, rename the table.
	start := startRename
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := db.Exec("ALTER TABLE test.public.t RENAME TO test.public.t2"); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-start
	if _, err := db.Exec("ALTER TABLE test.public.t2 RENAME TO test.public.t3"); err != nil {
		t.Fatal(err)
	}
	close(finishRename)

	wg.Wait()

	// Table rename to t3 was successful.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "test", "t3")
	if version := tableDesc.Version; expectedVersion != version {
		t.Fatalf("version mismatch: expected = %d, current = %d", expectedVersion, version)
	}

	// Old names are gone.
	if _, err := db.Exec("SELECT * FROM test.public.t"); !testutils.IsError(
		err, `relation "test.public.t" does not exist`) {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT * FROM test.public.t2"); !testutils.IsError(
		err, `relation "test.public.t2" does not exist`) {
		t.Fatal(err)
	}
}
