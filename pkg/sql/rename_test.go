// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TestRenameTable tests the table descriptor changes during
// a rename operation.
func TestRenameTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	counter := int64(keys.MinNonPredefinedUserDescID)

	oldDBID := descpb.ID(counter)
	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// Create table in 'test'.
	counter++
	oldName := "foo"
	if _, err := db.Exec(`CREATE TABLE test.foo (k INT PRIMARY KEY, v int)`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor.
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "foo")
	if tableDesc.GetName() != oldName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", oldName, tableDesc)
	}
	if tableDesc.GetParentID() != oldDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", oldDBID, tableDesc)
	}

	// Create database test2.
	counter++
	newDBID := descpb.ID(counter)
	if _, err := db.Exec(`CREATE DATABASE test2`); err != nil {
		t.Fatal(err)
	}

	// Move table to test2 and change its name as well.
	newName := "bar"
	if _, err := db.Exec(`ALTER TABLE test.foo RENAME TO test2.bar`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor again.
	renamedDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test2", "bar")
	if renamedDesc.GetName() != newName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", newName, tableDesc)
	}
	if renamedDesc.GetParentID() != newDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", newDBID, tableDesc)
	}
	if renamedDesc.GetID() != tableDesc.GetID() {
		t.Fatalf("Wrong ID after rename, got %d, expected %d",
			renamedDesc.GetID(), tableDesc.GetID())
	}
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
	defer log.Scope(t).Close(t)

	var lmKnobs lease.ManagerTestingKnobs
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
	var waitTableID descpb.ID
	// renamed is used to block until the node cannot get leases with the original
	// table name. It will be signaled once the table has been renamed and the update
	// about the new name has been processed. Moreover, not only does an update to
	// the name needs to have been received, but the version of the descriptor needs to
	// have also been incremented in order to guarantee that the node cannot get
	// leases using the old name (an update with the new name but the original
	// version is ignored by the leasing refresh mechanism).
	renamed := make(chan interface{})
	lmKnobs.TestingDescriptorRefreshedEvent =
		func(descriptor *descpb.Descriptor) {
			mu.Lock()
			defer mu.Unlock()
			id, version, name, _, _, err := descpb.GetDescriptorMetadata(descriptor)
			if err != nil {
				t.Fatal(err)
			}
			if waitTableID != id {
				return
			}
			if name == "t2" && version == 2 {
				close(renamed)
				waitTableID = 0
			}
		}
	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.Background())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	mu.Lock()
	waitTableID = tableDesc.GetID()
	mu.Unlock()

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Run a command to make the transaction resolves the table name.
	if _, err := txn.Exec("SELECT * FROM test.t"); err != nil {
		t.Fatal(err)
	}

	// Concurrently, rename the table.
	threadDone := make(chan error)
	go func() {
		// The ALTER will commit and signal the main thread through `renamed`, but
		// the schema changer will remain blocked by the lease on the "t" version
		// held by the txn started above.
		_, err := db.Exec("ALTER TABLE test.t RENAME TO test.t2")
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

	// Block until the Manager has processed the gossip update.
	<-renamed

	// Run another command in the transaction and make sure that we can still
	// resolve the table name.
	if _, err := txn.Exec("SELECT * FROM test.t"); err != nil {
		t.Fatal(err)
	}

	// Check that the name cannot be reused while somebody still has a lease on
	// the old one (the mechanism for ensuring this is that the entry for the old
	// name is not deleted from the database until the async schema changer checks
	// that there's no more leases on the old version).
	if _, err := db.Exec("CREATE TABLE test.t (a INT PRIMARY KEY)"); !testutils.IsError(
		err, `relation "test.public.t" already exists`) {
		t.Fatal(err)
	}

	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Check that the old name is not usable outside of the transaction now
	// that the node doesn't have a lease on it anymore (committing the txn
	// should have released the lease on the version of the descriptor with the
	// old name), even though the name mapping still exists.

	var foundLease bool
	s.LeaseManager().(*lease.Manager).VisitLeases(func(
		desc catalog.Descriptor, dropped bool, refCount int, expiration tree.DTimestamp,
	) (wantMore bool) {
		if desc.GetID() == tableDesc.GetID() && desc.GetName() == "t" {
			foundLease = true
		}
		return true
	})
	if foundLease {
		t.Fatalf(`still have lease on "t"`)
	}
	if _, err := db.Exec("SELECT * FROM test.t"); !testutils.IsError(
		err, `relation "test.t" does not exist`) {
		t.Fatal(err)
	}
}

// Test that a txn doing a rename can use the new name immediately.
// It can also use the old name if it took a lease on it before the rename, for
// better or worse.
func TestTxnCanUseNewNameAfterRename(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure we take a lease on the version called "t".
	if _, err := db.Exec("SELECT * FROM test.t"); err != nil {
		t.Fatal(err)
	}
	{
		txn, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}

		if _, err := txn.Exec("ALTER TABLE test.t RENAME TO test.t2"); err != nil {
			t.Fatal(err)
		}
		// Check that we can use the new name.
		if _, err := txn.Exec("SELECT * FROM test.t2"); err != nil {
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

		if _, err := txn.Exec("ALTER TABLE test.t2 RENAME TO test.t"); err != nil {
			t.Fatal(err)
		}
		// Check that we can use the new name.
		if _, err := txn.Exec("SELECT * FROM test.t"); err != nil {
			t.Fatal(err)
		}
		// Check that we cannot use the old name.
		if _, err := txn.Exec(`
SELECT * FROM test.t2
`); !testutils.IsError(err, "relation \"test.t2\" does not exist") {
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
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ALTER TABLE test.t RENAME TO test.t2"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ALTER TABLE test.t2 RENAME TO test.t3"); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("ALTER TABLE test.t3 RENAME TO test.t4"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}

	// Check that the temp names have been properly cleaned up by creating tables
	// with those names.
	if _, err := db.Exec("CREATE TABLE test.t (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE test.t2 (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE test.t3 (a INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
}

// Tests that a RENAME while a name is being drained will result in the
// table version being incremented again, implying that all old names
// are drained correctly. The new RENAME will succeed with
// all old names drained.
func TestRenameDuringDrainingName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
				// Don't run the schema changer for the second RENAME so that we can be
				// sure that the first schema changer runs both schema changes. This
				// behavior is due to the fact that the schema changer for the first job
				// will process all the draining names on the table descriptor,
				// including the ones queued up by the second job. It's not ideal since
				// we would like jobs to manage their own state without interference
				// from other jobs, but that's how schema change jobs work right now.
				SchemaChangeJobNoOp: func() bool {
					return startRename == nil
				},
			},
		}}

	s, db, kvDB := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.Background())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t (a INT PRIMARY KEY);
`
	_, err := db.Exec(sql)
	if err != nil {
		t.Fatal(err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	// The expected version will be the result of two increments for the two
	// schema changes and one increment for signaling of the completion of the
	// drain. See the above comment for an explanation of why there's only one
	// expected version update for draining names.
	expectedVersion := tableDesc.GetVersion() + 3

	// Concurrently, rename the table.
	start := startRename
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		if _, err := db.Exec("ALTER TABLE test.t RENAME TO test.t2"); err != nil {
			t.Error(err)
		}
		wg.Done()
	}()

	<-start
	if _, err := db.Exec("ALTER TABLE test.t2 RENAME TO test.t3"); err != nil {
		t.Fatal(err)
	}
	close(finishRename)

	wg.Wait()

	// Table rename to t3 was successful.
	tableDesc = catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t3")
	if version := tableDesc.GetVersion(); expectedVersion != version {
		t.Fatalf("version mismatch: expected = %d, current = %d", expectedVersion, version)
	}

	// Old names are gone.
	if _, err := db.Exec("SELECT * FROM test.t"); !testutils.IsError(
		err, `relation "test.t" does not exist`) {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT * FROM test.t2"); !testutils.IsError(
		err, `relation "test.t2" does not exist`) {
		t.Fatal(err)
	}
}
