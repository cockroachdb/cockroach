// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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

	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	oldDBID := descpb.ID(sqlutils.QueryDatabaseID(t, db, "test"))

	// Create table in 'test'.
	oldName := "foo"
	if _, err := db.Exec(`CREATE TABLE test.foo (k INT PRIMARY KEY, v int)`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "foo")
	if tableDesc.GetName() != oldName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", oldName, tableDesc)
	}
	if tableDesc.GetParentID() != oldDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", oldDBID, tableDesc)
	}

	newName := "bar"
	if _, err := db.Exec(`ALTER TABLE test.foo RENAME TO test.bar`); err != nil {
		t.Fatal(err)
	}

	// Check the table descriptor again.
	renamedDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "bar")
	if renamedDesc.GetName() != newName {
		t.Fatalf("Wrong table name, expected %s, got: %+v", newName, tableDesc)
	}
	if renamedDesc.GetParentID() != oldDBID {
		t.Fatalf("Wrong parent ID on table, expected %d, got: %+v", oldDBID, tableDesc)
	}
	if renamedDesc.GetID() != tableDesc.GetID() {
		t.Fatalf("Wrong ID after rename, got %d, expected %d",
			renamedDesc.GetID(), tableDesc.GetID())
	}
}

// Test that a SQL txn that resolved a name can keep resolving that name during
// its lifetime even after the table has been renamed.
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
			id, version, name, _, err := descpb.GetDescriptorMetadata(descriptor)
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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
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
SET autocommit_before_ddl=off;
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
	if _, err := txn.Exec("SET LOCAL autocommit_before_ddl = false"); err != nil {
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
