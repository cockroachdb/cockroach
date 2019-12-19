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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// This test creates table/database descriptors that have entries in the
// deprecated namespace table. This simulates objects created in the window
// where the migration from the old -> new system.namespace has run, but the
// cluster version has not been finalized yet.
func TestNamespaceTableSemantics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// IDs to map (parentID, name) to. Actual ID value is irrelevant to the test.
	idCounter := keys.MinNonPredefinedUserDescID

	// Database name.
	dKey := sqlbase.NewDeprecatedDatabaseKey("test").Key()
	if gr, err := kvDB.Get(ctx, dKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatal("expected non-existing key")
	}

	// Add an entry for the database in the deprecated namespace table directly.
	if err := kvDB.CPut(ctx, dKey, idCounter, nil); err != nil {
		t.Fatal(err)
	}
	idCounter++

	// Creating the database should fail, because an entry was explicitly added to
	// the system.namespace_deprecated table.
	_, err := sqlDB.Exec(`CREATE DATABASE test`)
	if !testutils.IsError(err, sqlbase.NewDatabaseAlreadyExistsError("test").Error()) {
		t.Fatalf("unexpected error %v", err)
	}

	// Renaming the database should fail as well.
	if _, err = sqlDB.Exec(`CREATE DATABASE test2`); err != nil {
		t.Fatal(err)
	}
	_, err = sqlDB.Exec(`ALTER DATABASE test2 RENAME TO test`)
	if !testutils.IsError(err, pgerror.Newf(pgcode.DuplicateDatabase,
		"the new database name \"test\" already exists").Error()) {
		t.Fatalf("unexpected error %v", err)
	}

	// Remove the entry.
	if err := kvDB.Del(ctx, dKey); err != nil {
		t.Fatal(err)
	}

	// Creating the database should work now, because we removed the mapping in
	// the old system.namespace table.
	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	// Ensure the new entry is added to the new namespace table.
	if gr, err := kvDB.Get(ctx, dKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatal("database key unexpectedly found in the deprecated system.namespace")
	}
	newDKey := sqlbase.NewDatabaseKey("test").Key()
	if gr, err := kvDB.Get(ctx, newDKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("database key not found in the new system.namespace")
	}

	txn := kvDB.NewTxn(ctx, "lookup-test-db-id")
	found, dbID, err := sqlbase.LookupDatabaseID(ctx, txn, "test")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("Error looking up the dbID")
	}

	// Simulate the same test for a table and sequence.
	tKey := sqlbase.NewDeprecatedTableKey(dbID, "rel").Key()
	if err := kvDB.CPut(ctx, tKey, idCounter, nil); err != nil {
		t.Fatal(err)
	}

	// Creating a table should fail now, because an entry was explicitly added to
	// the old system.namespace_deprecated table.
	_, err = sqlDB.Exec(`CREATE TABLE test.public.rel(a int)`)
	if !testutils.IsError(err, sqlbase.NewRelationAlreadyExistsError("rel").Error()) {
		t.Fatalf("unexpected error %v", err)
	}
	// Same applies to a table which doesn't explicitly specify the public schema,
	// as that is the default.
	_, err = sqlDB.Exec(`CREATE TABLE test.rel(a int)`)
	if !testutils.IsError(err, sqlbase.NewRelationAlreadyExistsError("rel").Error()) {
		t.Fatalf("unexpected error %v", err)
	}
	// Can not create a sequence with the same name either.
	_, err = sqlDB.Exec(`CREATE SEQUENCE test.rel`)
	if !testutils.IsError(err, sqlbase.NewRelationAlreadyExistsError("rel").Error()) {
		t.Fatalf("unexpected error %v", err)
	}

	// Can not rename a table to the same name either.
	if _, err = sqlDB.Exec(`CREATE TABLE rel2(a int)`); err != nil {
		t.Fatal(err)
	}
	_, err = sqlDB.Exec(`ALTER TABLE rel2 RENAME TO rel`)
	if testutils.IsError(err, sqlbase.NewRelationAlreadyExistsError("rel").Error()) {
		t.Fatalf("unexpected error %v", err)
	}

	// Can not rename sequences to the same name either.
	if _, err = sqlDB.Exec(`CREATE SEQUENCE rel2`); err != nil {
		t.Fatal(err)
	}
	_, err = sqlDB.Exec(`ALTER SEQUENCE rel2 RENAME TO rel`)
	if !testutils.IsError(err, sqlbase.NewRelationAlreadyExistsError("rel").Error()) {
		t.Fatalf("unexpected error %v", err)
	}

	// Remove the entry.
	if err := kvDB.Del(ctx, tKey); err != nil {
		t.Fatal(err)
	}

	// Creating a new table should succeed now.
	if _, err = sqlDB.Exec(`CREATE TABLE test.public.rel(a int)`); err != nil {
		t.Fatal(err)
	}

	// Ensure the new entry is added to the new namespace table.
	if gr, err := kvDB.Get(ctx, tKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatal("table key unexpectedly found in the deprecated system.namespace")
	}
	newTKey := sqlbase.NewPublicTableKey(dbID, "rel").Key()
	if gr, err := kvDB.Get(ctx, newTKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("table key not found in the new system.namespace")
	}
}
