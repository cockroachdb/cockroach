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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDatabaseAccessors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if err := kvDB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		if _, err := catalogkv.GetDatabaseDescByID(ctx, txn, keys.SystemSQLCodec, keys.SystemDatabaseID); err != nil {
			return err
		}
		if _, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, keys.SystemSQLCodec, keys.SystemDatabaseID); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseHasChildSchemas(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a database and schema.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
USE d;
CREATE SCHEMA sc;
`); err != nil {
		t.Fatal(err)
	}

	getDB := func() catalog.DatabaseDescriptor {
		var db catalog.DatabaseDescriptor
		if err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			dbID, err := catalogkv.GetDatabaseID(ctx, txn, keys.SystemSQLCodec, "d", true /* required */)
			if err != nil {
				return err
			}
			db, err = catalogkv.GetDatabaseDescByID(ctx, txn, keys.SystemSQLCodec, dbID)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return db
	}

	// Now get the database descriptor from disk.
	db := getDB()
	if db.GetSchemaID("sc") == descpb.InvalidID {
		t.Fatal("expected to find child schema sc in db")
	}

	// Now rename the schema and ensure that the new entry shows up.
	if _, err := sqlDB.Exec(`ALTER SCHEMA sc RENAME TO sc2`); err != nil {
		t.Fatal(err)
	}

	db = getDB()
	if db.GetSchemaID("sc2") == descpb.InvalidID {
		t.Fatal("expected to find child schema sc2 in db")
	}
	if db.GetSchemaID("sc") != descpb.InvalidID {
		t.Fatal("expected to not find schema sc in db")
	}
}
