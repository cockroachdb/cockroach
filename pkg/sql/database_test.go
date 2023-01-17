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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDatabaseAccessors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if err := TestingDescsTxn(context.Background(), s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		_, err := col.ByID(txn.KV()).Get().Database(ctx, keys.SystemDatabaseID)
		return err
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

	// Now get the database descriptor from disk.
	db := desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "d")
	if db.GetSchemaID("sc") == descpb.InvalidID {
		t.Fatal("expected to find child schema sc in db")
	}

	// Now rename the schema and ensure that the new entry shows up.
	if _, err := sqlDB.Exec(`ALTER SCHEMA sc RENAME TO sc2`); err != nil {
		t.Fatal(err)
	}

	db = desctestutils.TestingGetDatabaseDescriptor(kvDB, keys.SystemSQLCodec, "d")
	if db.GetSchemaID("sc2") == descpb.InvalidID {
		t.Fatal("expected to find child schema sc2 in db")
	}
	if db.GetSchemaID("sc") != descpb.InvalidID {
		t.Fatal("expected to not find schema sc in db")
	}
}
