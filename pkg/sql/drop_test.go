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

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Returns an error if a zone config for the specified table or
// database ID doesn't match the expected parameter. If expected
// is nil, then we verify no zone config exists.
func zoneExists(sqlDB *gosql.DB, expected *config.ZoneConfig, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.public.zones WHERE id = $1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if exists := (expected != nil); exists != rows.Next() {
		return errors.Errorf("zone config exists = %v", exists)
	}
	if expected != nil {
		// Ensure that the zone config matches.
		var storedID sqlbase.ID
		var val []byte
		if err := rows.Scan(&storedID, &val); err != nil {
			return errors.Errorf("row scan failed: %s", err)
		}
		if storedID != id {
			return errors.Errorf("e = %d, v = %d", id, storedID)
		}
		var cfg config.ZoneConfig
		if err := protoutil.Unmarshal(val, &cfg); err != nil {
			return err
		}
		if !expected.Equal(cfg) {
			return errors.Errorf("e = %v, v = %v", expected, cfg)
		}
	}
	return nil
}

// Returns an error if a descriptor "exists" for the table id.
func descExists(sqlDB *gosql.DB, exists bool, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.public.descriptor WHERE id = $1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if exists != rows.Next() {
		return errors.Errorf("descriptor exists = %v", exists)
	}
	return nil
}

func TestDropDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Disable the asynchronous path so that the table data left
			// behind is not cleaned up.
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.public.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	dbNameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "t")
	r, err := kvDB.Get(ctx, dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := desc.GetDatabase()

	tbNameKey := sqlbase.MakeNameMetadataKey(dbDesc.ID, "kv")
	gr, err := kvDB.Get(ctx, tbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	if err := kvDB.GetProto(ctx, tbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	tbDesc := desc.GetTable()

	// Add a zone config for both the table and database.
	cfg := config.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, dbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, &cfg, dbDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tbDesc.TableSpan()
	if kvs, err := kvDB.Scan(ctx, tableSpan.Key, tableSpan.EndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 6; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t RESTRICT`); !testutils.IsError(err,
		`database "t" is not empty`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t CASCADE`); err != nil {
		t.Fatal(err)
	}

	// Data is not deleted.
	if kvs, err := kvDB.Scan(ctx, tableSpan.Key, tableSpan.EndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 6; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if err := descExists(sqlDB, true, tbDesc.ID); err != nil {
		t.Fatal(err)
	}

	if gr, err := kvDB.Get(ctx, tbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table descriptor key still exists after database is dropped")
	}

	if err := descExists(sqlDB, false, dbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, nil, dbDesc.ID); err != nil {
		t.Fatal(err)
	}

	if gr, err := kvDB.Get(ctx, dbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database descriptor key still exists after database is dropped")
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
}

// Test that a dropped database's data gets deleted properly.
func TestDropDatabaseDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.public.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	dbNameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "t")
	r, err := kvDB.Get(ctx, dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(ctx, dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := desc.GetDatabase()

	tbNameKey := sqlbase.MakeNameMetadataKey(dbDesc.ID, "kv")
	gr, err := kvDB.Get(ctx, tbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	if err := kvDB.GetProto(ctx, tbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	tbDesc := desc.GetTable()

	// Add a zone config for both the table and database.
	cfg := config.DefaultZoneConfig()
	cfg.GC.TTLSeconds = 0 // Set TTL so the data is deleted immediately.
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, dbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, &cfg, dbDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tbDesc.TableSpan()
	if kvs, err := kvDB.Scan(ctx, tableSpan.Key, tableSpan.EndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 6; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t RESTRICT`); !testutils.IsError(err,
		`database "t" is not empty`) {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t CASCADE`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tbDesc.ID); err != nil {
			return err
		}

		return zoneExists(sqlDB, nil, tbDesc.ID)
	})

	// Data is deleted.
	if kvs, err := kvDB.Scan(ctx, tableSpan.Key, tableSpan.EndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 0; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}
}

// Tests that SHOW TABLES works correctly when a database is recreated
// during the time the underlying tables are still being deleted.
func TestShowTablesAfterRecreateDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	// Turn off the application of schema changes so that tables do not
	// get completely dropped.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableMigrations: true,
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.public.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`
DROP DATABASE t CASCADE;
CREATE DATABASE t;
`); err != nil {
		t.Fatal(err)
	}

	rows, err := sqlDB.Query(`
SET DATABASE=t;
SHOW TABLES;
`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("table should be invisible through SHOW TABLES")
	}
}

func TestDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const chunkSize = 200
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	numRows := 2*chunkSize + 1
	if err := tests.CreateKVTable(sqlDB, numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")

	idx, _, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(idx.ID)

	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	if _, err := sqlDB.Exec(`DROP INDEX t.public.kv@foo`); err != nil {
		t.Fatal(err)
	}
	tests.CheckKeyCount(t, kvDB, indexSpan, 0)

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

func TestDropIndexWithZoneConfigOSS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const chunkSize = 200
	const numRows = 2*chunkSize + 1

	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{BackfillChunkSize: chunkSize},
	}
	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	defer s.Stopper().Stop(context.Background())

	// Create a test table with a secondary index.
	if err := tests.CreateKVTable(sqlDB.DB, numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	indexDesc, _, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(indexDesc.ID)
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

	// Hack in zone configs for the primary and secondary indexes. (You need a CCL
	// binary to do this properly.) Dropping the index will thus require
	// regenerating the zone config's SubzoneSpans, which will fail with a "CCL
	// required" error.
	zoneConfig := config.ZoneConfig{
		Subzones: []config.Subzone{
			{IndexID: uint32(tableDesc.PrimaryIndex.ID), Config: config.DefaultZoneConfig()},
			{IndexID: uint32(indexDesc.ID), Config: config.DefaultZoneConfig()},
		},
	}
	zoneConfigBytes, err := protoutil.Marshal(&zoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.Exec(t, `INSERT INTO system.public.zones VALUES ($1, $2)`, tableDesc.ID, zoneConfigBytes)
	if exists := sqlutils.ZoneConfigExists(t, sqlDB, "t.public.kv@foo"); !exists {
		t.Fatal("zone config for index does not exist")
	}

	// Verify that dropping the index fails with a "CCL required" error.
	_, err = sqlDB.DB.Exec(`DROP INDEX t.public.kv@foo`)
	if pqErr, ok := err.(*pq.Error); !ok || pqErr.Code != sqlbase.CodeCCLRequired {
		t.Fatalf("expected pq error with CCLRequired code, but got %v", err)
	}

	// Verify that the index and its zone config still exist.
	if exists := sqlutils.ZoneConfigExists(t, sqlDB, "t.public.kv@foo"); !exists {
		t.Fatal("zone config for index no longer exists")
	}
	tests.CheckKeyCount(t, kvDB, indexSpan, numRows)
	// TODO(benesch): Run scrub here. It can't currently handle the way t.kv
	// declares column families.

	// Manually remove the zone config. (Again, doing this through the normal
	// channels requires a CCL binary.)
	sqlDB.Exec(t, `DELETE FROM system.public.zones WHERE id = $1`, tableDesc.ID)

	// Verify the index can now be properly dropped from an OSS binary.
	sqlDB.Exec(t, `DROP INDEX t.public.kv@foo`)
	if exists := sqlutils.ZoneConfigExists(t, sqlDB, "t.public.kv@foo"); exists {
		t.Fatal("zone config for index still exists after dropping index")
	}
	tests.CheckKeyCount(t, kvDB, indexSpan, 0)
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

func TestDropIndexInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const chunkSize = 200
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	numRows := 2*chunkSize + 1
	tests.CreateKVInterleavedTable(t, sqlDB, numRows)

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	tableSpan := tableDesc.TableSpan()

	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)

	if _, err := sqlDB.Exec(`DROP INDEX t.public.intlv@intlv_idx`); err != nil {
		t.Fatal(err)
	}
	tests.CheckKeyCount(t, kvDB, tableSpan, 2*numRows)

	// Ensure that index is not active.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "intlv")
	if _, _, err := tableDesc.FindIndexByName("intlv_idx"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

// Tests DROP TABLE and also checks that the table data is not deleted
// via the synchronous path.
func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Disable the asynchronous path so that the table data left
			// behind is not cleaned up.
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	numRows := 2*sql.TableTruncateChunkSize + 1
	if err := tests.CreateKVTable(sqlDB, numRows); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, err := kvDB.Get(ctx, nameKey)

	if err != nil {
		t.Fatal(err)
	}

	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	// Add a zone config for the table.
	cfg := config.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan()
	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.public.kv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents
	// regressions where name -> descriptor ID caches might make
	// this statement erronously work.
	if _, err := sqlDB.Exec(
		`SELECT * FROM t.public.kv`,
	); !testutils.IsError(err, `relation "t.public.kv" does not exist`) {
		t.Fatalf("different error than expected: %+v", err)
	}

	if gr, err := kvDB.Get(ctx, nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table namekey still exists")
	}

	// Can create a table with the same name.
	if err := tests.CreateKVTable(sqlDB, numRows); err != nil {
		t.Fatal(err)
	}

	// A lot of garbage has been left behind to be cleaned up by the
	// asynchronous path.
	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)

	if err := descExists(sqlDB, true, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}
}

// Test that after a DROP TABLE the table eventually gets deleted.
func TestDropTableDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Turn on quick garbage collection.
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	numRows := 2*sql.TableTruncateChunkSize + 1
	if err := tests.CreateKVTable(sqlDB, numRows); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, err := kvDB.Get(ctx, nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	// Add a zone config for the table.
	cfg := config.DefaultZoneConfig()
	cfg.GC.TTLSeconds = 0 // Set TTL so the data is deleted immediately.
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.public.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, &cfg, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan()
	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.public.kv`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tableDesc.ID); err != nil {
			return err
		}

		return zoneExists(sqlDB, nil, tableDesc.ID)
	})

	tests.CheckKeyCount(t, kvDB, tableSpan, 0)
}

func writeTableDesc(ctx context.Context, db *client.DB, tableDesc *sqlbase.TableDescriptor) error {
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
}

// TestDropTableWhileUpgradingFormat ensures that it's safe for a migration to
// upgrade the table descriptor's format while the table is scheduled to be
// dropped.
//
// The new format must be backwards-compatible with the old format, but that's
// true in general.
func TestDropTableWhileUpgradingFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	blockSchemaChanges := make(chan struct{})
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Block schema changes so the data is not cleaned up until we're ready.
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: func() error {
				<-blockSchemaChanges
				return nil
			},
		},
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	s, sqlDBRaw, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	const numRows = 100
	sqlutils.CreateTable(t, sqlDB.DB, "t", "a INT", numRows, sqlutils.ToRowFn(sqlutils.RowIdxFn))

	// Set TTL so the data is deleted immediately.
	sqlDB.Exec(t, `ALTER TABLE test.public.t EXPERIMENTAL CONFIGURE ZONE '{gc: {ttlseconds: 0}}'`)

	// Give the table an old format version.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	tableDesc.FormatVersion = sqlbase.FamilyFormatVersion
	if err := writeTableDesc(ctx, kvDB, tableDesc); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan()
	tests.CheckKeyCount(t, kvDB, tableSpan, numRows)

	sqlDB.Exec(t, `DROP TABLE test.public.t`)

	// Simulate a migration upgrading the table descriptor's format version after
	// the table has been dropped but before the truncation has occurred.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, "test", "t")
	if !tableDesc.Dropped() {
		t.Fatalf("expected descriptor to be in DROP state, but was in %s", tableDesc.State)
	}
	tableDesc.FormatVersion = sqlbase.InterleavedFormatVersion
	tableDesc.UpVersion = true
	if err := writeTableDesc(ctx, kvDB, tableDesc); err != nil {
		t.Fatal(err)
	}

	// Allow the schema change to proceed and verify that the data is eventually
	// deleted, despite the interleaved modification to the table descriptor.
	close(blockSchemaChanges)
	testutils.SucceedsSoon(t, func() error {
		return descExists(sqlDB.DB, false, tableDesc.ID)
	})
	tests.CheckKeyCount(t, kvDB, tableSpan, 0)
}

// Tests dropping a table that is interleaved within
// another table.
func TestDropTableInterleavedDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Turn on quick garbage collection.
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	numRows := 2*sql.TableTruncateChunkSize + 1
	tests.CreateKVInterleavedTable(t, sqlDB, numRows)

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	tableDescInterleaved := sqlbase.GetTableDescriptor(kvDB, "t", "intlv")
	tableSpan := tableDesc.TableSpan()

	tests.CheckKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.public.intlv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents regressions where
	// name -> descriptor ID caches might make this statement erronously work.
	if _, err := sqlDB.Exec(`SELECT * FROM t.public.intlv`); !testutils.IsError(
		err, `relation "t.public.intlv" does not exist`,
	) {
		t.Fatalf("different error than expected: %v", err)
	}

	testutils.SucceedsSoon(t, func() error {
		return descExists(sqlDB, false, tableDescInterleaved.ID)
	})

	tests.CheckKeyCount(t, kvDB, tableSpan, numRows)
}

func TestDropTableInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.kv (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`DROP TABLE t.public.kv`); err != nil {
		t.Fatal(err)
	}

	// We might still be able to read/write in the table inside this transaction
	// until the schema changer runs, but we shouldn't be able to ALTER it.
	if _, err := tx.Exec(`ALTER TABLE t.public.kv ADD COLUMN w CHAR`); !testutils.IsError(err,
		`table "kv" is being dropped`) {
		t.Fatalf("different error than expected: %v", err)
	}

	// Can't commit after ALTER errored, so we ROLLBACK.
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

}

func TestDropAndCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	params.UseDatabase = "test"
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		if _, err := db.Exec(`DROP TABLE IF EXISTS foo`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TABLE foo (k INT PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO foo VALUES (1), (2), (3)`); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDropAndCreateDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip(`#22256`)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: `test`})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	for i := 0; i < 20; i++ {
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS test`)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
		sqlDB.Exec(t, `INSERT INTO foo VALUES (1), (2), (3)`)
	}
}

// Test commands while a table is being dropped.
func TestCommandsWhileTableBeingDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	// Block schema changers so that the table we're about to DROP is not
	// actually dropped; it will be left in the "deleted" state.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableMigrations: true,
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.public.t(a INT PRIMARY KEY);
`
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}

	// DROP the table
	if _, err := db.Exec(`DROP TABLE test.public.t`); err != nil {
		t.Fatal(err)
	}

	// Check that SHOW TABLES marks a dropped table with the " (dropped)"
	// suffix.
	rows, err := db.Query(`SHOW TABLES FROM test`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("table should be invisible through SHOW TABLES")
	}

	// Check that CREATE TABLE with the same name returns a proper error.
	if _, err := db.Exec(`CREATE TABLE test.public.t(a INT PRIMARY KEY)`); !testutils.IsError(err, `relation "t" already exists`) {
		t.Fatal(err)
	}

	// Check that DROP TABLE with the same name returns a proper error.
	if _, err := db.Exec(`DROP TABLE test.public.t`); !testutils.IsError(err, `table "t" is being dropped`) {
		t.Fatal(err)
	}
}
