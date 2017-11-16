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
	"bytes"
	gosql "database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// Returns an error if a zone config "exists" for the table id.
func zoneExists(sqlDB *gosql.DB, exists bool, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.zones WHERE id = $1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if exists != rows.Next() {
		return errors.Errorf("zone config exists = %v", exists)
	}
	// Ensure that the zone config is the default one.
	if exists {
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
		if e := config.DefaultZoneConfig(); !reflect.DeepEqual(e, cfg) {
			return errors.Errorf("e = %v, v = %v", e, cfg)
		}
	}
	return nil
}

// Returns an error if a descriptor "exists" for the table id.
func descExists(sqlDB *gosql.DB, exists bool, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.descriptor WHERE id = $1`, id)
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
	params, _ := createTestServerParams()
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
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
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
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, dbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, true, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, true, dbDesc.ID); err != nil {
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
	if err := zoneExists(sqlDB, false, dbDesc.ID); err != nil {
		t.Fatal(err)
	}

	if gr, err := kvDB.Get(ctx, dbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database descriptor key still exists after database is dropped")
	}

	if err := zoneExists(sqlDB, true, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
}

// Test that a dropped database's data gets deleted properly.
func TestDropDatabaseDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Turn on quick garbage collection.
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
	ctx := context.TODO()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
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
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, dbDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, true, tbDesc.ID); err != nil {
		t.Fatal(err)
	}
	if err := zoneExists(sqlDB, true, dbDesc.ID); err != nil {
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

		return zoneExists(sqlDB, false, tbDesc.ID)
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
	params, _ := createTestServerParams()
	// Turn off the application of schema changes so that tables do not
	// get completely dropped.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
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

func checkKeyCount(t *testing.T, kvDB *client.DB, span roachpb.Span, numKeys int) {
	if kvs, err := kvDB.Scan(context.TODO(), span.Key, span.EndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := numKeys; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}
}

func createKVTable(sqlDB *gosql.DB, numRows int) error {
	// Fix the column families so the key counts don't change if the family
	// heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE IF NOT EXISTS t;
CREATE TABLE t.kv (k INT PRIMARY KEY, v INT, FAMILY (k), FAMILY (v));
CREATE INDEX foo on t.kv (v);
`); err != nil {
		return err
	}

	// Bulk insert.
	var insert bytes.Buffer
	if _, err := insert.WriteString(fmt.Sprintf(`INSERT INTO t.kv VALUES (%d, %d)`, 0, numRows-1)); err != nil {
		return err
	}
	for i := 1; i < numRows; i++ {
		if _, err := insert.WriteString(fmt.Sprintf(` ,(%d, %d)`, i, numRows-i)); err != nil {
			return err
		}
	}
	_, err := sqlDB.Exec(insert.String())
	return err
}

func TestDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const chunkSize = 200
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	numRows := 2*chunkSize + 1
	if err := createKVTable(sqlDB, numRows); err != nil {
		t.Fatal(err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")

	idx, _, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	indexSpan := tableDesc.IndexSpan(idx.ID)

	checkKeyCount(t, kvDB, indexSpan, numRows)
	if _, err := sqlDB.Exec(`DROP INDEX t.kv@foo`); err != nil {
		t.Fatal(err)
	}
	checkKeyCount(t, kvDB, indexSpan, 0)

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
}

func createKVInterleavedTable(t *testing.T, sqlDB *gosql.DB, numRows int) {
	// Fix the column families so the key counts don't change if the family
	// heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
SET DATABASE=t;
CREATE TABLE kv (k INT PRIMARY KEY, v INT);
CREATE TABLE intlv (k INT, m INT, n INT, PRIMARY KEY (k, m)) INTERLEAVE IN PARENT kv (k);
CREATE INDEX intlv_idx ON intlv (k, n) INTERLEAVE IN PARENT kv (k);
`); err != nil {
		t.Fatal(err)
	}

	var insert bytes.Buffer
	if _, err := insert.WriteString(fmt.Sprintf(`INSERT INTO t.kv VALUES (%d, %d)`, 0, numRows-1)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < numRows; i++ {
		if _, err := insert.WriteString(fmt.Sprintf(` ,(%d, %d)`, i, numRows-i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqlDB.Exec(insert.String()); err != nil {
		t.Fatal(err)
	}
	insert.Reset()
	if _, err := insert.WriteString(fmt.Sprintf(`INSERT INTO t.intlv VALUES (%d, %d, %d)`, 0, numRows-1, numRows-1)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < numRows; i++ {
		if _, err := insert.WriteString(fmt.Sprintf(` ,(%d, %d, %d)`, i, numRows-i, numRows-i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := sqlDB.Exec(insert.String()); err != nil {
		t.Fatal(err)
	}
}

func TestDropIndexInterleaved(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const chunkSize = 200
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			BackfillChunkSize: chunkSize,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	numRows := 2*chunkSize + 1
	createKVInterleavedTable(t, sqlDB, numRows)

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	tableSpan := tableDesc.TableSpan()

	checkKeyCount(t, kvDB, tableSpan, 3*numRows)

	if _, err := sqlDB.Exec(`DROP INDEX t.intlv@intlv_idx`); err != nil {
		t.Fatal(err)
	}
	checkKeyCount(t, kvDB, tableSpan, 2*numRows)

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
	params, _ := createTestServerParams()
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
	if err := createKVTable(sqlDB, numRows); err != nil {
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
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, true, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan()
	checkKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents
	// regressions where name -> descriptor ID caches might make
	// this statement erronously work.
	if _, err := sqlDB.Exec(
		`SELECT * FROM t.kv`,
	); !testutils.IsError(err, `relation "t.kv" does not exist`) {
		t.Fatalf("different error than expected: %+v", err)
	}

	if gr, err := kvDB.Get(ctx, nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table namekey still exists")
	}

	// Can create a table with the same name.
	if err := createKVTable(sqlDB, numRows); err != nil {
		t.Fatal(err)
	}

	// A lot of garbage has been left behind to be cleaned up by the
	// asynchronous path.
	checkKeyCount(t, kvDB, tableSpan, 3*numRows)

	if err := descExists(sqlDB, true, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, true, tableDesc.ID); err != nil {
		t.Fatal(err)
	}
}

// Test that after a DROP TABLE the table eventually gets deleted.
func TestDropTableDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
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
	if err := createKVTable(sqlDB, numRows); err != nil {
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
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	if err := zoneExists(sqlDB, true, tableDesc.ID); err != nil {
		t.Fatal(err)
	}

	tableSpan := tableDesc.TableSpan()
	checkKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		if err := descExists(sqlDB, false, tableDesc.ID); err != nil {
			return err
		}

		return zoneExists(sqlDB, false, tableDesc.ID)
	})

	checkKeyCount(t, kvDB, tableSpan, 0)
}

// Tests dropping a table that is interleaved within
// another table.
func TestDropTableInterleavedDeleteData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			// Turn on quick garbage collection.
			AsyncExecQuickly: true,
		},
	}
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	numRows := 2*sql.TableTruncateChunkSize + 1
	createKVInterleavedTable(t, sqlDB, numRows)

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	tableDescInterleaved := sqlbase.GetTableDescriptor(kvDB, "t", "intlv")
	tableSpan := tableDesc.TableSpan()

	checkKeyCount(t, kvDB, tableSpan, 3*numRows)
	if _, err := sqlDB.Exec(`DROP TABLE t.intlv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents regressions where
	// name -> descriptor ID caches might make this statement erronously work.
	if _, err := sqlDB.Exec(`SELECT * FROM t.intlv`); !testutils.IsError(
		err, `relation "t.intlv" does not exist`,
	) {
		t.Fatalf("different error than expected: %v", err)
	}

	testutils.SucceedsSoon(t, func() error {
		return descExists(sqlDB, false, tableDescInterleaved.ID)
	})

	checkKeyCount(t, kvDB, tableSpan, numRows)
}

func TestDropTableInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	// We might still be able to read/write in the table inside this transaction
	// until the schema changer runs, but we shouldn't be able to ALTER it.
	if _, err := tx.Exec(`ALTER TABLE t.kv ADD COLUMN w CHAR`); !testutils.IsError(err,
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
	params, _ := createTestServerParams()
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

// Test commands while a table is being dropped.
func TestCommandsWhileTableBeingDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	// Block schema changers so that the table we're about to DROP is not
	// actually dropped; it will be left in the "deleted" state.
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	sql := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY);
`
	if _, err := db.Exec(sql); err != nil {
		t.Fatal(err)
	}

	// DROP the table
	if _, err := db.Exec(`DROP TABLE test.t`); err != nil {
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
	if _, err := db.Exec(`CREATE TABLE test.t(a INT PRIMARY KEY)`); !testutils.IsError(err, `relation "t" already exists`) {
		t.Fatal(err)
	}

	// Check that DROP TABLE with the same name returns a proper error.
	if _, err := db.Exec(`DROP TABLE test.t`); !testutils.IsError(err, `table "t" is being dropped`) {
		t.Fatal(err)
	}
}
