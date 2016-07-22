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
// Author: XisiHuang (cockhuangxh@163.com)

package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

func TestDropDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
	r, err := kvDB.Get(dbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(r.ValueInt()))
	desc := &sqlbase.Descriptor{}
	if err := kvDB.GetProto(dbDescKey, desc); err != nil {
		t.Fatal(err)
	}
	dbDesc := desc.GetDatabase()

	tbNameKey := sqlbase.MakeNameMetadataKey(dbDesc.ID, "kv")
	gr, err := kvDB.Get(tbNameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))
	if err := kvDB.GetProto(tbDescKey, desc); err != nil {
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

	tbZoneKey := sqlbase.MakeZoneKey(tbDesc.ID)
	dbZoneKey := sqlbase.MakeZoneKey(dbDesc.ID)
	if gr, err := kvDB.Get(tbZoneKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatalf("table zone config entry not found")
	}
	if gr, err := kvDB.Get(dbZoneKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatalf("database zone config entry not found")
	}

	tablePrefix := keys.MakeTablePrefix(uint32(tbDesc.ID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 6; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if _, err := sqlDB.Exec(`DROP DATABASE t`); err != nil {
		t.Fatal(err)
	}

	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 0; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if gr, err := kvDB.Get(tbDescKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table descriptor still exists after database is dropped: %q", tbDescKey)
	}

	if gr, err := kvDB.Get(tbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table descriptor key still exists after database is dropped")
	}

	if gr, err := kvDB.Get(dbDescKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database descriptor still exists after database is dropped")
	}

	if gr, err := kvDB.Get(dbNameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database descriptor key still exists after database is dropped")
	}

	if gr, err := kvDB.Get(tbZoneKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table zone config entry still exists after the database is dropped")
	}

	if gr, err := kvDB.Get(dbZoneKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("database zone config entry still exists after the database is dropped")
	}
}

func TestDropIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
CREATE INDEX foo on t.kv (v);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")

	status, i, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	if status != sqlbase.DescriptorActive {
		t.Fatal("Index 'foo' is not active.")
	}
	indexPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.Indexes[i].ID)

	indexStartKey := roachpb.Key(indexPrefix)
	indexEndKey := indexStartKey.PrefixEnd()
	if kvs, err := kvDB.Scan(indexStartKey, indexEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 3; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if _, err := sqlDB.Exec(`DROP INDEX t.kv@foo`); err != nil {
		t.Fatal(err)
	}

	if kvs, err := kvDB.Scan(indexStartKey, indexEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 0; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "kv")

	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, FAMILY (k), FAMILY (v));
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "kv")
	nameKey := sqlbase.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, err := kvDB.Get(nameKey)

	if err != nil {
		t.Fatal(err)
	}

	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	descKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(gr.ValueInt()))

	// Add a zone config for the table.
	cfg := config.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	zoneKey := sqlbase.MakeZoneKey(tableDesc.ID)
	if gr, err := kvDB.Get(zoneKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatalf("zone config entry not found")
	}

	tablePrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 6; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if _, err := sqlDB.Exec(`DROP TABLE t.kv`); err != nil {
		t.Fatal(err)
	}

	// Test that deleted table cannot be used. This prevents regressions where
	// name -> descriptor ID caches might make this statement erronously work.
	if _, err := sqlDB.Exec(`SELECT * FROM t.kv`); !testutils.IsError(err, `table "t.kv" does not exist`) {
		t.Fatalf("different error than expected: %s", err)
	}

	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if l := 0; len(kvs) != l {
		t.Fatalf("expected %d key value pairs, but got %d", l, len(kvs))
	}

	if gr, err := kvDB.Get(descKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table descriptor still exists after the table is dropped")
	}

	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("table namekey still exists after the table is dropped")
	}

	if gr, err := kvDB.Get(zoneKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatalf("zone config entry still exists after the table is dropped")
	}
}

func TestDropTableInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

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
		`table "kv" has been deleted`) {
		t.Fatalf("different error than expected: %s", err)
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
	defer s.Stopper().Stop()

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
