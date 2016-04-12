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
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

func TestDropDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	dbNameKey := sql.MakeNameMetadataKey(keys.RootNamespaceID, "t")
	r, pErr := kvDB.Get(dbNameKey)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !r.Exists() {
		t.Fatalf(`database "t" does not exist`)
	}
	dbDescKey := sql.MakeDescMetadataKey(sql.ID(r.ValueInt()))
	desc := &sql.Descriptor{}
	if pErr := kvDB.GetProto(dbDescKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	dbDesc := desc.GetDatabase()

	tbNameKey := sql.MakeNameMetadataKey(dbDesc.ID, "kv")
	gr, pErr := kvDB.Get(tbNameKey)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !gr.Exists() {
		t.Fatalf(`table "kv" does not exist`)
	}
	tbDescKey := sql.MakeDescMetadataKey(sql.ID(gr.ValueInt()))
	if pErr := kvDB.GetProto(tbDescKey, desc); pErr != nil {
		t.Fatal(pErr)
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

	tbZoneKey := sql.MakeZoneKey(tbDesc.ID)
	dbZoneKey := sql.MakeZoneKey(dbDesc.ID)
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
		t.Fatalf("table descriptor still exists after database is dropped")
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
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
CREATE INDEX foo on t.kv (v);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	nameKey := sql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, pErr := kvDB.Get(nameKey)
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	descKey := sql.MakeDescMetadataKey(sql.ID(gr.ValueInt()))
	desc := &sql.Descriptor{}
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	tableDesc := desc.GetTable()

	status, i, err := tableDesc.FindIndexByName("foo")
	if err != nil {
		t.Fatal(err)
	}
	if status != sql.DescriptorActive {
		t.Fatal("Index 'foo' is not active.")
	}
	indexPrefix := sql.MakeIndexKeyPrefix(tableDesc.ID, tableDesc.Indexes[i].ID)

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

	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}
	tableDesc = desc.GetTable()
	if _, _, err := tableDesc.FindIndexByName("foo"); err == nil {
		t.Fatalf("table descriptor still contains index after index is dropped")
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestDropTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR);
INSERT INTO t.kv VALUES ('c', 'e'), ('a', 'c'), ('b', 'd');
`); err != nil {
		t.Fatal(err)
	}

	nameKey := sql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, pErr := kvDB.Get(nameKey)
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}

	descKey := sql.MakeDescMetadataKey(sql.ID(gr.ValueInt()))
	desc := &sql.Descriptor{}
	if pErr := kvDB.GetProto(descKey, desc); pErr != nil {
		t.Fatal(pErr)
	}
	tableDesc := desc.GetTable()

	// Add a zone config for the table.
	cfg := config.DefaultZoneConfig()
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`INSERT INTO system.zones VALUES ($1, $2)`, tableDesc.ID, buf); err != nil {
		t.Fatal(err)
	}

	zoneKey := sql.MakeZoneKey(tableDesc.ID)
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
