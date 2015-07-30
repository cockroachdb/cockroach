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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func setup(t *testing.T) (*server.TestServer, *sql.DB, *client.DB) {
	s := server.StartTestServer(nil)
	sqlDB, err := sql.Open("cockroach", "https://root@"+s.ServingAddr()+"?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}
	kvDB, err := client.Open("https://root@" + s.ServingAddr() + "?certs=test_certs")
	if err != nil {
		t.Fatal(err)
	}

	return s, sqlDB, kvDB
}

func cleanup(s *server.TestServer, db *sql.DB) {
	_ = db.Close()
	s.Stop()
}

func TestDatabaseDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, kvDB := setup(t)
	defer cleanup(s, sqlDB)

	// The first `MaxReservedDescID` (plus 0) are set aside.
	expectedCounter := int64(structured.MaxReservedDescID + 1)

	// Test values before creating the database.
	// descriptor ID counter.
	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if ir.ValueInt() != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, ir.ValueInt())
	}

	// Database name.
	nameKey := keys.MakeNameMetadataKey(structured.RootNamespaceID, "test")
	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatal("expected non-existing key")
	}

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	// Check keys again.
	// descriptor ID counter.
	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if ir.ValueInt() != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, ir.ValueInt())
	}

	// Database name.
	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// database descriptor.
	descKey := keys.MakeDescMetadataKey(uint32(expectedCounter - 1))
	if gr, err := kvDB.Get(descKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// Now try to create it again. We should fail, but not increment the counter.
	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err == nil {
		t.Fatal("failure expected")
	}

	// Check keys again.
	// descriptor ID counter.
	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if ir.ValueInt() != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, ir.ValueInt())
	}

	// Database name.
	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// database descriptor.
	if gr, err := kvDB.Get(descKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}
}
