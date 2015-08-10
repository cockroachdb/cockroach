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
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

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
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	nameKey := structured.MakeNameMetadataKey(structured.RootNamespaceID, "test")
	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatal("expected non-existing key")
	}

	// Write some junk that is going to interfere with table creation.
	dbDescKey := structured.MakeDescMetadataKey(structured.ID(expectedCounter))
	if err := kvDB.CPut(dbDescKey, "foo", nil); err != nil {
		t.Fatal(err)
	}

	// Table creation should fail, and nothing should have been written.
	if _, err := sqlDB.Exec(`CREATE DATABASE test`); !testutils.IsError(err, "unexpected value") {
		t.Fatalf("unexpected error %s", err)
	}

	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	if kvs, err := kvDB.Scan(keys.NameMetadataPrefix, keys.NameMetadataPrefix.PrefixEnd(), 0); err != nil {
		t.Fatal(err)
	} else {
		if a, e := len(kvs), 0; a != e {
			t.Fatalf("expected %d keys to have been written, found %d keys", e, a)
		}
	}

	// Remove the junk; allow table creation to proceed.
	if err := kvDB.Del(dbDescKey); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	// Check keys again.
	// descriptor ID counter.
	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// database descriptor.
	if gr, err := kvDB.Get(dbDescKey); err != nil {
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
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	if gr, err := kvDB.Get(nameKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// database descriptor.
	if gr, err := kvDB.Get(dbDescKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}
}
