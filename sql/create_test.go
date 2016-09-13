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
// Author: Marc Berhault (marc@cockroachlabs.com)

package sql_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestDatabaseDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()
	ctx := context.TODO()

	expectedCounter := int64(keys.MaxReservedDescID + 1)

	// Test values before creating the database.
	// descriptor ID counter.
	if ir, err := kvDB.Get(ctx, keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	nameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "test")
	if gr, err := kvDB.Get(ctx, nameKey); err != nil {
		t.Fatal(err)
	} else if gr.Exists() {
		t.Fatal("expected non-existing key")
	}

	// Write a descriptor key that will interfere with database creation.
	dbDescKey := sqlbase.MakeDescMetadataKey(sqlbase.ID(expectedCounter))
	dbDesc := &sqlbase.Descriptor{
		Union: &sqlbase.Descriptor_Database{
			Database: &sqlbase.DatabaseDescriptor{
				Name:       "sentinel",
				ID:         sqlbase.ID(expectedCounter),
				Privileges: &sqlbase.PrivilegeDescriptor{},
			},
		},
	}
	if err := kvDB.CPut(ctx, dbDescKey, dbDesc, nil); err != nil {
		t.Fatal(err)
	}

	// Database creation should fail, and nothing should have been written.
	if _, err := sqlDB.Exec(`CREATE DATABASE test`); !testutils.IsError(err, "unexpected value") {
		t.Fatalf("unexpected error %v", err)
	}

	if ir, err := kvDB.Get(ctx, keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	start := roachpb.Key(keys.MakeTablePrefix(uint32(keys.NamespaceTableID)))
	if kvs, err := kvDB.Scan(ctx, start, start.PrefixEnd(), 0); err != nil {
		t.Fatal(err)
	} else {
		if a, e := len(kvs), server.GetBootstrapSchema().SystemDescriptorCount(); a != e {
			t.Fatalf("expected %d keys to have been written, found %d keys", e, a)
		}
	}

	// Remove the junk; allow database creation to proceed.
	if err := kvDB.Del(ctx, dbDescKey); err != nil {
		t.Fatal(err)
	}

	if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}
	expectedCounter++

	// Check keys again.
	// descriptor ID counter.
	if ir, err := kvDB.Get(ctx, keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	if gr, err := kvDB.Get(ctx, nameKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// database descriptor.
	if gr, err := kvDB.Get(ctx, dbDescKey); err != nil {
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
	if ir, err := kvDB.Get(ctx, keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	if gr, err := kvDB.Get(ctx, nameKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}

	// database descriptor.
	if gr, err := kvDB.Get(ctx, dbDescKey); err != nil {
		t.Fatal(err)
	} else if !gr.Exists() {
		t.Fatal("key is missing")
	}
}
