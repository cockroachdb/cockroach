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
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/testutils/testcluster"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/syncutil"
)

func TestDatabaseDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop()

	expectedCounter := int64(keys.MaxReservedDescID + 1)

	// Test values before creating the database.
	// descriptor ID counter.
	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	// Database name.
	nameKey := sqlbase.MakeNameMetadataKey(keys.RootNamespaceID, "test")
	if gr, err := kvDB.Get(nameKey); err != nil {
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
	if err := kvDB.CPut(dbDescKey, dbDesc, nil); err != nil {
		t.Fatal(err)
	}

	// Database creation should fail, and nothing should have been written.
	if _, err := sqlDB.Exec(`CREATE DATABASE test`); !testutils.IsError(err, "unexpected value") {
		t.Fatalf("unexpected error %v", err)
	}

	if ir, err := kvDB.Get(keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	start := roachpb.Key(keys.MakeTablePrefix(uint32(keys.NamespaceTableID)))
	if kvs, err := kvDB.Scan(start, start.PrefixEnd(), 0); err != nil {
		t.Fatal(err)
	} else {
		if a, e := len(kvs), server.GetBootstrapSchema().SystemDescriptorCount(); a != e {
			t.Fatalf("expected %d keys to have been written, found %d keys", e, a)
		}
	}

	// Remove the junk; allow database creation to proceed.
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

// TestCreateTable tests that concurrent create table requests are correctly
// filled.
func TestCreateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numberOfTables = 30
	const minimumCreatedTables = 10
	const numberOfNodes = 3

	tc := testcluster.StartTestCluster(t, numberOfNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop()
	if err := tc.WaitForFullReplication(); err != nil {
		t.Fatal(err)
	}

	db0 := tc.ServerConn(0)
	if _, err := db0.Exec(`CREATE DATABASE "test"`); err != nil {
		t.Fatal(err)
	}

	// Completed stores all reported created tables.
	completed := struct {
		syncutil.Mutex
		tables map[int]struct{}
	}{
		tables: make(map[int]struct{}),
	}

	createTestTable := func(
		ID int,
		db *gosql.DB,
		wg *sync.WaitGroup,
		signal chan struct{},
		hideErrors bool,
	) {
		defer wg.Done()
		select {
		case <-tc.Stopper().ShouldStop():
			t.Fatalf("text exiting, start signal never received")
		case <-signal:
		}

		tableSQL := fmt.Sprintf(`
			CREATE TABLE "test"."table_%d" (
			id INT PRIMARY KEY,
			val INT
		)`, ID)
		if _, err := db.Exec(tableSQL); err != nil {
			if !hideErrors {
				t.Logf("table %d: could be created: %s", ID, err)
			}
		} else {
			// Mark the table as created.
			completed.Lock()
			defer completed.Unlock()
			completed.tables[ID] = struct{}{}
		}
	}

	var wg1 sync.WaitGroup
	wg1.Add(numberOfTables)
	signal1 := make(chan struct{})
	for i := 0; i < numberOfTables; i++ {
		db := tc.ServerConn(i % numberOfNodes)
		go createTestTable(i, db, &wg1, signal1, false /*hideErrors*/)
	}

	close(signal1)
	wg1.Wait()

	completed.Lock()
	for ID := range completed.tables {
		insertTableSQL := fmt.Sprintf(`
			INSERT INTO "test"."table_%d"
			VALUES ($1, $1)
		`, ID)
		if _, err := db0.Exec(insertTableSQL, ID); err != nil {
			t.Fatal(err)
		}
	}
	totalCreated := len(completed.tables)
	completed.Unlock()
	if totalCreated < minimumCreatedTables {
		t.Fatalf("expected at least %d tables created, only got %d", minimumCreatedTables, totalCreated)
	}

	// Perform the same test, but with conflicting table names. only one table
	// should be created.
	finalTableNumber := numberOfTables * 2
	var wg2 sync.WaitGroup
	wg2.Add(numberOfTables)
	signal2 := make(chan struct{})
	for i := 0; i < numberOfTables; i++ {
		db := tc.ServerConn(i % numberOfNodes)
		go createTestTable(finalTableNumber, db, &wg2, signal2, true /*hideErrors*/)
	}

	close(signal2)
	wg2.Wait()

	insertFinalTableSQL := fmt.Sprintf(`
		INSERT INTO "test"."table_%d"
		VALUES (0, 0)
	`, finalTableNumber)
	if _, err := db0.Exec(insertFinalTableSQL); err != nil {
		t.Fatal(err)
	}
}
