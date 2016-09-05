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

	// createTestTable blocks until the signal channel is closed and then tries
	// to create a table whose name is based on the passed in id.
	createTestTable := func(
		id int,
		db *gosql.DB,
		wg *sync.WaitGroup,
		signal chan struct{},
		completed chan int,
		hideErrors bool,
	) {
		defer wg.Done()
		select {
		case <-signal:
		}

		tableSQL := fmt.Sprintf(`
			CREATE TABLE "test"."table_%d" (
			id INT PRIMARY KEY,
			val INT
		)`, id)
		if _, err := db.Exec(tableSQL); err != nil {
			if !hideErrors {
				t.Logf("table %d: could be created: %s", id, err)
			}
		} else {
			completed <- id
		}
	}

	var wg1 sync.WaitGroup
	wg1.Add(numberOfTables)
	signal1 := make(chan struct{})
	completed1 := make(chan int, numberOfTables)
	for i := 0; i < numberOfTables; i++ {
		db := tc.ServerConn(i % numberOfNodes)
		go createTestTable(i, db, &wg1, signal1, completed1, false /*hideErrors*/)
	}

	close(signal1)
	wg1.Wait()
	close(completed1)

	// testAndCountTables inserts a value into each table based on the ids of
	// the passed in channel. It then returns the total number of tables that
	// were created.
	testAndCountTables := func(completed chan int) int {
		var numTablesCreated int
		for id := range completed {
			insertTableSQL := fmt.Sprintf(`
			INSERT INTO "test"."table_%d"
			VALUES ($1, $1)
		`, id)
			if _, err := db0.Exec(insertTableSQL, id); err != nil {
				t.Fatal(err)
			}
			numTablesCreated++
		}
		return numTablesCreated
	}

	completedTables1 := testAndCountTables(completed1)
	if completedTables1 < minimumCreatedTables {
		t.Fatalf("expected at least %d tables created, only got %d", minimumCreatedTables, completedTables1)
	}

	// Perform the same test, but with conflicting table names. only one table
	// should be created.
	finalTableNumber := numberOfTables * 2
	var wg2 sync.WaitGroup
	wg2.Add(numberOfTables)
	signal2 := make(chan struct{})
	completed2 := make(chan int, numberOfTables)
	for i := 0; i < numberOfTables; i++ {
		db := tc.ServerConn(i % numberOfNodes)
		go createTestTable(finalTableNumber, db, &wg2, signal2, completed2, true /*hideErrors*/)
	}

	close(signal2)
	wg2.Wait()
	close(completed2)

	completedTables2 := testAndCountTables(completed2)
	if completedTables2 != 1 {
		t.Fatalf("expected 1 table, got %d", completedTables2)
	}
}
