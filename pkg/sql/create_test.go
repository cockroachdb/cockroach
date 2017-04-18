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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/migrations"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDatabaseDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())
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
		migrationDescriptors, _, err := migrations.AdditionalInitialDescriptors(ctx, kvDB)
		if err != nil {
			t.Fatal(err)
		}
		e := server.GetBootstrapSchema().SystemDescriptorCount() + migrationDescriptors
		if a := len(kvs); a != e {
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

// createTestTable tries to create a new table named based on the passed in id.
// It is designed to be synced with a number of concurrent calls to this
// function. Before starting, it first signals a done on the start waitgroup
// and then will block until the signal channel is closed. Once closed, it will
// proceed to try to create the table. Once the table creation is finished (be
// it successful or not) it signals a done on the end waitgroup.
func createTestTable(
	t *testing.T,
	tc *testcluster.TestCluster,
	id int,
	db *gosql.DB,
	wgStart *sync.WaitGroup,
	wgEnd *sync.WaitGroup,
	signal chan struct{},
	completed chan int,
) {
	defer wgEnd.Done()

	wgStart.Done()
	<-signal

	tableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "test"."table_%d" (
			id INT PRIMARY KEY,
			val INT
		)`, id)

	for {
		if _, err := db.Exec(tableSQL); err != nil {
			if testutils.IsSQLRetryableError(err) {
				continue
			}
			t.Errorf("table %d: could not be created: %v", id, err)
			return
		}
		completed <- id
		break
	}
}

// verifyTables ensures that the correct number of tables were created and that
// they all correspond to individual table descriptor IDs in the correct range
// of values.
func verifyTables(
	t *testing.T,
	tc *testcluster.TestCluster,
	completed chan int,
	expectedNumOfTables int,
	descIDStart int64,
) {
	descIDEnd := descIDStart + int64(expectedNumOfTables)
	usedTableIDs := make(map[sqlbase.ID]string)
	var count int
	for id := range completed {
		count++
		tableName := fmt.Sprintf("table_%d", id)
		kvDB := tc.Servers[count%tc.NumServers()].KVClient().(*client.DB)
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", tableName)
		if int64(tableDesc.ID) < descIDStart || int64(tableDesc.ID) >= descIDEnd {
			t.Fatalf(
				"table %s's ID %d is not within the expected range of %d to %d",
				tableName,
				tableDesc.ID,
				descIDStart,
				descIDEnd,
			)
		}
		usedTableIDs[tableDesc.ID] = tableName
	}

	if e, a := expectedNumOfTables, len(usedTableIDs); e != a {
		t.Fatalf("expected %d tables created, only got %d", e, a)
	}

	kvDB := tc.Servers[count%tc.NumServers()].KVClient().(*client.DB)
	if descID, err := kvDB.Get(context.Background(), keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else {
		if e, a := descIDEnd, descID.ValueInt(); e != a {
			t.Fatalf("expected next descriptor ID to be %d, got %d", e, a)
		}
	}
}

// TestParallelCreateTables tests that concurrent create table requests are
// correctly filled.
func TestParallelCreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#13504 Test is flaky under stress.")

	// This number has to be around 10 or else testrace will take too long to
	// finish.
	const numberOfTables = 10
	const numberOfNodes = 3

	tc := testcluster.StartTestCluster(t, numberOfNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())

	if _, err := tc.ServerConn(0).Exec(`CREATE DATABASE "test"`); err != nil {
		t.Fatal(err)
	}
	// Get the id descriptor generator count.
	kvDB := tc.Servers[0].KVClient().(*client.DB)
	var descIDStart int64
	if descID, err := kvDB.Get(context.Background(), keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else {
		descIDStart = descID.ValueInt()
	}

	var wgStart sync.WaitGroup
	var wgEnd sync.WaitGroup
	wgStart.Add(numberOfTables)
	wgEnd.Add(numberOfTables)
	signal := make(chan struct{})
	completed := make(chan int, numberOfTables)
	for i := 0; i < numberOfTables; i++ {
		db := tc.ServerConn(i % numberOfNodes)
		go createTestTable(t, tc, i, db, &wgStart, &wgEnd, signal, completed)
	}

	// Wait until all goroutines are ready.
	wgStart.Wait()
	// Signal the create table goroutines to start.
	close(signal)
	// Wait until all create tables are finished.
	wgEnd.Wait()
	close(completed)

	verifyTables(
		t,
		tc,
		completed,
		numberOfTables,
		descIDStart,
	)
}

// TestParallelCreateConflictingTables tests that concurrent create table
// requests with same name are only filled once. This is the same test as
// TestParallelCreateTables but in this test the tables names are all the same
// and is designed to specifically test the IF NOT EXIST clause.
func TestParallelCreateConflictingTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numberOfTables = 30
	const numberOfNodes = 3

	tc := testcluster.StartTestCluster(t, numberOfNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.TODO())

	if _, err := tc.ServerConn(0).Exec(`CREATE DATABASE "test"`); err != nil {
		t.Fatal(err)
	}

	// Get the id descriptor generator count.
	kvDB := tc.Servers[0].KVClient().(*client.DB)
	var descIDStart int64
	if descID, err := kvDB.Get(context.Background(), keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else {
		descIDStart = descID.ValueInt()
	}

	var wgStart sync.WaitGroup
	var wgEnd sync.WaitGroup
	wgStart.Add(numberOfTables)
	wgEnd.Add(numberOfTables)
	signal := make(chan struct{})
	completed := make(chan int, numberOfTables)
	for i := 0; i < numberOfTables; i++ {
		db := tc.ServerConn(i % numberOfNodes)
		go createTestTable(t, tc, 0, db, &wgStart, &wgEnd, signal, completed)
	}

	// Wait until all goroutines are ready.
	wgStart.Wait()
	// Signal the create table goroutines to start.
	close(signal)
	// Wait until all create tables are finished.
	wgEnd.Wait()
	close(completed)

	verifyTables(
		t,
		tc,
		completed,
		1,
		descIDStart,
	)
}
