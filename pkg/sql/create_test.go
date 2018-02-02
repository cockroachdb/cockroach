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
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func TestDatabaseDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
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

	// Even though the CREATE above failed, the counter is still incremented
	// (that's performed non-transactionally).
	expectedCounter++

	if ir, err := kvDB.Get(ctx, keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else if actual := ir.ValueInt(); actual != expectedCounter {
		t.Fatalf("expected descriptor ID == %d, got %d", expectedCounter, actual)
	}

	start := roachpb.Key(keys.MakeTablePrefix(uint32(keys.NamespaceTableID)))
	if kvs, err := kvDB.Scan(ctx, start, start.PrefixEnd(), 0); err != nil {
		t.Fatal(err)
	} else {
		descriptorIDs, err := sqlmigrations.ExpectedDescriptorIDs(ctx, kvDB)
		if err != nil {
			t.Fatal(err)
		}
		if e, a := len(descriptorIDs), len(kvs); a != e {
			t.Fatalf("expected %d keys to have been written, found %d keys", e, a)
		}
	}

	// Remove the junk; allow database creation to proceed.
	if err := kvDB.Del(ctx, dbDescKey); err != nil {
		t.Fatal(err)
	}

	dbDescKey = sqlbase.MakeDescMetadataKey(sqlbase.ID(expectedCounter))
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
		CREATE TABLE IF NOT EXISTS test.public.table_%d (
			id INT PRIMARY KEY,
			val INT
		)`, id)

	for {
		if _, err := db.Exec(tableSQL); err != nil {
			if testutils.IsSQLRetryableError(err) {
				continue
			}
			t.Errorf("table %d: could not be created: %s", id, err)
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
	descIDStart sqlbase.ID,
) {
	usedTableIDs := make(map[sqlbase.ID]string)
	var count int
	tableIDs := make(map[sqlbase.ID]struct{})
	maxID := descIDStart
	for id := range completed {
		count++
		tableName := fmt.Sprintf("table_%d", id)
		kvDB := tc.Servers[count%tc.NumServers()].DB()
		tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", tableName)
		if tableDesc.ID < descIDStart {
			t.Fatalf(
				"table %s's ID %d is too small. Expected >= %d",
				tableName,
				tableDesc.ID,
				descIDStart,
			)

			if _, ok := tableIDs[tableDesc.ID]; ok {
				t.Fatalf("duplicate ID: %d", id)
			}
			tableIDs[tableDesc.ID] = struct{}{}
			if tableDesc.ID > maxID {
				maxID = tableDesc.ID
			}

		}
		usedTableIDs[tableDesc.ID] = tableName
	}

	if e, a := expectedNumOfTables, len(usedTableIDs); e != a {
		t.Fatalf("expected %d tables created, only got %d", e, a)
	}

	// Check that no extra descriptors have been written in the range
	// descIDStart..maxID.
	kvDB := tc.Servers[0].DB()
	for id := descIDStart; id < maxID; id++ {
		if _, ok := tableIDs[id]; ok {
			continue
		}
		descKey := sqlbase.MakeDescMetadataKey(id)
		desc := &sqlbase.Descriptor{}
		if err := kvDB.GetProto(context.TODO(), descKey, desc); err != nil {
			t.Fatal(err)
		}
		if (*desc != sqlbase.Descriptor{}) {
			t.Fatalf("extra descriptor with id %d", id)
		}
	}
}

// TestParallelCreateTables tests that concurrent create table requests are
// correctly filled.
func TestParallelCreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
	kvDB := tc.Servers[0].DB()
	var descIDStart sqlbase.ID
	if descID, err := kvDB.Get(context.Background(), keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else {
		descIDStart = sqlbase.ID(descID.ValueInt())
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
	kvDB := tc.Servers[0].DB()
	var descIDStart sqlbase.ID
	if descID, err := kvDB.Get(context.Background(), keys.DescIDGenerator); err != nil {
		t.Fatal(err)
	} else {
		descIDStart = sqlbase.ID(descID.ValueInt())
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
		1, /* expectedNumOfTables */
		descIDStart,
	)
}

// Test that the modification time on a table descriptor is initialized.
func TestTableReadErrorsBeforeTableCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.timestamp (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		schema string
	}{
		{"CREATE TABLE t.public.kv0 (k CHAR PRIMARY KEY, v CHAR)"},
		{"CREATE TABLE t.public.kv1 AS SELECT * FROM t.public.kv0"},
		{"CREATE VIEW t.public.kv2 AS SELECT k, v FROM t.public.kv0"},
	}

	for i, testCase := range testCases {
		tx, err := sqlDB.Begin()
		if err != nil {
			t.Fatal(err)
		}

		// Insert an entry so that the transaction is guaranteed to be
		// assigned a timestamp.
		if _, err := tx.Exec(fmt.Sprintf(`
INSERT INTO t.public.timestamp VALUES ('%d', 'b');
`, i)); err != nil {
			t.Fatal(err)
		}

		// Create schema and read data so that a table lease is acquired.
		if _, err := sqlDB.Exec(testCase.schema); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(fmt.Sprintf(`
SELECT * FROM t.public.kv%d
`, i)); err != nil {
			t.Fatal(err)
		}

		// This select should not see any data.
		if _, err := tx.Query(fmt.Sprintf(
			`SELECT * FROM t.public.kv%d`, i,
		)); !testutils.IsError(err, fmt.Sprintf("id %d is not a table", 52+i)) {
			t.Fatalf("err = %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCreateStatementType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	e := s.Executor().(*sql.Executor)
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, e,
		nil /* remote */, &sql.MemoryMetrics{}, nil /* conn */)
	session.StartUnlimitedMonitor()
	defer session.Finish(e)

	query := "CREATE DATABASE t; CREATE TABLE t.public.foo(x INT); CREATE TABLE t.public.bar AS SELECT * FROM generate_series(1,10)"
	res, err := e.ExecuteStatementsBuffered(session, query, nil, 3)
	if err != nil {
		t.Fatal("expected no error, got", err)
	}
	defer res.Close(session.Ctx())
	if res.Empty {
		t.Fatal("expected non-empty results")
	}

	result := res.ResultList[1]
	if result.Err != nil {
		t.Fatal("expected no error, got", err)
	}
	if result.PGTag != "CREATE TABLE" {
		t.Fatal("expected CREATE TABLE, got", result.PGTag)
	}
	if result.Type != tree.DDL {
		t.Fatal("expected result type tree.DDL, got", result.Type)
	}
	if result.RowsAffected != 0 {
		t.Fatal("expected 0 rows affected, got", result.RowsAffected)
	}

	result = res.ResultList[2]
	if result.Err != nil {
		t.Fatal("expected no error, got", err)
	}
	if result.PGTag != "SELECT" {
		t.Fatal("expected SELECT, got", result.PGTag)
	}
	if result.Type != tree.RowsAffected {
		t.Fatal("expected result type tree.RowsAffected, got", result.Type)
	}
	if result.RowsAffected != 10 {
		t.Fatal("expected 10 rows affected, got", result.RowsAffected)
	}
}

// Test that the user's password cannot be set in insecure mode.
func TestSetUserPasswordInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.TODO())

	errFail := "cluster in insecure mode; user cannot use password authentication"

	testCases := []struct {
		sql       string
		errString string
	}{
		{"CREATE USER user1", ""},
		{"CREATE USER user2 WITH PASSWORD ''", "empty passwords are not permitted"},
		{"CREATE USER user2 WITH PASSWORD 'cockroach'", errFail},
		{"ALTER USER user1 WITH PASSWORD 'somepass'", errFail},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			_, err := sqlDB.Exec(testCase.sql)
			if testCase.errString != "" {
				if !testutils.IsError(err, testCase.errString) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}

	testCases = []struct {
		sql       string
		errString string
	}{
		{"CREATE USER $1 WITH PASSWORD $2", errFail},
		{"ALTER USER $1 WITH PASSWORD $2", errFail},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			stmt, err := sqlDB.Prepare(testCase.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stmt.Exec("user3", "cockroach")
			if testCase.errString != "" {
				if !testutils.IsError(err, testCase.errString) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
