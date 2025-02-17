// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v5"
)

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
			// Scenario where an ambiguous commit error happens is described in more
			// detail in
			// https://reviewable.io/reviews/cockroachdb/cockroach/10251#-KVGGLbjhbPdlR6EFlfL
			if testutils.IsError(err, "result is ambiguous") {
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
	kvDB *kv.DB,
	codec keys.SQLCodec,
	completed chan int,
	expectedNumOfTables int,
	descIDStart descpb.ID,
) {
	usedTableIDs := make(map[descpb.ID]string)
	var count int
	tableIDs := make(map[descpb.ID]struct{})
	maxID := descIDStart
	for id := range completed {
		count++
		tableName := fmt.Sprintf("table_%d", id)
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", tableName)
		if tableDesc.GetID() < descIDStart {
			t.Fatalf(
				"table %s's ID %d is too small. Expected >= %d",
				tableName,
				tableDesc.GetID(),
				descIDStart,
			)

			if _, ok := tableIDs[tableDesc.GetID()]; ok {
				t.Fatalf("duplicate ID: %d", id)
			}
			tableIDs[tableDesc.GetID()] = struct{}{}
			if tableDesc.GetID() > maxID {
				maxID = tableDesc.GetID()
			}

		}
		usedTableIDs[tableDesc.GetID()] = tableName
	}

	if e, a := expectedNumOfTables, len(usedTableIDs); e != a {
		t.Fatalf("expected %d tables created, only got %d", e, a)
	}

	// Check that no extra descriptors have been written in the range
	// descIDStart..maxID.
	for id := descIDStart; id < maxID; id++ {
		if _, ok := tableIDs[id]; ok {
			continue
		}
		descKey := catalogkeys.MakeDescMetadataKey(codec, id)
		desc := &descpb.Descriptor{}
		if err := kvDB.GetProto(context.Background(), descKey, desc); err != nil {
			t.Fatal(err)
		}
		if !desc.Equal(descpb.Descriptor{}) {
			t.Fatalf("extra descriptor with id %d", id)
		}
	}
}

// TestParallelCreateTables tests that concurrent create table requests are
// correctly filled.
func TestParallelCreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This number has to be around 10 or else testrace will take too long to
	// finish.
	const numberOfTables = 10
	const numberOfNodes = 3

	tc := testcluster.StartTestCluster(t, numberOfNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	if _, err := tc.ServerConn(0).Exec(`CREATE DATABASE "test"`); err != nil {
		t.Fatal(err)
	}
	// Get the id descriptor generator count.
	s := tc.Servers[0].ApplicationLayer()
	idgen := descidgen.NewGenerator(s.ClusterSettings(), s.Codec(), s.DB())
	descIDStart, err := idgen.PeekNextUniqueDescID(context.Background())
	if err != nil {
		t.Fatal(err)
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
		s.DB(),
		s.Codec(),
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
	defer log.Scope(t).Close(t)

	const numberOfTables = 30
	const numberOfNodes = 3

	tc := testcluster.StartTestCluster(t, numberOfNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	if _, err := tc.ServerConn(0).Exec(`CREATE DATABASE "test"`); err != nil {
		t.Fatal(err)
	}

	// Get the id descriptor generator count.
	s := tc.Servers[0].ApplicationLayer()
	idgen := descidgen.NewGenerator(s.ClusterSettings(), s.Codec(), s.DB())
	descIDStart, err := idgen.PeekNextUniqueDescID(context.Background())
	if err != nil {
		t.Fatal(err)
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
		s.DB(),
		s.Codec(),
		completed,
		1, /* expectedNumOfTables */
		descIDStart,
	)
}

// Test that the modification time on a table descriptor is initialized.
func TestTableReadErrorsBeforeTableCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := createTestServerParamsAllowTenants()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.timestamp (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		schema string
	}{
		{"CREATE TABLE t.kv0 (k CHAR PRIMARY KEY, v CHAR)"},
		{"CREATE TABLE t.kv1 AS SELECT * FROM t.kv0"},
		{"CREATE VIEW t.kv2 AS SELECT k, v FROM t.kv0"},
	}

	for i, testCase := range testCases {
		tx, err := sqlDB.Begin()
		if err != nil {
			t.Fatal(err)
		}

		// Insert an entry so that the transaction is guaranteed to be
		// assigned a timestamp.
		if _, err := tx.Exec(fmt.Sprintf(`
INSERT INTO t.timestamp VALUES ('%d', 'b');
`, i)); err != nil {
			t.Fatal(err)
		}

		// Create schema and read data so that a table lease is acquired.
		if _, err := sqlDB.Exec(testCase.schema); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(fmt.Sprintf(`
SELECT * FROM t.kv%d
`, i)); err != nil {
			t.Fatal(err)
		}

		// This select should not see any data.
		if _, err := tx.Query(fmt.Sprintf(
			`SELECT * FROM t.kv%d`, i,
		)); !testutils.IsError(err, fmt.Sprintf("relation \"t.kv%d\" does not exist", i)) {
			t.Fatalf("err = %v", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCreateStatementType(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// This test does not use s.SQLConn() because it cares about looking
	// at the statement tags in responses.
	pgURL, cleanup := pgurlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, pgxConfig)
	if err != nil {
		t.Fatal(err)
	}

	cmdTag, err := conn.Exec(ctx, "CREATE DATABASE t")
	if err != nil {
		t.Fatal(err)
	}
	if cmdTag.String() != "CREATE DATABASE" {
		t.Fatal("expected CREATE DATABASE, got", cmdTag)
	}

	cmdTag, err = conn.Exec(ctx, "CREATE TABLE t.foo(x INT)")
	if err != nil {
		t.Fatal(err)
	}
	if cmdTag.String() != "CREATE TABLE" {
		t.Fatal("expected CREATE TABLE, got", cmdTag)
	}

	cmdTag, err = conn.Exec(ctx, "CREATE TABLE t.bar AS SELECT * FROM generate_series(1,10)")
	if err != nil {
		t.Fatal(err)
	}
	if cmdTag.String() != "CREATE TABLE AS" {
		t.Fatal("expected CREATE TABLE AS, got", cmdTag)
	}
}

// Test that the user's password cannot be set in insecure mode.
func TestSetUserPasswordInsecure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.Background())

	errFail := "setting or updating a password is not supported in insecure mode"

	testCases := []struct {
		sql       string
		errString string
	}{
		{"CREATE USER user1", ""},
		{"CREATE USER user2 WITH PASSWORD ''", errFail},
		{"CREATE USER user2 WITH PASSWORD 'cockroach'", errFail},
		{"CREATE USER user3 WITH PASSWORD NULL", ""},
		{"ALTER USER user1 WITH PASSWORD 'somepass'", errFail},
		{"ALTER USER user1 WITH PASSWORD NULL", ""},
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
		{"CREATE USER user3 WITH PASSWORD $1", errFail},
		{"ALTER USER user3 WITH PASSWORD $1", errFail},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			stmt, err := sqlDB.Prepare(testCase.sql)
			if err != nil {
				t.Fatal(err)
			}
			_, err = stmt.Exec("cockroach")
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
