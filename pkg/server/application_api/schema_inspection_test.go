// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminAPIDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	ac := ts.AmbientCtx()
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	testDbName := generateRandomName()
	testDbEscaped := tree.NameString(testDbName)
	query := "CREATE DATABASE " + testDbEscaped
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	// Test needs to revoke CONNECT on the public database to properly exercise
	// fine-grained permissions logic.
	if _, err := db.Exec(fmt.Sprintf("REVOKE CONNECT ON DATABASE %s FROM public", testDbEscaped)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("REVOKE CONNECT ON DATABASE defaultdb FROM public"); err != nil {
		t.Fatal(err)
	}

	// We have to create the non-admin user before calling
	// "GRANT ... TO apiconstants.TestingUserNameNoAdmin".
	// This is done in "GetAuthenticatedHTTPClient".
	if _, err := ts.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession); err != nil {
		t.Fatal(err)
	}

	// Grant permissions to view the tables for the given viewing user.
	privileges := []string{"CONNECT"}
	query = fmt.Sprintf(
		"GRANT %s ON DATABASE %s TO %s",
		strings.Join(privileges, ", "),
		testDbEscaped,
		apiconstants.TestingUserNameNoAdmin().SQLIdentifier(),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	// Non admins now also require VIEWACTIVITY.
	query = fmt.Sprintf(
		"GRANT SYSTEM %s TO %s",
		"VIEWACTIVITY",
		apiconstants.TestingUserNameNoAdmin().SQLIdentifier(),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		expectedDBs []string
		isAdmin     bool
	}{
		{[]string{"defaultdb", "postgres", "system", testDbName}, true},
		{[]string{"postgres", testDbName}, false},
	} {
		t.Run(fmt.Sprintf("isAdmin:%t", tc.isAdmin), func(t *testing.T) {
			// Test databases endpoint.
			var resp serverpb.DatabasesResponse
			if err := srvtestutils.GetAdminJSONProtoWithAdminOption(
				ts,
				"databases",
				&resp,
				tc.isAdmin,
			); err != nil {
				t.Fatal(err)
			}

			if a, e := len(resp.Databases), len(tc.expectedDBs); a != e {
				t.Fatalf("length of result %d != expected %d", a, e)
			}

			sort.Strings(tc.expectedDBs)
			sort.Strings(resp.Databases)
			for i, e := range tc.expectedDBs {
				if a := resp.Databases[i]; a != e {
					t.Fatalf("database name %s != expected %s", a, e)
				}
			}

			// Test database details endpoint.
			var details serverpb.DatabaseDetailsResponse
			urlEscapeDbName := url.PathEscape(testDbName)

			if err := srvtestutils.GetAdminJSONProtoWithAdminOption(
				ts,
				"databases/"+urlEscapeDbName,
				&details,
				tc.isAdmin,
			); err != nil {
				t.Fatal(err)
			}

			if a, e := len(details.Grants), 3; a != e {
				t.Fatalf("# of grants %d != expected %d", a, e)
			}

			userGrants := make(map[string][]string)
			for _, grant := range details.Grants {
				switch grant.User {
				case username.AdminRole, username.RootUser, apiconstants.TestingUserNoAdmin:
					userGrants[grant.User] = append(userGrants[grant.User], grant.Privileges...)
				default:
					t.Fatalf("unknown grant to user %s", grant.User)
				}
			}
			for u, p := range userGrants {
				switch u {
				case username.AdminRole:
					if !reflect.DeepEqual(p, []string{"ALL"}) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				case username.RootUser:
					if !reflect.DeepEqual(p, []string{"ALL"}) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				case apiconstants.TestingUserNoAdmin:
					sort.Strings(p)
					if !reflect.DeepEqual(p, privileges) {
						t.Fatalf("privileges %v != expected %v", p, privileges)
					}
				default:
					t.Fatalf("unknown grant to user %s", u)
				}
			}

			// Verify Descriptor ID.
			databaseID, err := ts.QueryDatabaseID(ctx, username.RootUserName(), testDbName)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := details.DescriptorID, int64(databaseID); a != e {
				t.Fatalf("db had descriptorID %d, expected %d", a, e)
			}
		})
	}
}

func TestAdminAPIDatabaseDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	const errPattern = "database.+does not exist"
	if err := srvtestutils.GetAdminJSONProto(ts, "databases/i_do_not_exist", nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPIDatabaseSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	const fakedb = "system;DROP DATABASE system;"
	const path = "databases/" + fakedb
	const errPattern = `target database or schema does not exist`
	if err := srvtestutils.GetAdminJSONProto(ts, path, nil); !testutils.IsError(err, errPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDoesNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	const fakename = "i_do_not_exist"
	const badDBPath = "databases/" + fakename + "/tables/foo"
	const dbErrPattern = `relation \\"` + fakename + `.foo\\" does not exist`
	if err := srvtestutils.GetAdminJSONProto(ts, badDBPath, nil); !testutils.IsError(err, dbErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, dbErrPattern)
	}

	const badTablePath = "databases/system/tables/" + fakename
	const tableErrPattern = `relation \\"system.` + fakename + `\\" does not exist`
	if err := srvtestutils.GetAdminJSONProto(ts, badTablePath, nil); !testutils.IsError(err, tableErrPattern) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, tableErrPattern)
	}
}

func TestAdminAPITableSQLInjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	const fakeTable = "users;DROP DATABASE system;"
	const path = "databases/system/tables/" + fakeTable
	const errPattern = `relation \"system.` + fakeTable + `\" does not exist`
	if err := srvtestutils.GetAdminJSONProto(ts, path, nil); !testutils.IsError(err, regexp.QuoteMeta(errPattern)) {
		t.Fatalf("unexpected error: %v\nexpected: %s", err, errPattern)
	}
}

func TestAdminAPITableDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const schemaName = "testschema"

	for _, tc := range []struct {
		name, dbName, tblName, pkName string
	}{
		{name: "lower", dbName: "test", tblName: "tbl", pkName: "tbl_pkey"},
		{name: "lower other schema", dbName: "test", tblName: `testschema.tbl`, pkName: "tbl_pkey"},
		{name: "lower with space", dbName: "test test", tblName: `"tbl tbl"`, pkName: "tbl tbl_pkey"},
		{name: "upper", dbName: "TEST", tblName: `"TBL"`, pkName: "TBL_pkey"}, // Regression test for issue #14056
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer s.Stopper().Stop(context.Background())
			ts := s.ApplicationLayer()

			escDBName := tree.NameStringP(&tc.dbName)
			tblName := tc.tblName

			ac := ts.AmbientCtx()
			ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
			defer span.Finish()

			tableSchema := `nulls_allowed INT8,
							nulls_not_allowed INT8 NOT NULL DEFAULT 1000,
							default2 INT8 DEFAULT 2,
							string_default STRING DEFAULT 'default_string',
						  INDEX descidx (default2 DESC)`

			setupQueries := []string{
				fmt.Sprintf("CREATE DATABASE %s", escDBName),
				fmt.Sprintf("CREATE SCHEMA %s.%s", escDBName, schemaName),
				fmt.Sprintf(`CREATE TABLE %s.%s (%s)`, escDBName, tblName, tableSchema),
				"CREATE USER readonly",
				"CREATE USER app",
				fmt.Sprintf("GRANT SELECT ON %s.%s TO readonly", escDBName, tblName),
				fmt.Sprintf("GRANT SELECT,UPDATE,DELETE ON %s.%s TO app", escDBName, tblName),
				fmt.Sprintf("CREATE STATISTICS test_stats FROM %s.%s", escDBName, tblName),
			}
			db := ts.SQLConn(t, serverutils.DBName(tc.dbName))
			for _, q := range setupQueries {
				t.Logf("executing: %v", q)
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}

			// Perform API call.
			var resp serverpb.TableDetailsResponse
			url := fmt.Sprintf("databases/%s/tables/%s", tc.dbName, tblName)
			if err := srvtestutils.GetAdminJSONProto(ts, url, &resp); err != nil {
				t.Fatal(err)
			}

			// Verify columns.
			expColumns := []serverpb.TableDetailsResponse_Column{
				{Name: "nulls_allowed", Type: "INT8", Nullable: true, DefaultValue: ""},
				{Name: "nulls_not_allowed", Type: "INT8", Nullable: false, DefaultValue: "1000"},
				{Name: "default2", Type: "INT8", Nullable: true, DefaultValue: "2"},
				{Name: "string_default", Type: "STRING", Nullable: true, DefaultValue: "'default_string'"},
				{Name: "rowid", Type: "INT8", Nullable: false, DefaultValue: "unique_rowid()", Hidden: true},
			}
			testutils.SortStructs(expColumns, "Name")
			testutils.SortStructs(resp.Columns, "Name")
			if a, e := len(resp.Columns), len(expColumns); a != e {
				t.Fatalf("# of result columns %d != expected %d (got: %#v)", a, e, resp.Columns)
			}
			for i, a := range resp.Columns {
				e := expColumns[i]
				if a.String() != e.String() {
					t.Fatalf("mismatch at column %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify grants.
			expGrants := []serverpb.TableDetailsResponse_Grant{
				{User: username.AdminRole, Privileges: []string{"ALL"}},
				{User: username.RootUser, Privileges: []string{"ALL"}},
				{User: "app", Privileges: []string{"DELETE"}},
				{User: "app", Privileges: []string{"SELECT"}},
				{User: "app", Privileges: []string{"UPDATE"}},
				{User: "readonly", Privileges: []string{"SELECT"}},
			}
			testutils.SortStructs(expGrants, "User")
			testutils.SortStructs(resp.Grants, "User")
			if a, e := len(resp.Grants), len(expGrants); a != e {
				t.Fatalf("# of grant columns %d != expected %d (got: %#v)", a, e, resp.Grants)
			}
			for i, a := range resp.Grants {
				e := expGrants[i]
				sort.Strings(a.Privileges)
				sort.Strings(e.Privileges)
				if a.String() != e.String() {
					t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify indexes.
			expIndexes := []serverpb.TableDetailsResponse_Index{
				{Name: tc.pkName, Column: "string_default", Direction: "N/A", Unique: true, Seq: 5, Storing: true},
				{Name: tc.pkName, Column: "default2", Direction: "N/A", Unique: true, Seq: 4, Storing: true},
				{Name: tc.pkName, Column: "nulls_not_allowed", Direction: "N/A", Unique: true, Seq: 3, Storing: true},
				{Name: tc.pkName, Column: "nulls_allowed", Direction: "N/A", Unique: true, Seq: 2, Storing: true},
				{Name: tc.pkName, Column: "rowid", Direction: "ASC", Unique: true, Seq: 1},
				{Name: "descidx", Column: "rowid", Direction: "ASC", Unique: false, Seq: 2, Implicit: true},
				{Name: "descidx", Column: "default2", Direction: "DESC", Unique: false, Seq: 1},
			}
			testutils.SortStructs(expIndexes, "Name", "Seq")
			testutils.SortStructs(resp.Indexes, "Name", "Seq")
			for i, a := range resp.Indexes {
				e := expIndexes[i]
				if a.String() != e.String() {
					t.Fatalf("mismatch at index %d: actual %#v != %#v", i, a, e)
				}
			}

			// Verify range count.
			if a, e := resp.RangeCount, int64(1); a != e {
				t.Fatalf("# of ranges %d != expected %d", a, e)
			}

			// Verify Create Table Statement.
			{

				showCreateTableQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", escDBName, tblName)

				row := db.QueryRow(showCreateTableQuery)
				var createStmt, tableName string
				if err := row.Scan(&tableName, &createStmt); err != nil {
					t.Fatal(err)
				}

				if a, e := resp.CreateTableStatement, createStmt; a != e {
					t.Fatalf("mismatched create table statement; expected %s, got %s", e, a)
				}
			}

			// Verify statistics last updated.
			{

				showStatisticsForTableQuery := fmt.Sprintf("SELECT max(created) AS stats_last_created_at FROM [SHOW STATISTICS FOR TABLE %s.%s]", escDBName, tblName)

				row := db.QueryRow(showStatisticsForTableQuery)
				var createdTs time.Time
				if err := row.Scan(&createdTs); err != nil {
					t.Fatal(err)
				}

				if a, e := resp.StatsLastCreatedAt, createdTs; reflect.DeepEqual(a, e) {
					t.Fatalf("mismatched statistics creation timestamp; expected %s, got %s", e, a)
				}
			}

			// Verify Descriptor ID.
			tableID, err := ts.QueryTableID(ctx, username.RootUserName(), tc.dbName, tc.tblName)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := resp.DescriptorID, int64(tableID); a != e {
				t.Fatalf("table had descriptorID %d, expected %d", a, e)
			}
		})
	}
}

func TestAdminAPIDatabaseDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numServers = 3
	tc := testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	db := tc.ApplicationLayer(0).SQLConn(t)

	_, err := db.Exec("CREATE DATABASE test")
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE test.foo (id INT PRIMARY KEY, val STRING)")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err := db.Exec("INSERT INTO test.foo VALUES($1, $2)", i, "test")
		require.NoError(t, err)
	}

	// Flush all stores here so that we can read the ApproximateDiskBytes field without waiting for a flush.
	for i := 0; i < numServers; i++ {
		s := tc.Server(i).StorageLayer()
		err = s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			return store.TODOEngine().Flush()
		})
		require.NoError(t, err)
	}

	s := tc.Server(0).ApplicationLayer()

	var resp serverpb.DatabaseDetailsResponse
	require.NoError(t, serverutils.GetJSONProto(s, "/_admin/v1/databases/test", &resp))
	assert.Nil(t, resp.Stats, "No Stats unless we ask for them explicitly.")

	nodeIDs := tc.NodeIDs()
	testutils.SucceedsSoon(t, func() error {
		var resp serverpb.DatabaseDetailsResponse
		require.NoError(t, serverutils.GetJSONProto(s, "/_admin/v1/databases/test?include_stats=true", &resp))

		if resp.Stats.RangeCount != int64(1) {
			return errors.Newf("expected range-count=1, got %d", resp.Stats.RangeCount)
		}
		if len(resp.Stats.NodeIDs) != len(nodeIDs) {
			return errors.Newf("expected node-ids=%s, got %s", nodeIDs, resp.Stats.NodeIDs)
		}
		assert.Equal(t, nodeIDs, resp.Stats.NodeIDs, "NodeIDs")

		// We've flushed data so this estimation should be non-zero.
		assert.Positive(t, resp.Stats.ApproximateDiskBytes, "ApproximateDiskBytes")

		return nil
	})
}

func TestAdminAPITableStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "flaky under stress #107156")
	skip.UnderRace(t, "flaky under race #107156")

	const nodeCount = 3
	tc := testcluster.StartTestCluster(t, nodeCount, base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	})
	defer tc.Stopper().Stop(context.Background())

	server0 := tc.Server(0).ApplicationLayer()

	// Create clients (SQL, HTTP) connected to server 0.
	db := server0.SQLConn(t)

	client, err := server0.GetAdminHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	client.Timeout = time.Hour // basically no timeout

	// Make a single table and insert some data. The database and test have
	// names which require escaping, in order to verify that database and
	// table names are being handled correctly.
	if _, err := db.Exec(`CREATE DATABASE "test test"`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE "test test"."foo foo" (
			id INT PRIMARY KEY,
			val STRING
		)`,
	); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db.Exec(`
			INSERT INTO "test test"."foo foo" VALUES(
				$1, $2
			)`, i, "test",
		); err != nil {
			t.Fatal(err)
		}
	}

	url := server0.AdminURL().WithPath("/_admin/v1/databases/test test/tables/foo foo/stats").String()
	var tsResponse serverpb.TableStatsResponse

	// The new SQL table may not yet have split into its own range. Wait for
	// this to occur, and for full replication.
	testutils.SucceedsSoon(t, func() error {
		if err := httputil.GetJSON(client, url, &tsResponse); err != nil {
			t.Fatal(err)
		}
		if len(tsResponse.MissingNodes) != 0 {
			return errors.Errorf("missing nodes: %+v", tsResponse.MissingNodes)
		}
		if tsResponse.RangeCount != 1 {
			return errors.Errorf("Table range not yet separated.")
		}
		if tsResponse.NodeCount != nodeCount {
			return errors.Errorf("Table range not yet replicated to %d nodes.", 3)
		}
		if a, e := tsResponse.ReplicaCount, int64(nodeCount); a != e {
			return errors.Errorf("expected %d replicas, found %d", e, a)
		}
		if a, e := tsResponse.Stats.KeyCount, int64(30); a < e {
			return errors.Errorf("expected at least %d total keys, found %d", e, a)
		}
		return nil
	})

	if len(tsResponse.MissingNodes) > 0 {
		t.Fatalf("expected no missing nodes, found %v", tsResponse.MissingNodes)
	}

	// Kill a node, ensure it shows up in MissingNodes and that ReplicaCount is
	// lower.
	tc.StopServer(1)

	if err := httputil.GetJSON(client, url, &tsResponse); err != nil {
		t.Fatal(err)
	}
	if a, e := tsResponse.NodeCount, int64(nodeCount); a != e {
		t.Errorf("expected %d nodes, found %d", e, a)
	}
	if a, e := tsResponse.RangeCount, int64(1); a != e {
		t.Errorf("expected %d ranges, found %d", e, a)
	}
	if a, e := tsResponse.ReplicaCount, int64((nodeCount/2)+1); a != e {
		t.Errorf("expected %d replicas, found %d", e, a)
	}
	if a, e := tsResponse.Stats.KeyCount, int64(10); a < e {
		t.Errorf("expected at least 10 total keys, found %d", a)
	}
	if len(tsResponse.MissingNodes) != 1 {
		t.Errorf("expected one missing node, found %v", tsResponse.MissingNodes)
	}
	if len(tsResponse.NodeIDs) == 0 {
		t.Error("expected at least one node in NodeIds list")
	}

	// Call TableStats with a very low timeout. This tests that fan-out queries
	// do not leak goroutines if the calling context is abandoned.
	// Interestingly, the call can actually sometimes succeed, despite the small
	// timeout; however, in aggregate (or in stress tests) this will suffice for
	// detecting leaks.
	client.Timeout = 1 * time.Nanosecond
	_ = httputil.GetJSON(client, url, &tsResponse)
}
