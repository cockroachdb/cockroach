// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// doGrantsRequest issues a GET to url with client and returns the response
// status code and body, failing the test on any transport error.
func doGrantsRequest(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func TestGetDatabaseGrants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Setup server.
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(db)
	// Create test databases.
	conn.Exec(t, `CREATE DATABASE test_db`)
	conn.Exec(t, `CREATE DATABASE no_access_db`)

	// Get the database IDs.
	var testDBID, noAccessDBID, systemDbId int
	conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_db'`).Scan(&testDBID)
	conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'no_access_db'`).Scan(&noAccessDBID)
	conn.QueryRow(t, `SELECT id FROM system.namespace WHERE "parentID" = 0 AND "parentSchemaID" = 0 AND name = 'system'`).Scan(&systemDbId)

	// Create test users.
	users := []string{"user1", "user2", "user3"}
	for _, user := range users {
		conn.Exec(t, fmt.Sprintf("CREATE USER %s", user))
	}

	// Setup clients.
	adminClient, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	authenticatedClient, _, err := s.GetAuthenticatedHTTPClientAndCookie(username.MakeSQLUsernameFromPreNormalizedString("no_access_user"), false, 1)
	require.NoError(t, err)

	// Grant different privileges to users.
	conn.Exec(t, `GRANT CREATE, CONNECT ON DATABASE test_db TO user1`)
	conn.Exec(t, `GRANT ZONECONFIG, BACKUP ON DATABASE test_db TO user2`)
	conn.Exec(t, `GRANT ALL ON DATABASE test_db TO user3`)
	conn.Exec(t, `REVOKE CONNECT, TEMPORARY ON DATABASE no_access_db FROM public`)

	// Define test cases.
	testCases := []struct {
		name           string
		dbID           int
		pageSize       int
		pageNum        int
		sortBy         string
		sortOrder      string
		expectedStatus int
		expectedTotal  int
		expected       []grantRecord
		// If specified, only this client will be used.
		// Otherwise, both the authenticated and admin client will be tested.
		client *http.Client
	}{
		{
			name:           "Grants on test_db",
			dbID:           testDBID,
			pageSize:       0,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			expectedTotal:  9,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "public", Privilege: "CONNECT"},
				{Grantee: "public", Privilege: "TEMPORARY"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "user1", Privilege: "CONNECT"},
				{Grantee: "user1", Privilege: "CREATE"},
				{Grantee: "user2", Privilege: "BACKUP"},
				{Grantee: "user2", Privilege: "ZONECONFIG"},
				{Grantee: "user3", Privilege: "ALL"},
			},
		},
		{
			name:           "Grants on test_db desc order by grantee",
			dbID:           testDBID,
			pageSize:       0,
			pageNum:        0,
			sortBy:         "grantee",
			sortOrder:      "desc",
			expectedStatus: http.StatusOK,
			expectedTotal:  9,
			expected: []grantRecord{
				{Grantee: "user3", Privilege: "ALL"},
				{Grantee: "user2", Privilege: "BACKUP"},
				{Grantee: "user2", Privilege: "ZONECONFIG"},
				{Grantee: "user1", Privilege: "CONNECT"},
				{Grantee: "user1", Privilege: "CREATE"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "public", Privilege: "CONNECT"},
				{Grantee: "public", Privilege: "TEMPORARY"},
				{Grantee: "admin", Privilege: "ALL"},
			},
		},
		{
			name:           "Grants on test_db asc order by privilege",
			dbID:           testDBID,
			pageSize:       0,
			pageNum:        0,
			sortBy:         "privilege",
			expectedStatus: http.StatusOK,
			expectedTotal:  9,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "user3", Privilege: "ALL"},
				{Grantee: "user2", Privilege: "BACKUP"},
				{Grantee: "public", Privilege: "CONNECT"},
				{Grantee: "user1", Privilege: "CONNECT"},
				{Grantee: "user1", Privilege: "CREATE"},
				{Grantee: "public", Privilege: "TEMPORARY"},
				{Grantee: "user2", Privilege: "ZONECONFIG"},
			},
		},
		{
			name:           "Grants on test_db limit 5",
			dbID:           testDBID,
			pageSize:       5,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			expectedTotal:  9,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "public", Privilege: "CONNECT"},
				{Grantee: "public", Privilege: "TEMPORARY"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "user1", Privilege: "CONNECT"},
			},
		},
		{
			name:           "No access to no_access_db (no_access_user) returns 404",
			dbID:           noAccessDBID,
			pageSize:       10,
			pageNum:        0,
			expectedStatus: http.StatusNotFound,
			expectedTotal:  0,
			client:         &authenticatedClient,
			expected:       []grantRecord{},
		},
		{
			name:           "Admin access to no_access_db",
			dbID:           noAccessDBID,
			pageSize:       10,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			client:         &adminClient,
			expectedTotal:  2,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
			},
		},
		{
			name:           "Page size and number combined on test_db",
			dbID:           testDBID,
			pageSize:       2,
			pageNum:        3,
			expectedStatus: http.StatusOK,
			expectedTotal:  9,
			expected: []grantRecord{
				{Grantee: "user1", Privilege: "CONNECT"},
				{Grantee: "user1", Privilege: "CREATE"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var queryParams string
			if tc.pageSize > 0 {
				queryParams += fmt.Sprintf("%s=%d&", pageSizeKey, tc.pageSize)
			}
			if tc.pageNum > 0 {
				queryParams += fmt.Sprintf("%s=%d", pageNumKey, tc.pageNum)
			}
			if tc.sortBy != "" {
				queryParams += fmt.Sprintf("%s=%s&", sortByKey, tc.sortBy)
			}
			if tc.sortOrder != "" {
				queryParams += fmt.Sprintf("%s=%s", sortOrderKey, tc.sortOrder)
			}
			url := fmt.Sprintf("%s/api/v2/grants/databases/%d/?%s", s.AdminURL(), tc.dbID, queryParams)
			req, err := http.NewRequest("GET", url, nil)
			require.NoError(t, err)

			clients := []*http.Client{&adminClient, &authenticatedClient}
			if tc.client != nil {
				clients = []*http.Client{tc.client}
			}

			for _, client := range clients {
				resp, err := client.Do(req)
				require.NoError(t, err)

				require.Equal(t, tc.expectedStatus, resp.StatusCode)

				if tc.expectedStatus == http.StatusOK {
					var apiResp databaseGrantsResponseWithPagination
					err = json.NewDecoder(resp.Body).Decode(&apiResp)
					require.NoError(t, err)

					require.Equal(t, tc.pageSize, apiResp.PaginationInfo.PageSize)
					require.Equal(t, tc.pageNum, apiResp.PaginationInfo.PageNum)
					require.Equal(t, tc.expectedTotal, int(apiResp.PaginationInfo.TotalResults))
					require.Equal(t, tc.pageSize, apiResp.PaginationInfo.PageSize)
					require.Equal(t, tc.pageNum, apiResp.PaginationInfo.PageNum)
					require.Len(t, apiResp.Results, len(tc.expected))

					for i, grant := range apiResp.Results {
						require.Equal(t, tc.expected[i].Grantee, grant.Grantee)
						require.Equal(t, tc.expected[i].Privilege, grant.Privilege)
					}
				}
				require.NoError(t, resp.Body.Close())
			}
		})
	}

	// Test for non-existent database.
	t.Run("Non-existent database", func(t *testing.T) {
		urlBase := fmt.Sprintf("%s/api/v2/grants/databases/", s.AdminURL())
		urls := []string{
			"not-an-int/",
			"99999/?pageSize=10&pageNum=0",
			"-1",
		}
		for _, url := range urls {
			req, err := http.NewRequest("GET", urlBase+url, nil)
			require.NoError(t, err)
			resp, err := adminClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		}
	})

	// A database the user has no privileges on must be indistinguishable from a
	// non-existent one: identical 404 status and body, with no name leaked. Tested
	// at offset 0 and past the last page (offset > 0), since those take different
	// code paths but must produce the same response.
	t.Run("Inaccessible database indistinguishable from non-existent", func(t *testing.T) {
		nonexistentURL := fmt.Sprintf("%s/api/v2/grants/databases/99999/?pageSize=10&pageNum=0", s.AdminURL())
		wantStatus, wantBody := doGrantsRequest(t, &authenticatedClient, nonexistentURL)
		require.Equal(t, http.StatusNotFound, wantStatus)

		for _, q := range []string{"pageSize=10&pageNum=0", "pageSize=5&pageNum=100"} {
			url := fmt.Sprintf("%s/api/v2/grants/databases/%d/?%s", s.AdminURL(), noAccessDBID, q)
			status, body := doGrantsRequest(t, &authenticatedClient, url)
			require.Equal(t, wantStatus, status, "query %q", q)
			require.Equal(t, wantBody, body, "query %q", q)
			require.NotContains(t, body, "no_access_db", "query %q", q)
		}
	})

	// A database the user can access, paged past its last grant, must return a 200
	// empty page with the name and pagination metadata preserved — not a spurious
	// 404. no_access_user sees test_db via the public CONNECT privilege.
	t.Run("Accessible database paged past end returns empty page", func(t *testing.T) {
		url := fmt.Sprintf("%s/api/v2/grants/databases/%d/?pageSize=5&pageNum=100", s.AdminURL(), testDBID)
		status, body := doGrantsRequest(t, &authenticatedClient, url)
		require.Equal(t, http.StatusOK, status)
		var apiResp databaseGrantsResponseWithPagination
		require.NoError(t, json.Unmarshal([]byte(body), &apiResp))
		require.Equal(t, "test_db", apiResp.Name)
		require.Empty(t, apiResp.Results)
		require.Equal(t, 5, apiResp.PaginationInfo.PageSize)
		require.Equal(t, 100, apiResp.PaginationInfo.PageNum)
	})

	t.Run("Retrieving grants for databases with special names", func(t *testing.T) {
		databases := []string{
			"mixedCaseDatabase", "database with spaces", "database-with-dashes", "databases.with spe. cial characters",
		}
		for _, db := range databases {
			conn.Exec(t, fmt.Sprintf(`CREATE DATABASE "%s"`, db))
			dbID := 0
			conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = $1`, db).Scan(&dbID)

			url := fmt.Sprintf("%s/api/v2/grants/databases/%d/?limit=10&offset=0", s.AdminURL(), dbID)
			req, err := http.NewRequest("GET", url, nil)
			require.NoError(t, err)
			resp, err := adminClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		}
	})

	t.Run("400 bad request", func(t *testing.T) {
		urlBase := fmt.Sprintf("%s/api/v2/grants/databases/", s.AdminURL())
		urls := []string{
			"23/?pageSize=fe",
			"23/?pageNum=fe",
			"23/?sortBy=grantee&sortOrder=ascending",
		}
		for _, url := range urls {
			req, err := http.NewRequest("GET", urlBase+url, nil)
			require.NoError(t, err)
			resp, err := adminClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		}
	})
}

func TestGetTableGrants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Setup server.
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	conn := sqlutils.MakeSQLRunner(s.ApplicationLayer().SQLConn(t))
	// Create test tables.
	conn.Exec(t, `CREATE DATABASE test_db`)
	conn.Exec(t, `CREATE DATABASE no_access_db`)
	conn.Exec(t, `CREATE TABLE test_db.test_table (id INT PRIMARY KEY, name STRING)`)
	conn.Exec(t, `CREATE TABLE test_db.no_access_table (id INT PRIMARY KEY, name STRING)`)
	conn.Exec(t, `CREATE TABLE no_access_db.no_access_table (id INT PRIMARY KEY, name STRING)`)

	// Get the table IDs.
	var testTableID, testDbNoAccessTableID, noAccessDbTestTableID int
	conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_table' AND "parentID" = (SELECT id FROM system.namespace WHERE name = 'test_db')`).Scan(&testTableID)
	conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'no_access_table' AND "parentID" = (SELECT id FROM system.namespace WHERE name = 'test_db')`).Scan(&testDbNoAccessTableID)
	conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'no_access_table' AND "parentID" = (SELECT id FROM system.namespace WHERE name = 'no_access_db')`).Scan(&noAccessDbTestTableID)

	// Create test users.
	users := []string{"user1", "user2", "user3"}
	for _, user := range users {
		conn.Exec(t, fmt.Sprintf("CREATE USER %s", user))
	}

	// Setup clients.
	adminClient, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	authenticatedClient, _, err := s.GetAuthenticatedHTTPClientAndCookie(username.MakeSQLUsernameFromPreNormalizedString("no_access_user"), false, 1)
	require.NoError(t, err)

	// Grant different privileges to users on the test_table.
	conn.Exec(t, `GRANT SELECT, INSERT ON TABLE test_db.test_table TO user1`)
	conn.Exec(t, `GRANT UPDATE, DELETE ON TABLE test_db.test_table TO user2`)
	conn.Exec(t, `GRANT ALL ON TABLE test_db.test_table TO user3`)
	conn.Exec(t, `REVOKE CONNECT, TEMPORARY ON DATABASE no_access_db FROM public`)

	// Define test cases.
	testCases := []struct {
		name           string
		tableID        int
		pageSize       int
		pageNum        int
		sortBy         string
		sortOrder      string
		expectedStatus int
		expectedTotal  int
		expected       []grantRecord
		client         *http.Client
	}{
		{
			name:           "Grants on test_table",
			tableID:        testTableID,
			pageSize:       0,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			expectedTotal:  7,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "user1", Privilege: "INSERT"},
				{Grantee: "user1", Privilege: "SELECT"},
				{Grantee: "user2", Privilege: "DELETE"},
				{Grantee: "user2", Privilege: "UPDATE"},
				{Grantee: "user3", Privilege: "ALL"},
			},
		},
		{
			name:           "Grants on test_table desc order by grantee",
			tableID:        testTableID,
			pageSize:       0,
			pageNum:        0,
			sortBy:         "grantee",
			sortOrder:      "desc",
			expectedStatus: http.StatusOK,
			expectedTotal:  7,
			expected: []grantRecord{
				{Grantee: "user3", Privilege: "ALL"},
				{Grantee: "user2", Privilege: "DELETE"},
				{Grantee: "user2", Privilege: "UPDATE"},
				{Grantee: "user1", Privilege: "INSERT"},
				{Grantee: "user1", Privilege: "SELECT"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "admin", Privilege: "ALL"},
			},
		},
		{
			name:           "Grants on test_table asc order by privilege",
			tableID:        testTableID,
			pageSize:       0,
			pageNum:        0,
			sortBy:         "privilege",
			expectedStatus: http.StatusOK,
			expectedTotal:  7,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "user3", Privilege: "ALL"},
				{Grantee: "user2", Privilege: "DELETE"},
				{Grantee: "user1", Privilege: "INSERT"},
				{Grantee: "user1", Privilege: "SELECT"},
				{Grantee: "user2", Privilege: "UPDATE"},
			},
		},
		{
			name:           "Grants on test_table with limit 5",
			tableID:        testTableID,
			pageSize:       5,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			expectedTotal:  7,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
				{Grantee: "user1", Privilege: "INSERT"},
				{Grantee: "user1", Privilege: "SELECT"},
				{Grantee: "user2", Privilege: "DELETE"},
			},
		},
		{
			name:           "Grants on test_db.no_access_table should still be visible",
			tableID:        testDbNoAccessTableID,
			pageSize:       10,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			expectedTotal:  2,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
			},
		},
		{
			name:           "Grants on no_access_db.no_access_table returns 404 (no name leak)",
			tableID:        noAccessDbTestTableID,
			pageSize:       10,
			pageNum:        0,
			expectedStatus: http.StatusNotFound,
			expectedTotal:  0,
			client:         &authenticatedClient,
			expected:       []grantRecord{},
		},
		{
			name:           "Admin access to no_access_db",
			tableID:        noAccessDbTestTableID,
			pageSize:       10,
			pageNum:        0,
			expectedStatus: http.StatusOK,
			client:         &adminClient,
			expectedTotal:  2,
			expected: []grantRecord{
				{Grantee: "admin", Privilege: "ALL"},
				{Grantee: "root", Privilege: "ALL"},
			},
		},
		{
			name:           "Page size and number combined on test_table",
			tableID:        testTableID,
			pageSize:       2,
			pageNum:        2,
			expectedTotal:  7,
			expectedStatus: http.StatusOK,
			expected: []grantRecord{
				{Grantee: "user1", Privilege: "INSERT"},
				{Grantee: "user1", Privilege: "SELECT"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var queryParams string
			if tc.pageSize > 0 {
				queryParams += fmt.Sprintf("%s=%d&", pageSizeKey, tc.pageSize)
			}
			if tc.pageNum > 0 {
				queryParams += fmt.Sprintf("%s=%d&", pageNumKey, tc.pageNum)
			}
			if tc.sortBy != "" {
				queryParams += fmt.Sprintf("%s=%s&", sortByKey, tc.sortBy)
			}
			if tc.sortOrder != "" {
				queryParams += fmt.Sprintf("%s=%s", sortOrderKey, tc.sortOrder)
			}
			url := fmt.Sprintf("%s/api/v2/grants/tables/%d/?%s", s.AdminURL(), tc.tableID, queryParams)
			req, err := http.NewRequest("GET", url, nil)
			require.NoError(t, err)

			clients := []*http.Client{&adminClient, &authenticatedClient}
			if tc.client != nil {
				clients = []*http.Client{tc.client}
			}

			for _, client := range clients {
				resp, err := client.Do(req)
				require.NoError(t, err)

				require.Equal(t, tc.expectedStatus, resp.StatusCode)

				if tc.expectedStatus != http.StatusOK {
					require.NoError(t, resp.Body.Close())
					continue
				}

				var apiResp tableGrantsResponseWithPagination
				err = json.NewDecoder(resp.Body).Decode(&apiResp)
				require.NoError(t, err)

				require.Equal(t, tc.pageSize, apiResp.PaginationInfo.PageSize)
				require.Equal(t, tc.pageNum, apiResp.PaginationInfo.PageNum)
				require.Equal(t, tc.expectedTotal, int(apiResp.PaginationInfo.TotalResults))
				require.Equal(t, tc.pageSize, apiResp.PaginationInfo.PageSize)
				require.Equal(t, tc.pageNum, apiResp.PaginationInfo.PageNum)
				require.Len(t, apiResp.Results, len(tc.expected))

				for i, grant := range apiResp.Results {
					require.Equal(t, tc.expected[i].Grantee, grant.Grantee)
					require.Equal(t, tc.expected[i].Privilege, grant.Privilege)
				}
				require.NoError(t, resp.Body.Close())
			}
		})
	}

	// Test for non-existent table.
	t.Run("Non-existent table", func(t *testing.T) {
		urlBase := fmt.Sprintf("%s/api/v2/grants/tables/", s.AdminURL())
		urls := []string{
			"not-an-int/",
			"99999/?pageSize=10&pageNum=0",
			"-1",
		}
		for _, url := range urls {
			req, err := http.NewRequest("GET", urlBase+url, nil)
			require.NoError(t, err)
			resp, err := adminClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusNotFound, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		}
	})

	// A table the user has no privileges on must be indistinguishable from a
	// non-existent one: identical 404 status and body, with no name leaked. Tested
	// at offset 0 and past the last page (offset > 0), since those take different
	// code paths but must produce the same response.
	t.Run("Inaccessible table indistinguishable from non-existent", func(t *testing.T) {
		nonexistentURL := fmt.Sprintf("%s/api/v2/grants/tables/99999/?pageSize=10&pageNum=0", s.AdminURL())
		wantStatus, wantBody := doGrantsRequest(t, &authenticatedClient, nonexistentURL)
		require.Equal(t, http.StatusNotFound, wantStatus)

		for _, q := range []string{"pageSize=10&pageNum=0", "pageSize=5&pageNum=100"} {
			url := fmt.Sprintf("%s/api/v2/grants/tables/%d/?%s", s.AdminURL(), noAccessDbTestTableID, q)
			status, body := doGrantsRequest(t, &authenticatedClient, url)
			require.Equal(t, wantStatus, status, "query %q", q)
			require.Equal(t, wantBody, body, "query %q", q)
			require.NotContains(t, body, "no_access_table", "query %q", q)
			require.NotContains(t, body, "no_access_db", "query %q", q)
		}
	})

	// A table the user can access, paged past its last grant, must return a 200
	// empty page with the names and pagination metadata preserved — not a spurious
	// 404. no_access_user sees test_db.test_table via the public CONNECT privilege
	// on test_db.
	t.Run("Accessible table paged past end returns empty page", func(t *testing.T) {
		url := fmt.Sprintf("%s/api/v2/grants/tables/%d/?pageSize=5&pageNum=100", s.AdminURL(), testTableID)
		status, body := doGrantsRequest(t, &authenticatedClient, url)
		require.Equal(t, http.StatusOK, status)
		var apiResp tableGrantsResponseWithPagination
		require.NoError(t, json.Unmarshal([]byte(body), &apiResp))
		require.Equal(t, "test_table", apiResp.TableName)
		require.Empty(t, apiResp.Results)
		require.Equal(t, 5, apiResp.PaginationInfo.PageSize)
		require.Equal(t, 100, apiResp.PaginationInfo.PageNum)
	})

	t.Run("Retrieving grants for tables with special names", func(t *testing.T) {
		tables := []string{
			"mixedCaseTable", "tables with spaces", "table-with-dashes", "tables.with spe. cial characters",
		}
		// Create a special schema too.
		conn.Exec(t, `CREATE database "my.db name"`)
		conn.Exec(t, `CREATE SCHEMA "my.db name"."my special. schema"`)
		for _, table := range tables {
			conn.Exec(t, fmt.Sprintf(`CREATE TABLE "my.db name"."my special. schema"."%s" (id INT PRIMARY KEY)`, table))
			tableID := 0
			conn.QueryRow(t, `SELECT id FROM system.namespace WHERE name = $1 AND "parentID" = (SELECT id FROM system.namespace WHERE name = 'my.db name')`, table).Scan(&tableID)

			url := fmt.Sprintf("%s/api/v2/grants/tables/%d/?pageSize=10&pageNum=0", s.AdminURL(), tableID)
			req, err := http.NewRequest("GET", url, nil)
			require.NoError(t, err)
			resp, err := adminClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			apiResp := tableGrantsResponseWithPagination{}
			err = json.NewDecoder(resp.Body).Decode(&apiResp)
			require.NoError(t, err)
			require.Equal(t, table, apiResp.TableName)
			require.NoError(t, resp.Body.Close())
		}
	})

	t.Run("400 bad request", func(t *testing.T) {
		urlBase := fmt.Sprintf("%s/api/v2/grants/tables/", s.AdminURL())
		urls := []string{
			"23/?pageSize=fe",
			"23/?pageNum=fe",
			"23/?sortBy=grantee&sortOrder=ascending",
		}
		for _, url := range urls {
			req, err := http.NewRequest("GET", urlBase+url, nil)
			require.NoError(t, err)
			resp, err := adminClient.Do(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		}
	})
}
