// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"cmp"
	"context"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func defaultTMComparator(first, second tableMetadata) int {
	return cmp.Compare(first.TableId, second.TableId)
}

func defaultDMComparator(first, second dbMetadata) int { return cmp.Compare(first.DbId, second.DbId) }

func descendingComparator[T any](comparator func(first, second T) int) func(first, second T) int {
	return func(f, s T) int {
		return -1 * comparator(f, s)
	}
}

func TestGetTableMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	conn := testCluster.ServerConn(0)
	defer conn.Close()
	var (
		db1Name = "new_test_db_1"
		db2Name = "new_test_db_2"
	)
	db1Id, db2Id := setupTest(t, conn, db1Name, db2Name)

	ts := testCluster.Server(0)
	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)
	t.Run("non GET method 405 error", func(t *testing.T) {
		req, err := http.NewRequest("POST", ts.AdminURL().WithPath("/api/v2/table_metadata/?dbId=10").String(), nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 405, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(respBytes), "Method Not Allowed")
	})
	t.Run("unknown db id", func(t *testing.T) {
		mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, client, ts.AdminURL().WithPath("/api/v2/table_metadata/?dbId=1000").String())
		require.Len(t, mdResp.Results, 0)
		require.Equal(t, int64(0), mdResp.PaginationInfo.TotalResults)
	})
	t.Run("authorization", func(t *testing.T) {
		sessionUsername := username.TestUserName()
		userClient, _, err := ts.GetAuthenticatedHTTPClientAndCookie(sessionUsername, false, 1)
		require.NoError(t, err)

		// Assert that the test user gets an empty response for db 1
		uri1 := fmt.Sprintf("/api/v2/table_metadata/?dbId=%d", db1Id)
		mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri1).String())

		require.Empty(t, mdResp.Results)
		require.Zero(t, mdResp.PaginationInfo.TotalResults)

		// Assert that the test user gets an empty response for db 2
		uri2 := fmt.Sprintf("/api/v2/table_metadata/?dbId=%d", db2Id)
		mdResp = makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri2).String())

		require.Empty(t, mdResp.Results)
		require.Zero(t, mdResp.PaginationInfo.TotalResults)

		// Grant connect access to DB 1
		_, e := conn.Exec(fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", db1Name, sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri1).String())

		// Assert that user now see results for db1
		require.NotEmpty(t, mdResp.Results)
		require.True(t, slices.IsSortedFunc(mdResp.Results, defaultTMComparator))
		// Assert that the test user gets an empty response for db 2
		mdResp = makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri2).String())

		require.Empty(t, mdResp.Results)
		require.Zero(t, mdResp.PaginationInfo.TotalResults)

		// Revoke connect access from db1
		_, e = conn.Exec(fmt.Sprintf("REVOKE CONNECT ON DATABASE %s FROM %s", db1Name, sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri1).String())

		// Assert that user no longer sees results from db1
		require.Empty(t, mdResp.Results)

		// Make user admin
		// Revoke connect access from db1
		_, e = conn.Exec(fmt.Sprintf("GRANT admin TO %s", sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri1).String())

		// Assert that user now see results for db1
		require.NotEmpty(t, mdResp.Results)
		require.True(t, slices.IsSortedFunc(mdResp.Results, defaultTMComparator))
		// Assert that user now see results for db1
		mdResp = makeApiRequest[PaginatedResponse[[]tableMetadata]](t, userClient, ts.AdminURL().WithPath(uri2).String())

		require.NotEmpty(t, mdResp.Results)
		require.True(t, slices.IsSortedFunc(mdResp.Results, defaultTMComparator))
	})
	t.Run("sorting", func(t *testing.T) {
		nameComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				cmp.Compare(first.SchemaName, second.SchemaName),
				cmp.Compare(first.TableName, second.TableName),
				defaultTMComparator(first, second),
			)
		}
		replicationSizeComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				cmp.Compare(first.ReplicationSizeBytes, second.ReplicationSizeBytes),
				defaultTMComparator(first, second),
			)
		}
		rangeComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				cmp.Compare(first.RangeCount, second.RangeCount),
				defaultTMComparator(first, second),
			)
		}

		liveDataComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				cmp.Compare(first.PercentLiveData, second.PercentLiveData),
				defaultTMComparator(first, second),
			)
		}

		columnCountComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				cmp.Compare(first.ColumnCount, second.ColumnCount),
				defaultTMComparator(first, second),
			)
		}
		indexCountComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				cmp.Compare(first.IndexCount, second.IndexCount),
				defaultTMComparator(first, second),
			)
		}

		lastUpdatedComparator := func(first, second tableMetadata) int {
			return cmp.Or(
				first.LastUpdated.Compare(second.LastUpdated),
				defaultTMComparator(first, second),
			)
		}

		var sortTests = []struct {
			name        string
			dbId        int
			queryString string
			comparator  func(first, second tableMetadata) int
		}{
			{"no sort", db1Id, "?", defaultTMComparator},
			{"no sort db_id 2", db2Id, "?", defaultTMComparator},
			{"empty sort", db1Id, "?sortBy=", defaultTMComparator},
			{"not support arg", db1Id, "?sortBy=asdfas", defaultTMComparator},
			{"empty query string and set sort order", db1Id, "?sortOrder=desc", defaultTMComparator},
			{"sort by name", db1Id, "?sortBy=name", nameComparator},
			{"sort by replication size", db1Id, "?sortBy=replicationSize", replicationSizeComparator},
			{"sort by ranges", db1Id, "?sortBy=ranges", rangeComparator},
			{"sort by percentage live data", db1Id, "?sortBy=liveData", liveDataComparator},
			{"sort by total columns", db1Id, "?sortBy=columns", columnCountComparator},
			{"sort by total indexes", db1Id, "?sortBy=indexes", indexCountComparator},
			{"sort by total lastUpdated", db1Id, "?sortBy=lastUpdated", lastUpdatedComparator},
			{"sort by name descending", db1Id, "?sortBy=name&sortOrder=desc", descendingComparator(nameComparator)},
			{"sort by replication size descending", db1Id, "?sortBy=replicationSize&sortOrder=desc", descendingComparator(replicationSizeComparator)},
			{"sort by ranges descending", db1Id, "?sortBy=ranges&sortOrder=desc", descendingComparator(rangeComparator)},
			{"sort by percentage live data descending", db1Id, "?sortBy=liveData&sortOrder=desc", descendingComparator(liveDataComparator)},
			{"sort by total columns descending", db1Id, "?sortBy=columns&sortOrder=desc", descendingComparator(columnCountComparator)},
			{"sort by total indexes descending", db1Id, "?sortBy=indexes&sortOrder=desc", descendingComparator(indexCountComparator)},
			{"sort by total lastUpdated descending", db1Id, "?sortBy=lastUpdated&sortOrder=desc", descendingComparator(lastUpdatedComparator)},
		}

		for _, tt := range sortTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/table_metadata/%s&dbId=%d", tt.queryString, tt.dbId)
				mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
				require.NotEmpty(t, mdResp.Results)
				require.True(t, slices.IsSortedFunc(mdResp.Results, tt.comparator))
				for _, tbmd := range mdResp.Results {
					require.EqualValues(t, tt.dbId, tbmd.DbId)
				}
			})
		}
	})
	t.Run("table name filter", func(t *testing.T) {
		var tableNameTests = []struct {
			name          string
			dbId          int
			nameFilter    string
			expectedCount int
		}{
			// matches tables: mySchema1.MyTable1 and mySchema2.MyTable1
			{"with table name", db1Id, "MyTable1", 2},
			// matches tables: mySchema1.MyTable1 and mySchema2.MyTable1
			{"with table name lower case", db1Id, "mytable1", 2},
			// matches tables: mySchema1.MyTable1 and mySchema2.MyTable1
			{"with partial table name", db1Id, "ble1", 2},
			// matches tables: mySchema1.MyTable1
			{"with schema and table name", db1Id, "myschema1.mytable1", 1},
			// matches tables: mySchema1.MyTable1 and mySchema2.MyTable1
			{"with partial schema and table name", db1Id, "schem.ble1", 2},
		}

		for _, tt := range tableNameTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/table_metadata/?dbId=%d&name=%s", tt.dbId, tt.nameFilter)
				mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, client, ts.AdminURL().WithPath(uri).String())

				require.Equal(t, int64(tt.expectedCount), mdResp.PaginationInfo.TotalResults)
			})
		}
	})
	t.Run("pagination", func(t *testing.T) {
		var pageTests = []struct {
			name             string
			queryString      string
			expectedPageNum  int
			expectedPageSize int
		}{
			{"no page size or page num", "?", defaultPageNum, defaultPageSize},
			{"set page size", "?pageSize=11", defaultPageNum, 11},
			{"set page size and page num", "?pageSize=2&pageNum=2", 2, 2},
			{"invalid page size and num", "?pageSize=0&pageNum=0", defaultPageNum, defaultPageSize},
		}
		for _, tt := range pageTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/table_metadata/%s&dbId=%d", tt.queryString, db1Id)
				mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
				require.NotEmpty(t, mdResp.Results)
				require.LessOrEqual(t, len(mdResp.Results), tt.expectedPageSize)
				require.Equal(t, tt.expectedPageSize, mdResp.PaginationInfo.PageSize)
				require.Equal(t, tt.expectedPageNum, mdResp.PaginationInfo.PageNum)
			})
		}

		t.Run("large page num", func(t *testing.T) {
			uri := fmt.Sprintf("/api/v2/table_metadata/?dbId=%d&pageSize=1&pageNum=100", db1Id)
			mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
			require.Empty(t, mdResp.Results)
		})
	})
	t.Run("filter store id", func(t *testing.T) {
		storeIds := []int64{1, 2}
		uri := fmt.Sprintf("/api/v2/table_metadata/?dbId=%d&storeId=%d&storeId=%d", db1Id, storeIds[0], storeIds[1])
		mdResp := makeApiRequest[PaginatedResponse[[]tableMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
		require.NotEmpty(t, mdResp.Results)
		for _, tmdr := range mdResp.Results {
			require.Condition(t, func() (success bool) {
				return slices.Contains(tmdr.StoreIds, storeIds[0]) || slices.Contains(tmdr.StoreIds, storeIds[1])
			})
		}
	})
	t.Run("422 unprocessable", func(t *testing.T) {
		var unprocessableTest = []struct {
			name        string
			queryString string
		}{
			{"dbId", "?dbId=a"},
			{"pageNum", "?pageNum=a"},
			{"pageSize", "?pageSize=a"},
			{"storeId", "?storeId=a"},
			{"multiple storeIds", "?storeId=1&storeId=a"},
		}
		for _, tt := range unprocessableTest {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/table_metadata/%s", tt.queryString)
				req, err := http.NewRequest("GET", ts.AdminURL().WithPath(uri).String(), nil)
				require.NoError(t, err)
				resp, err := client.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
			})
		}
	})
}

func TestGetDBMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	conn := testCluster.ServerConn(0)
	defer conn.Close()
	var (
		db1Name = "new_test_db_1"
		db2Name = "new_test_db_2"
	)
	db1Id, _ := setupTest(t, conn, db1Name, db2Name)

	ts := testCluster.Server(0)
	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)
	t.Run("non GET method 405 error", func(t *testing.T) {})
	t.Run("sorting", func(t *testing.T) {

		nameComparator := func(first, second dbMetadata) int {
			return cmp.Or(cmp.Compare(first.DbName, second.DbName), defaultDMComparator(first, second))
		}
		sizeComparator := func(first, second dbMetadata) int {
			return cmp.Or(cmp.Compare(first.SizeBytes, second.SizeBytes), defaultDMComparator(first, second))
		}
		tableCountComparator := func(first, second dbMetadata) int {
			return cmp.Or(cmp.Compare(first.TableCount, second.TableCount), defaultDMComparator(first, second))
		}
		lastUpdatedComparator := func(first, second dbMetadata) int {
			return cmp.Or(first.LastUpdated.Compare(second.LastUpdated), defaultDMComparator(first, second))
		}

		var sortTests = []struct {
			name        string
			queryString string
			comparator  func(first, second dbMetadata) int
		}{
			{"no sort", "", defaultDMComparator},
			{"empty sort", "?sortBy=", defaultDMComparator},
			{"non-sortable param", "?sortBy=asdfas", defaultDMComparator},
			{"empty query string and set sort order", "?sortOrder=desc", defaultDMComparator},
			{"sort by size", "?sortBy=name", nameComparator},
			{"sort by size", "?sortBy=size", sizeComparator},
			{"sort by table count", "?sortBy=tableCount", tableCountComparator},
			{"sort by lastUpdated", "?sortBy=lastUpdated", lastUpdatedComparator},
			{"sort by name descending", "?sortBy=name&sortOrder=desc", descendingComparator(nameComparator)},
			{"sort by size descending", "?sortBy=size&sortOrder=desc", descendingComparator(sizeComparator)},
			{"sort by table count descending", "?sortBy=tableCount&sortOrder=desc", descendingComparator(tableCountComparator)},
			{"sort by last updated descending", "?sortBy=lastUpdated&sortOrder=desc", descendingComparator(lastUpdatedComparator)},
		}
		for _, tt := range sortTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/database_metadata/%s", tt.queryString)
				mdResp := makeApiRequest[PaginatedResponse[[]dbMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
				require.NotEmpty(t, mdResp.Results)
				isSorted := slices.IsSortedFunc(mdResp.Results, tt.comparator)
				require.True(t, isSorted)
			})
		}
	})

	t.Run("authorization", func(t *testing.T) {
		sessionUsername := username.TestUserName()
		userClient, _, err := ts.GetAuthenticatedHTTPClientAndCookie(sessionUsername, false, 1)
		require.NoError(t, err)

		// Assert that the test user gets an empty response for db 1
		uri := "/api/v2/database_metadata/"
		mdResp := makeApiRequest[PaginatedResponse[[]dbMetadata]](t, userClient, ts.AdminURL().WithPath(uri).String())

		require.Empty(t, mdResp.Results)
		require.Zero(t, mdResp.PaginationInfo.TotalResults)

		// Grant connect access to DB 1
		_, e := conn.Exec(fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", db1Name, sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[PaginatedResponse[[]dbMetadata]](t, userClient, ts.AdminURL().WithPath(uri).String())

		// Assert that user now see results for db1
		require.Len(t, mdResp.Results, 1)
		require.True(t, mdResp.Results[0].DbId == int64(db1Id))
		require.True(t, slices.IsSortedFunc(mdResp.Results, defaultDMComparator))

		// Revoke connect access from db1
		_, e = conn.Exec(fmt.Sprintf("REVOKE CONNECT ON DATABASE %s FROM %s", db1Name, sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[PaginatedResponse[[]dbMetadata]](t, userClient, ts.AdminURL().WithPath(uri).String())

		// Assert that user no longer sees results from db1
		require.Empty(t, mdResp.Results)

		// Make user admin
		_, e = conn.Exec(fmt.Sprintf("GRANT admin TO %s", sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[PaginatedResponse[[]dbMetadata]](t, userClient, ts.AdminURL().WithPath(uri).String())

		// Assert that user now see results for all dbs
		require.Len(t, mdResp.Results, 2)
		require.True(t, slices.IsSortedFunc(mdResp.Results, defaultDMComparator))

	})
	t.Run("pagination", func(t *testing.T) {
		var pageTests = []struct {
			name             string
			queryString      string
			expectedPageNum  int
			expectedPageSize int
		}{
			{"no page size or page num", "?", defaultPageNum, defaultPageSize},
			{"set page size", "?pageSize=1", defaultPageNum, 1},
			{"set page size and page num", "?pageSize=1&pageNum=2", 2, 1},
			{"invalid page size and num", "?pageSize=0&pageNum=0", defaultPageNum, defaultPageSize},
		}
		for _, tt := range pageTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/database_metadata/%s", tt.queryString)
				mdResp := makeApiRequest[PaginatedResponse[[]dbMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
				require.NotEmpty(t, mdResp.Results)
				require.LessOrEqual(t, len(mdResp.Results), tt.expectedPageSize)
				require.Equal(t, tt.expectedPageSize, mdResp.PaginationInfo.PageSize)
				require.Equal(t, tt.expectedPageNum, mdResp.PaginationInfo.PageNum)
			})
		}
	})
	t.Run("db name filter", func(t *testing.T) {
		var dbtableNameTests = []struct {
			name          string
			nameFilter    string
			expectedCount int
		}{
			// matches database: new_test_db_1
			{"with db name", db1Name, 1},
			// matches database new_test_db_1
			{"with db name non-matching case", strings.ToUpper(db1Name), 1},
			// matches database new_test_db_1, new_test_db2
			{"with partial database name", db1Name[0:4], 2},
		}

		for _, tt := range dbtableNameTests {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/database_metadata/?name=%s", tt.nameFilter)
				mdResp := makeApiRequest[PaginatedResponse[[]dbMetadata]](t, client, ts.AdminURL().WithPath(uri).String())

				require.Equal(t, int64(tt.expectedCount), mdResp.PaginationInfo.TotalResults)
			})
		}
	})
	t.Run("filter store id", func(t *testing.T) {
		storeIds := []int64{8, 9}
		uri := fmt.Sprintf("/api/v2/database_metadata/?storeId=%d&storeId=%d", storeIds[0], storeIds[1])
		mdResp := makeApiRequest[PaginatedResponse[[]dbMetadata]](t, client, ts.AdminURL().WithPath(uri).String())
		for _, dmdr := range mdResp.Results {
			require.Condition(t, func() (success bool) {
				return slices.Contains(dmdr.StoreIds, storeIds[0]) || slices.Contains(dmdr.StoreIds, storeIds[1])
			})
		}
	})
	t.Run("422 unprocessable", func(t *testing.T) {
		var unprocessableTest = []struct {
			name        string
			queryString string
		}{
			{"pageNum", "?pageNum=a"},
			{"pageSize", "?pageSize=a"},
			{"storeId", "?storeId=a"},
			{"multiple storeIds", "?storeId=1&storeId=a"},
		}
		for _, tt := range unprocessableTest {
			t.Run(tt.name, func(t *testing.T) {
				uri := fmt.Sprintf("/api/v2/database_metadata/%s", tt.queryString)
				req, err := http.NewRequest("GET", ts.AdminURL().WithPath(uri).String(), nil)
				require.NoError(t, err)
				resp, err := client.Do(req)
				require.NoError(t, err)
				defer resp.Body.Close()
				require.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
			})
		}
	})
}

func TestGetTableMetadataUpdateJobStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	conn := testCluster.ServerConn(0)
	defer conn.Close()

	ts := testCluster.Server(0)

	t.Run("authorized", func(t *testing.T) {
		uri := "/api/v2/table_metadata/updatejob/"
		sessionUsername := username.TestUserName()
		userClient, _, err := ts.GetAuthenticatedHTTPClientAndCookie(sessionUsername, false, 1)
		require.NoError(t, err)

		failed := makeApiRequest[interface{}](t, userClient, ts.AdminURL().WithPath(uri).String())
		require.Equal(t, http.StatusText(http.StatusNotFound), failed)

		_, e := conn.Exec(fmt.Sprintf("GRANT CONNECT ON DATABASE defaultdb TO %s", sessionUsername.Normalized()))
		require.NoError(t, e)
		_, e = conn.Exec(fmt.Sprintf("GRANT SYSTEM viewjob TO %s", sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp := makeApiRequest[tmUpdateJobStatusResponse](t, userClient, ts.AdminURL().WithPath(uri).String())
		require.Equal(t, "running", mdResp.CurrentState)

		_, e = conn.Exec(fmt.Sprintf("REVOKE CONNECT ON DATABASE defaultdb FROM %s", sessionUsername.Normalized()))
		require.NoError(t, e)
		failed = makeApiRequest[string](t, userClient, ts.AdminURL().WithPath(uri).String())
		require.Equal(t, http.StatusText(http.StatusNotFound), failed)

		_, e = conn.Exec(fmt.Sprintf("GRANT admin TO %s", sessionUsername.Normalized()))
		require.NoError(t, e)
		mdResp = makeApiRequest[tmUpdateJobStatusResponse](t, userClient, ts.AdminURL().WithPath(uri).String())
		require.Equal(t, "running", mdResp.CurrentState)
	})
}

func makeApiRequest[T any](t *testing.T, client http.Client, uri string) (mdResp T) {
	req, err := http.NewRequest("GET", uri, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
	contentType := resp.Header.Get("Content-type")
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if strings.Contains(contentType, "text/plain") {
		data = []byte(fmt.Sprintf(`"%s"`, strings.TrimSpace(string(data))))
	}
	require.NoError(t, json.Unmarshal(data, &mdResp))
	return mdResp
}

func setupTest(t *testing.T, conn *gosql.DB, db1 string, db2 string) (dbId1 int, dbId2 int) {
	_, err := conn.Exec(`CREATE DATABASE IF NOT EXISTS ` + db1)
	require.NoError(t, err)

	_, err = conn.Exec(`CREATE DATABASE IF NOT EXISTS ` + db2)
	require.NoError(t, err)
	result, err := conn.Query(fmt.Sprintf(`SELECT crdb_internal.get_database_id('%s') AS database_id;`, db1))
	require.NoError(t, err)
	if result.Next() {
		err = result.Scan(&dbId1)
		require.NoError(t, err)
	} else {
		t.Fail()
	}

	result, err = conn.Query(fmt.Sprintf(`SELECT crdb_internal.get_database_id('%s') AS database_id;`, db2))
	require.NoError(t, err)
	if result.Next() {
		err = result.Scan(&dbId2)
		require.NoError(t, err)
	} else {
		t.Fail()
	}
	_, err = conn.Exec(fmt.Sprintf(`
		INSERT INTO system.table_metadata
			(db_id,
			db_name,
			table_id,
		 	schema_name,
			table_name,
			replication_size_bytes,
			total_ranges,
			total_live_data_bytes,
			total_data_bytes,
			perc_live_data,
			total_columns,
			total_indexes,
			store_ids,
			last_update_error,
			last_updated)
		VALUES
		(%[1]d, '%[3]s', 1, 'mySchema1', 'myTable1', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:00Z'),
		(%[1]d, '%[3]s', 2, 'mySchema1', 'myTable2', 10002, 18, 519, 1000, .519, 16, 5, ARRAY[1, 5, 6], null, '2025-06-20T00:00:01Z'),
		(%[1]d, '%[3]s', 3, 'mySchema1', 'myTable3', 10003, 17, 510, 1000, .510, 17, 5, ARRAY[1, 8, 9], null, '2025-06-20T00:00:02Z'),
		(%[1]d, '%[3]s', 4, 'mySchema1', 'myTable4', 10004, 16, 520, 1000, .52, 18, 5, ARRAY[2, 3], null, '2025-06-20T00:00:03Z'),
		(%[1]d, '%[3]s', 5, 'mySchema1', 'myTable5', 10005, 15, 511, 1000, .511, 13, 5, ARRAY[5, 2], null, '2025-06-20T00:00:04Z'),
		(%[1]d, '%[3]s', 6, 'mySchema2', 'myTable6', 10006, 14, 522, 1000, .522, 19, 2, ARRAY[7], null, '2025-06-20T00:00:05Z'),
		(%[1]d, '%[3]s', 7, 'mySchema2', 'myTable7', 10007, 13, 512, 1000, .512, 14, 5, ARRAY[9], null, '2025-06-20T00:00:06Z'),
		(%[1]d, '%[3]s', 8, 'mySchema2', 'myTable8', 10008, 12, 523, 1000, .523, 20, 5, ARRAY[3], null, '2025-06-20T00:00:07Z'),
		(%[1]d, '%[3]s', 11, 'mySchema2', 'myTable9', 10009, 11, 513, 1000, .513, 15, 3, ARRAY[2], null, '2025-06-20T00:00:08Z'),
		(%[1]d, '%[3]s', 10, 'mySchema2', 'myTable10', 10001, 10, 523, 1000, .523, 10, 5, ARRAY[1], null, '2025-06-20T00:00:09Z'),
		(%[2]d, '%[4]s', 9, 'mySchema', 'myTable11', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:10Z'),
		(%[2]d, '%[4]s', 12, 'mySchema', 'myTable12', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:11Z'),
		(%[2]d, '%[4]s', 13, 'mySchema', 'myTable13', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], 'some error', '2025-06-20T00:00:12Z')
`, dbId1, dbId2, db1, db2))
	require.NoError(t, err)

	return dbId1, dbId2
}
