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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

type testInput struct {
	Method string `json:"method,omitempty"`
	Uri    string `json:"uri,omitempty"`
	Body   string `json:"body,omitempty"`
}

type testOutput struct {
	StatusCode int         `json:"status_code"`
	Body       interface{} `json:"body,omitempty"`
}

func TestDbTablesMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	runTestDrivenApi(t, "testdata/api_databases_metadata/db_table_metadata", testCluster)
}

func runTestDrivenApi(t *testing.T, path string, tc serverutils.TestClusterInterface) {
	datadriven.RunTest(t, path,
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "sql":
				conn := tc.ServerConn(0)
				defer conn.Close()

				_, err := conn.Exec(d.Input)
				require.NoError(t, err)
			case "req":
				var input testInput
				err := json.Unmarshal([]byte(d.Input), &input)
				if err != nil {
					t.Fatalf("Could not marshall input")
				}
				var br io.Reader = nil
				if input.Body != "" {
					br = bytes.NewReader([]byte(input.Body))
				}
				ts := tc.Server(0)
				client, err := ts.GetAdminHTTPClient()
				req, err := http.NewRequest(input.Method, ts.AdminURL().WithPath(input.Uri).String(), br)
				require.NoError(t, err)
				resp, err := client.Do(req)
				defer resp.Body.Close()
				require.NoError(t, err)
				require.NotNil(t, resp)
				respStr, err := io.ReadAll(resp.Body)
				output := testOutput{
					StatusCode: resp.StatusCode,
				}
				if resp.Header.Get("Content-Type") == "application/json" {
					err = json.Unmarshal(respStr, &output.Body)
					require.NoError(t, err)
				} else {
					output.Body = string(respStr)
				}

				r, err := json.MarshalIndent(output, "", " ")
				require.NoError(t, err)
				return string(r)
			default:
				t.Fatalf("Invalid cmd")
			}

			return ""
		},
	)
}

func defaultComparator(first, second tableMetadata) int {
	return cmp.Compare(first.TableId, second.TableId)
}

func replicationSizeComparator(first, second tableMetadata) int {
	return cmp.Or(
		cmp.Compare(first.ReplicationSizeBytes, second.ReplicationSizeBytes),
		defaultComparator(first, second),
	)
}
func rangeComparator(first, second tableMetadata) int {
	return cmp.Or(
		cmp.Compare(first.RangeCount, second.RangeCount),
		defaultComparator(first, second),
	)
}

func liveDataComparator(first, second tableMetadata) int {
	return cmp.Or(
		cmp.Compare(first.PercentLiveData, second.PercentLiveData),
		defaultComparator(first, second),
	)
}

func columnCountComparator(first, second tableMetadata) int {
	return cmp.Or(
		cmp.Compare(first.ColumnCount, second.ColumnCount),
		defaultComparator(first, second),
	)
}
func indexCountComparator(first, second tableMetadata) int {
	return cmp.Or(
		cmp.Compare(first.IndexCount, second.IndexCount),
		defaultComparator(first, second),
	)
}

func lastUpdatedComparator(first, second tableMetadata) int {
	return cmp.Or(
		first.LastUpdated.Compare(second.LastUpdated),
		defaultComparator(first, second),
	)
}

func descendingComparator(
	comparator func(first, second tableMetadata) int,
) func(first, second tableMetadata) int {
	return func(f, s tableMetadata) int {
		return -1 * comparator(f, s)
	}
}

func Test_apiV2Server_DbTablesMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)
	setupTest(t, testCluster)
	ts := testCluster.Server(0)
	client, err := ts.GetAdminHTTPClient()
	require.NoError(t, err)

	var sortTests = []struct {
		name        string
		dbId        int
		queryString string
		comparator  func(first, second tableMetadata) int
	}{
		{"no query string", 1, "", defaultComparator},
		{"no query string db_id 2", 2, "", defaultComparator},
		{"empty query string", 1, "?", defaultComparator},
		{"empty query string and set sort order", 1, "?sortOrder=desc", defaultComparator},
		{"sort by replication size", 1, "?sortBy=replicationSize", replicationSizeComparator},
		{"sort by ranges", 1, "?sortBy=ranges", rangeComparator},
		{"sort by percentage live data", 1, "?sortBy=liveData", liveDataComparator},
		{"sort by total columns", 1, "?sortBy=columns", columnCountComparator},
		{"sort by total indexes", 1, "?sortBy=indexes", indexCountComparator},
		{"sort by total lastUpdated", 1, "?sortBy=lastUpdated", lastUpdatedComparator},
		{"sort by replication size descending", 1, "?sortBy=replicationSize&sortOrder=desc", descendingComparator(replicationSizeComparator)},
		{"sort by ranges descending", 1, "?sortBy=ranges&sortOrder=desc", descendingComparator(rangeComparator)},
		{"sort by percentage live data descending", 1, "?sortBy=liveData&sortOrder=desc", descendingComparator(liveDataComparator)},
		{"sort by total columns descending", 1, "?sortBy=columns&sortOrder=desc", descendingComparator(columnCountComparator)},
		{"sort by total indexes descending", 1, "?sortBy=indexes&sortOrder=desc", descendingComparator(indexCountComparator)},
		{"sort by total lastUpdated descending", 1, "?sortBy=lastUpdated&sortOrder=desc", descendingComparator(lastUpdatedComparator)},
	}

	for _, tt := range sortTests {
		t.Run(tt.name, func(t *testing.T) {
			uri := fmt.Sprintf("/api/v2/metadata/database/%d/tables/%s", tt.dbId, tt.queryString)
			mdResp := getTableMetadata(t, client, ts.AdminURL().WithPath(uri).String())
			require.NotEmpty(t, mdResp.Results)
			isSorted := slices.IsSortedFunc(mdResp.Results, tt.comparator)
			require.True(t, isSorted)
		})
	}

	t.Run("no tables for db", func(t *testing.T) {
		mdResp := getTableMetadata(t, client, ts.AdminURL().WithPath("/api/v2/metadata/database/10/tables/").String())
		require.Len(t, mdResp.Results, 0)
		require.Equal(t, mdResp.PaginationInfo.TotalResults, int64(0))
	})

	t.Run("non GET method 405 error", func(t *testing.T) {
		req, err := http.NewRequest("POST", ts.AdminURL().WithPath("/api/v2/metadata/database/10/tables/").String(), nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		defer resp.Body.Close()
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, 405, resp.StatusCode)
		respBytes, err := io.ReadAll(resp.Body)
		require.Contains(t, string(respBytes), "Method Not Allowed")
	})

	t.Run("filter table name", func(t *testing.T) {
		tableName := "Table1"
		uri := fmt.Sprintf("/api/v2/metadata/database/1/tables/?tableName=%s", tableName)
		mdResp := getTableMetadata(t, client, ts.AdminURL().WithPath(uri).String())
		for _, tmdr := range mdResp.Results {
			require.Contains(t, tmdr.TableName, tableName)
		}

		// Only 2 tables meet this table name requirement
		require.Equal(t, mdResp.PaginationInfo.TotalResults, int64(2))
	})

	t.Run("filter store id", func(t *testing.T) {
		storeIds := []int64{1, 2}
		uri := fmt.Sprintf("/api/v2/metadata/database/1/tables/?storeId=%d&storeId=%d", storeIds[0], storeIds[1])
		mdResp := getTableMetadata(t, client, ts.AdminURL().WithPath(uri).String())
		for _, tmdr := range mdResp.Results {
			require.Condition(t, func() (success bool) {
				return slices.Contains(tmdr.StoreIds, storeIds[0]) || slices.Contains(tmdr.StoreIds, storeIds[1])
			})
		}
	})

	var pageTests = []struct {
		name             string
		queryString      string
		expectedPageNum  int
		expectedPageSize int
	}{
		{"no page size or page num", "", defaultPageNum, defaultPageSize},
		{"set page size", "?pageSize=11", defaultPageNum, 11},
		{"set page size and page num", "?pageSize=2&pageNum=2", 2, 2},
		{"invalid page size and num", "?pageSize=0&pageNum=0", defaultPageNum, defaultPageSize},
	}
	for _, tt := range pageTests {
		t.Run(tt.name, func(t *testing.T) {
			uri := fmt.Sprintf("/api/v2/metadata/database/1/tables/%s", tt.queryString)
			mdResp := getTableMetadata(t, client, ts.AdminURL().WithPath(uri).String())
			require.NotEmpty(t, mdResp.Results)
			require.LessOrEqual(t, len(mdResp.Results), tt.expectedPageSize)
			require.Equal(t, mdResp.PaginationInfo.PageSize, tt.expectedPageSize)
			require.Equal(t, mdResp.PaginationInfo.PageNum, tt.expectedPageNum)
		})
	}

	t.Run("large page num", func(t *testing.T) {
		uri := "/api/v2/metadata/database/1/tables/?pageSize=1&pageNum=100"
		mdResp := getTableMetadata(t, client, ts.AdminURL().WithPath(uri).String())
		require.Empty(t, mdResp.Results)
	})
}

func getTableMetadata(
	t *testing.T, client http.Client, uri string,
) (mdResp dbTableMetadataResponse) {
	req, err := http.NewRequest("GET", uri, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	defer resp.Body.Close()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 200, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&mdResp))
	return mdResp
}
func setupTest(t *testing.T, tc serverutils.TestClusterInterface) {
	conn := tc.ServerConn(0)
	defer conn.Close()

	_, err := conn.Exec(`
		INSERT INTO system.table_metadata
			(db_id,
			db_name,
			table_id,
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
		(1, 'myDB1', 1, 'myTable1', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:00Z'),
		(1, 'myDB1', 2, 'myTable2', 10002, 18, 519, 1000, .519, 16, 5, ARRAY[1, 5, 6], null, '2025-06-20T00:00:01Z'),
		(1, 'myDB1', 3, 'myTable3', 10003, 17, 510, 1000, .510, 17, 5, ARRAY[1, 8, 9], null, '2025-06-20T00:00:02Z'),
		(1, 'myDB1', 4, 'myTable4', 10004, 16, 520, 1000, .52, 18, 5, ARRAY[2, 3], null, '2025-06-20T00:00:03Z'),
		(1, 'myDB1', 5, 'myTable5', 10005, 15, 511, 1000, .511, 13, 5, ARRAY[5, 2], null, '2025-06-20T00:00:04Z'),
		(1, 'myDB1', 6, 'myTable6', 10006, 14, 522, 1000, .522, 19, 2, ARRAY[7], null, '2025-06-20T00:00:05Z'),
		(1, 'myDB1', 7, 'myTable7', 10007, 13, 512, 1000, .512, 14, 5, ARRAY[9], null, '2025-06-20T00:00:06Z'),
		(1, 'myDB1', 8, 'myTable8', 10008, 12, 523, 1000, .523, 20, 5, ARRAY[3], null, '2025-06-20T00:00:07Z'),
		(1, 'myDB1', 9, 'myTable9', 10009, 11, 513, 1000, .513, 15, 3, ARRAY[2], null, '2025-06-20T00:00:08Z'),
		(1, 'myDB1', 10, 'myTable10', 10001, 10, 523, 1000, .523, 10, 5, ARRAY[1], null, '2025-06-20T00:00:09Z'),
		(2, 'myDB2', 11, 'myTable11', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:10Z'),
		(2, 'myDB2', 12, 'myTable12', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:11Z'),
		(2, 'myDB2', 13, 'myTable13', 10001, 19, 509, 1000, .509, 11, 1, ARRAY[1, 2, 3], null, '2025-06-20T00:00:12Z')
`)
	require.NoError(t, err)
}
