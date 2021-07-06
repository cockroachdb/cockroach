// Copyright 2021 The Cockroach Authors.
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
	"context"
	gosql "database/sql"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestListSessionsV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	var sqlConns []*gosql.Conn
	for i := 0; i < 15; i++ {
		serverConn := testCluster.ServerConn(i % 3)
		conn, err := serverConn.Conn(ctx)
		require.NoError(t, err)
		sqlConns = append(sqlConns, conn)
	}

	defer func() {
		for _, conn := range sqlConns {
			_ = conn.Close()
		}
	}()

	doSessionsRequest := func(client http.Client, limit int, start string) listSessionsResponse {
		req, err := http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"sessions/", nil)
		require.NoError(t, err)
		query := req.URL.Query()
		if limit > 0 {
			query.Add("limit", strconv.Itoa(limit))
		}
		if len(start) > 0 {
			query.Add("start", start)
		}
		req.URL.RawQuery = query.Encode()
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		bytesResponse, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		var sessionsResponse listSessionsResponse
		if resp.StatusCode != 200 {
			t.Fatal(string(bytesResponse))
		}
		require.NoError(t, json.Unmarshal(bytesResponse, &sessionsResponse))
		return sessionsResponse
	}

	time.Sleep(500 * time.Millisecond)
	adminClient, err := ts1.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)
	sessionsResponse := doSessionsRequest(adminClient, 0, "")
	require.LessOrEqual(t, 15, len(sessionsResponse.Sessions))
	require.Equal(t, 0, len(sessionsResponse.Errors))
	allSessions := sessionsResponse.Sessions
	sort.Slice(allSessions, func(i, j int) bool {
		return allSessions[i].Start.Before(allSessions[j].Start)
	})

	// Test the paginated version is identical to the non-paginated one.
	for limit := 1; limit <= 15; limit++ {
		var next string
		var paginatedSessions []serverpb.Session
		for {
			sessionsResponse := doSessionsRequest(adminClient, limit, next)
			paginatedSessions = append(paginatedSessions, sessionsResponse.Sessions...)
			next = sessionsResponse.Next
			require.LessOrEqual(t, len(sessionsResponse.Sessions), limit)
			if len(sessionsResponse.Sessions) < limit {
				break
			}
		}
		sort.Slice(paginatedSessions, func(i, j int) bool {
			return paginatedSessions[i].Start.Before(paginatedSessions[j].Start)
		})
		// Sometimes there can be a transient session that pops up in one of the two
		// calls. Exclude it by only comparing the first 15 sessions.
		require.Equal(t, paginatedSessions[:15], allSessions[:15])
	}

	// A non-admin user cannot see sessions at all.
	nonAdminClient, err := ts1.GetAuthenticatedHTTPClient(false)
	require.NoError(t, err)
	req, err := http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"sessions/", nil)
	require.NoError(t, err)
	resp, err := nonAdminClient.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	bytesResponse, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Contains(t, string(bytesResponse), "not allowed")
}

func TestHealthV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	client, err := ts1.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts1.AdminURL()+apiV2Path+"health/", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Check if the response was a 200.
	require.Equal(t, 200, resp.StatusCode)
	// Check if an unmarshal into the (empty) HealthResponse struct works.
	var hr serverpb.HealthResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&hr))
	require.NoError(t, resp.Body.Close())
}
