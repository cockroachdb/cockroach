// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	gosql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestListSessionsV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
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
		req, err := http.NewRequest("GET", ts1.AdminURL().WithPath(apiconstants.APIV2Path+"sessions/").String(), nil)
		require.NoError(t, err)
		query := req.URL.Query()
		query.Add("exclude_closed_sessions", "true")
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
		bytesResponse, err := io.ReadAll(resp.Body)
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
	adminClient, err := ts1.GetAdminHTTPClient()
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
	nonAdminClient, err := ts1.GetAuthenticatedHTTPClient(false, serverutils.SingleTenantSession)
	require.NoError(t, err)
	req, err := http.NewRequest("GET", ts1.AdminURL().WithPath(apiconstants.APIV2Path+"sessions/").String(), nil)
	require.NoError(t, err)
	resp, err := nonAdminClient.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	bytesResponse, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.Contains(t, string(bytesResponse), "not allowed")
}

func TestHealthV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	client, err := ts1.GetAdminHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts1.AdminURL().WithPath(apiconstants.APIV2Path+"health/").String(), nil)
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

func TestPlanDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// locality config to put 25 nodes in 5 localities
	locs := []string{"a", "b", "c", "d", "e"}
	const nodeCount = 25
	args := base.TestClusterArgs{}
	args.ServerArgsPerNode = make(map[int]base.TestServerArgs)
	for i := 0; i < nodeCount; i++ {
		args.ServerArgsPerNode[i] = base.TestServerArgs{
			Locality: roachpb.Locality{
				Tiers: []roachpb.Tier{
					{
						Key:   "region",
						Value: "test",
					},
					{
						Key:   "zone",
						Value: locs[i%len(locs)],
					},
				},
			},
		}
	}
	testCluster := serverutils.StartCluster(t, nodeCount, args)
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	client, err := ts1.GetAdminHTTPClient()
	require.NoError(t, err)

	const capacityTarget = 0.75
	doneNodes := map[roachpb.NodeID]struct{}{}
	moreBatches := true
	for moreBatches {
		url := ts1.AdminURL().WithPath(apiconstants.APIV2Path + "drain/plan/")
		query := url.Query()
		query.Add("capacityTarget", fmt.Sprintf("%f", capacityTarget))
		for nodeID := range doneNodes {
			query.Add("doneNodes", strconv.Itoa(int(nodeID)))
		}
		url.RawQuery = query.Encode()

		req, err := http.NewRequest("GET", url.String(), nil)
		require.NoError(t, err)
		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Check if the response was a 200.
		require.Equal(t, 200, resp.StatusCode)
		// Check if an unmarshal into the (empty) RestartPlanBatch struct works.
		var batchResult serverpb.RestartPlanBatch
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&batchResult))
		require.NoError(t, resp.Body.Close())

		// Check that the drain cohort is non-empty (which is expected here, since we're not actually downing any nodes)
		// NB: In a real client, an empty cohort should just cause you to sleep for like 30s and check again.
		require.Greater(t, len(batchResult.Batch), 0)
		// Check that we're not exceeding our disruption budget
		disruptionBudget := (1.0 - capacityTarget) * nodeCount
		require.LessOrEqual(t, len(batchResult.Batch), int(disruptionBudget))
		t.Logf("Cohort size %d", len(batchResult.Batch))

		// NB: In a real operations orchestrator, this is where you go drain and restart the nodes in
		// batchResult.Batch in parallel

		for _, toDrain := range batchResult.Batch {
			_, ok := doneNodes[toDrain.NodeID]
			// Check that we're not repeating any nodes that are already done
			require.False(t, ok, "repeated drain node %d", toDrain.NodeID)

			doneNodes[toDrain.NodeID] = struct{}{}
		}

		// Check the more flag
		if len(doneNodes) < nodeCount {
			require.True(t, batchResult.MoreBatches)
		} else {
			require.False(t, batchResult.MoreBatches)
		}

		moreBatches = batchResult.MoreBatches
	}
}

func TestRestartSafetyV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	client, err := ts1.GetAdminHTTPClient()
	require.NoError(t, err)

	urlStr := ts1.AdminURL().WithPath(apiconstants.APIV2Path + "health/restart_safety/").String()
	req, err := http.NewRequest("GET", urlStr, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 503, resp.StatusCode)
	// Check if an unmarshal into the DrainCheckResponse struct works.
	var response serverpb.DrainCheckResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.NoError(t, resp.Body.Close())

	stores, ok := ts1.GetStores().(*kvserver.Stores)
	require.True(t, ok)
	_ = stores.VisitStores(func(s *kvserver.Store) error {
		s.SetDraining(true, nil, false)
		return nil
	})

	req, err = http.NewRequest("GET", urlStr, nil)
	require.NoError(t, err)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	require.Equal(t, 200, resp.StatusCode)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.NoError(t, resp.Body.Close())

}

// TestRulesV2 tests the /api/v2/rules endpoint to ensure it
// returns valid YAML.
func TestRulesV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	ctx := context.Background()
	defer testCluster.Stopper().Stop(ctx)

	ts := testCluster.Server(0)
	client, err := ts.GetUnauthenticatedHTTPClient()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", ts.AdminURL().WithPath(apiconstants.APIV2Path+"rules/").String(), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Check if the response was a http.StatusOK.
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Check that the response was valid YAML.
	ruleGroups := make(map[string]metric.PrometheusRuleGroup)
	require.NoError(t, yaml.NewDecoder(resp.Body).Decode(&ruleGroups))
	require.NoError(t, resp.Body.Close())
}

func TestAuthV2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "insecure", func(t *testing.T, insecure bool) {
		testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Insecure: insecure,
			},
		})
		ctx := context.Background()
		defer testCluster.Stopper().Stop(ctx)

		ts := testCluster.Server(0)
		client, err := ts.GetUnauthenticatedHTTPClient()
		require.NoError(t, err)

		session, err := ts.GetAuthSession(true)
		require.NoError(t, err)
		sessionBytes, err := protoutil.Marshal(session)
		require.NoError(t, err)
		sessionEncoded := base64.StdEncoding.EncodeToString(sessionBytes)

		for _, tc := range []struct {
			name           string
			header         string
			cookie         string
			expectedStatus int
		}{
			{
				name:           "no auth",
				expectedStatus: http.StatusUnauthorized,
			},
			{
				name:           "session in header",
				header:         sessionEncoded,
				expectedStatus: http.StatusOK,
			},
			{
				name:           "cookie auth with correct magic header",
				cookie:         sessionEncoded,
				header:         authserver.APIV2UseCookieBasedAuth,
				expectedStatus: http.StatusOK,
			},
			{
				name:           "cookie auth but missing header",
				cookie:         sessionEncoded,
				expectedStatus: http.StatusUnauthorized,
			},
			{
				name:   "cookie auth but wrong magic header",
				cookie: sessionEncoded,
				header: "yes",
				// Bad Request and not Unauthorized because the session cannot be decoded.
				expectedStatus: http.StatusBadRequest,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				req, err := http.NewRequest("GET", ts.AdminURL().WithPath(apiconstants.APIV2Path+"sessions/").String(), nil)
				require.NoError(t, err)
				if tc.header != "" {
					req.Header.Set(authserver.APIV2AuthHeader, tc.header)
				}
				if tc.cookie != "" {
					req.AddCookie(&http.Cookie{
						Name:  authserver.SessionCookieName,
						Value: tc.cookie,
					})
				}
				resp, err := client.Do(req)
				require.NoError(t, err)
				require.NotNil(t, resp)
				defer resp.Body.Close()

				if !insecure && tc.expectedStatus != resp.StatusCode {
					body, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					t.Fatalf("expected status: %d but got: %d with body: %s", tc.expectedStatus, resp.StatusCode, string(body))
				}
				if insecure && http.StatusOK != resp.StatusCode {
					body, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					t.Fatalf("expected status: %d but got: %d with body: %s", http.StatusOK, resp.StatusCode, string(body))
				}
			})
		}

	})

}
