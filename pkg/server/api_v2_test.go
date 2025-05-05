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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
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

	require.Equal(
		t,
		http.StatusServiceUnavailable,
		resp.StatusCode,
		"expected service unavailable when node is not draining",
	)

	// Check if an unmarshal into the RestartSafetyResponse struct works.
	var response serverpb.RestartSafetyResponse
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

func Test_checkRestartSafe_Criticality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	ts1 := testCluster.Server(0)

	res, err := checkRestartSafe(ctx, ts1.NodeID(), ts1.NodeLiveness().(livenesspb.NodeVitalityInterface), ts1.GetStores().(storeVisitor), 3)
	require.NoError(t, err)
	require.False(t, res.IsRestartSafe)

	// Since we haven't drained, there will be some raft leaders, and others that are simple StoreNotDraining
	for _, rs := range res.Replicas {
		switch rs.Status {
		case "IsRaftLeader":
		case "StoreNotDraining":
		default:
			require.Fail(t, "unexpected status: %s", rs.String())
		}
	}

	err = drain(ctx, ts1, t)
	require.NoError(t, err)

	res, err = checkRestartSafe(ctx, ts1.NodeID(), ts1.NodeLiveness().(livenesspb.NodeVitalityInterface), ts1.GetStores().(storeVisitor), 3)
	// Now that we've drained, we're ok to restart
	require.NoError(t, err)
	require.True(t, res.IsRestartSafe)
}

func Test_checkRestartSafe_RangeStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var err error

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	ts0 := testCluster.Server(0)
	vitality := livenesspb.TestCreateNodeVitality(testCluster.NodeIDs()...)

	ts1nodeID := testCluster.Server(1).NodeID()
	vitality.DownNode(ts1nodeID)

	err = drain(ctx, ts0, t)
	require.NoError(t, err)
	vitality.Draining(ts0.NodeID(), true)

	require.True(t, vitality.GetNodeVitalityFromCache(ts0.NodeID()).IsDraining())
	require.False(t, vitality.GetNodeVitalityFromCache(ts1nodeID).IsLive(livenesspb.Metrics))

	res, err := checkRestartSafe(ctx, ts0.NodeID(), vitality, ts0.GetStores().(storeVisitor), 3)
	require.NoError(t, err)
	require.False(t, res.IsRestartSafe, "expected unsafe since a different node is down")

	foundRange := false
	for _, rs := range res.Replicas {
		if rs.Status == "Underreplicated" {
			foundRange = true
		}
	}
	require.True(t, foundRange)
}

func Test_checkRestartSafe_Integration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var err error

	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	ts0 := testCluster.Server(0)
	vitality := ts0.NodeLiveness().(livenesspb.NodeVitalityInterface)

	ts1nodeID := testCluster.Server(1).NodeID()
	testCluster.StopServer(1)

	testutils.SucceedsSoon(t, func() error {
		if vitality.GetNodeVitalityFromCache(ts1nodeID).IsLive(livenesspb.Metrics) {
			return fmt.Errorf("node is live")
		}
		return nil
	})

	err = drain(ctx, ts0, t)
	require.NoError(t, err)

	res, err := checkRestartSafe(ctx, ts0.NodeID(), vitality, ts0.GetStores().(storeVisitor), 3)
	require.NoError(t, err)
	require.False(t, res.IsRestartSafe, "expected unsafe since a different node is down")

	foundRange := false
	for _, rs := range res.Replicas {
		if rs.Status == "Underreplicated" {
			foundRange = true
		}
	}

	require.True(t, foundRange)
}

func drain(ctx context.Context, ts1 serverutils.TestServerInterface, t *testing.T) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err := ts1.DrainClients(ctx)
	require.NoError(t, err)

	for timeoutCtx.Err() == nil {
		drainStream, err := ts1.GetAdminClient(t).Drain(timeoutCtx, &serverpb.DrainRequest{
			Shutdown: false,
			DoDrain:  true,
			NodeId:   ts1.NodeID().String(),
			Verbose:  false,
		})
		require.NoError(t, err)
		drainRes, err := drainStream.Recv()
		require.NoError(t, err)
		require.True(t, drainRes.IsDraining)
		if drainRes.DrainRemainingIndicator == 0 {
			break
		}
	}

	return timeoutCtx.Err()
}
