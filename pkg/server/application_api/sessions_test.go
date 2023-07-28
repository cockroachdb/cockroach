// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package application_api_test

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestListSessionsSecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ts := s.(*server.TestServer)
	defer ts.Stopper().Stop(context.Background())

	ctx := context.Background()

	for _, requestWithAdmin := range []bool{true, false} {
		t.Run(fmt.Sprintf("admin=%v", requestWithAdmin), func(t *testing.T) {
			myUser := apiconstants.TestingUserNameNoAdmin()
			expectedErrOnListingRootSessions := "does not have permission to view sessions from user"
			if requestWithAdmin {
				myUser = apiconstants.TestingUserName()
				expectedErrOnListingRootSessions = ""
			}

			// HTTP requests respect the authenticated username from the HTTP session.
			testCases := []struct {
				endpoint    string
				expectedErr string
			}{
				{"local_sessions", ""},
				{"sessions", ""},
				{fmt.Sprintf("local_sessions?username=%s", myUser.Normalized()), ""},
				{fmt.Sprintf("sessions?username=%s", myUser.Normalized()), ""},
				{"local_sessions?username=" + username.RootUser, expectedErrOnListingRootSessions},
				{"sessions?username=" + username.RootUser, expectedErrOnListingRootSessions},
			}
			for _, tc := range testCases {
				var response serverpb.ListSessionsResponse
				err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, tc.endpoint, &response, requestWithAdmin)
				if tc.expectedErr == "" {
					if err != nil || len(response.Errors) > 0 {
						t.Errorf("unexpected failure listing sessions from %s; error: %v; response errors: %v",
							tc.endpoint, err, response.Errors)
					}
				} else {
					respErr := "<no error>"
					if len(response.Errors) > 0 {
						respErr = response.Errors[0].Message
					}
					if !testutils.IsError(err, tc.expectedErr) &&
						!strings.Contains(respErr, tc.expectedErr) {
						t.Errorf("did not get expected error %q when listing sessions from %s: %v",
							tc.expectedErr, tc.endpoint, err)
					}
				}
			}
		})
	}

	// gRPC requests behave as root and thus are always allowed.
	rootConfig := testutils.NewTestBaseContext(username.RootUserName())
	rpcContext := srvtestutils.NewRPCTestContext(ctx, ts, rootConfig)
	url := ts.AdvRPCAddr()
	nodeID := ts.NodeID()
	conn, err := rpcContext.GRPCDialNode(url, nodeID, rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	client := serverpb.NewStatusClient(conn)

	for _, user := range []string{"", apiconstants.TestingUser, username.RootUser} {
		request := &serverpb.ListSessionsRequest{Username: user}
		if resp, err := client.ListLocalSessions(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing local sessions for %q; error: %v; response errors: %v",
				user, err, resp.Errors)
		}
		if resp, err := client.ListSessions(ctx, request); err != nil || len(resp.Errors) > 0 {
			t.Errorf("unexpected failure listing sessions for %q; error: %v; response errors: %v",
				user, err, resp.Errors)
		}
	}
}

func TestStatusCancelSessionGatewayMetadataPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	// Start a SQL session as admin on node 1.
	sql0 := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))
	results := sql0.QueryStr(t, "SELECT session_id FROM [SHOW SESSIONS] LIMIT 1")
	sessionID, err := hex.DecodeString(results[0][0])
	require.NoError(t, err)

	// Attempt to cancel that SQL session as non-admin over HTTP on node 2.
	req := &serverpb.CancelSessionRequest{
		SessionID: sessionID,
	}
	resp := &serverpb.CancelSessionResponse{}
	err = srvtestutils.PostStatusJSONProtoWithAdminOption(testCluster.Server(1), "cancel_session/1", req, resp, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "status: 403 Forbidden")
}

func TestStatusAPIListSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := tests.CreateTestServerParams()
	ctx := context.Background()
	testCluster := serverutils.StartNewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer testCluster.Stopper().Stop(ctx)

	serverProto := testCluster.Server(0)
	serverSQL := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	appName := "test_sessions_api"
	serverSQL.Exec(t, fmt.Sprintf(`SET application_name = "%s"`, appName))

	getSessionWithTestAppName := func(response *serverpb.ListSessionsResponse) *serverpb.Session {
		require.NotEmpty(t, response.Sessions)
		for _, s := range response.Sessions {
			if s.ApplicationName == appName {
				return &s
			}
		}
		t.Errorf("expected to find session with app name %s", appName)
		return nil
	}

	userNoAdmin := apiconstants.TestingUserNameNoAdmin()
	var resp serverpb.ListSessionsResponse
	// Non-admin without VIEWWACTIVITY or VIEWACTIVITYREDACTED should work and fetch user's own sessions.
	err := srvtestutils.GetStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)

	// Grant VIEWACTIVITYREDACTED.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 1")
	err = srvtestutils.GetStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)
	session := getSessionWithTestAppName(&resp)
	require.Equal(t, session.LastActiveQuery, session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT _", session.LastActiveQueryNoConstants)

	// Grant VIEWACTIVITY, VIEWACTIVITYREDACTED should take precedence.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 1, 1")
	err = srvtestutils.GetStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)
	session = getSessionWithTestAppName(&resp)
	require.Equal(t, appName, session.ApplicationName)
	require.Equal(t, session.LastActiveQuery, session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT _, _", session.LastActiveQueryNoConstants)

	// Remove VIEWACTIVITYREDCATED. User should now see full query.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITYREDACTED", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 2")
	err = srvtestutils.GetStatusJSONProtoWithAdminOption(serverProto, "sessions", &resp, false)
	require.NoError(t, err)
	session = getSessionWithTestAppName(&resp)
	require.Equal(t, "SELECT _", session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT 2", session.LastActiveQuery)
}

func TestListClosedSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The active sessions might close before the stress race can finish.
	skip.UnderStressRace(t, "active sessions")

	ctx := context.Background()
	serverParams, _ := tests.CreateTestServerParams()
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: serverParams,
	})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0)

	doSessionsRequest := func(username string) serverpb.ListSessionsResponse {
		var resp serverpb.ListSessionsResponse
		path := "/_status/sessions?username=" + username
		err := serverutils.GetJSONProto(server, path, &resp)
		require.NoError(t, err)
		return resp
	}

	getUserConn := func(t *testing.T, username string, server serverutils.TestServerInterface) *gosql.DB {
		pgURL := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(username, "hunter2"),
			Host:   server.AdvSQLAddr(),
		}
		db, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return db
	}

	// Create a test user.
	users := []string{"test_user_a", "test_user_b", "test_user_c"}
	conn := testCluster.ServerConn(0)
	_, err := conn.Exec(fmt.Sprintf(`
CREATE USER %s with password 'hunter2';
CREATE USER %s with password 'hunter2';
CREATE USER %s with password 'hunter2';
`, users[0], users[1], users[2]))
	require.NoError(t, err)

	var dbs []*gosql.DB

	// Open 10 sessions for the user and then close them.
	for _, user := range users {
		for i := 0; i < 10; i++ {
			targetDB := getUserConn(t, user, testCluster.Server(0))
			dbs = append(dbs, targetDB)
			sqlutils.MakeSQLRunner(targetDB).Exec(t, `SELECT version()`)
		}
	}

	for _, db := range dbs {
		err := db.Close()
		require.NoError(t, err)
	}

	var wg sync.WaitGroup

	// Open 5 sessions for the user and leave them open by running pg_sleep(30).
	for _, user := range users {
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(user string) {
				// Open a session for the target user.
				targetDB := getUserConn(t, user, testCluster.Server(0))
				defer targetDB.Close()
				defer wg.Done()
				sqlutils.MakeSQLRunner(targetDB).Exec(t, `SELECT pg_sleep(30)`)
			}(user)
		}
	}

	// Open 3 sessions for the user and leave them idle by running version().
	for _, user := range users {
		for i := 0; i < 3; i++ {
			targetDB := getUserConn(t, user, testCluster.Server(0))
			defer targetDB.Close()
			sqlutils.MakeSQLRunner(targetDB).Exec(t, `SELECT version()`)
		}
	}

	countSessionStatus := func(allSessions []serverpb.Session) (int, int, int) {
		var active, idle, closed int
		for _, s := range allSessions {
			if s.Status.String() == "ACTIVE" {
				active++
			}
			// IDLE sessions are open sessions with no active queries.
			if s.Status.String() == "IDLE" {
				idle++
			}
			if s.Status.String() == "CLOSED" {
				closed++
			}
		}
		return active, idle, closed
	}

	expectedIdle := 3
	expectedActive := 5
	expectedClosed := 10

	testutils.SucceedsSoon(t, func() error {
		for _, user := range users {
			sessionsResponse := doSessionsRequest(user)
			allSessions := sessionsResponse.Sessions
			sort.Slice(allSessions, func(i, j int) bool {
				return allSessions[i].Start.Before(allSessions[j].Start)
			})

			active, idle, closed := countSessionStatus(allSessions)
			if idle != expectedIdle {
				return errors.Newf("User: %s: Expected %d idle sessions, got %d\n", user, expectedIdle, idle)
			}
			if active != expectedActive {
				return errors.Newf("User: %s: Expected %d active sessions, got %d\n", user, expectedActive, active)
			}
			if closed != expectedClosed {
				return errors.Newf("User: %s: Expected %d closed sessions, got %d\n", user, expectedClosed, closed)
			}
		}
		return nil
	})

	// Wait for the goroutines from the pg_sleep() command to finish, so we can
	// safely close their connections.
	wg.Wait()
}
