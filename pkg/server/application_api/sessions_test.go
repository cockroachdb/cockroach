// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package application_api_test

import (
	"context"
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srvtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestListSessionsSecurity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	s := srv.ApplicationLayer()

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
				err := srvtestutils.GetStatusJSONProtoWithAdminOption(s, tc.endpoint, &response, requestWithAdmin)
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
	client := s.GetStatusClient(t)

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

// TestListSessionsPrivileges tests that the VIEWACTIVITY and VIEWACTIVITYREDACTED privileges
// are respected when listing sessions, particularly for other users' sessions.
func TestListSessionsPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Skip under stress race as the sleep query might finish before the stress race can finish.
	skip.UnderRace(t, "list sessions privileges")

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.ApplicationLayer()

	endpoint := "sessions"
	appName := "test_sessions_privileges"
	user := apiconstants.TestingUserNameNoAdmin().Normalized()
	sleepQuery := "SELECT pg_sleep(3000)"
	sleepQueryRedacted := "SELECT pg_sleep(_)"

	serverSQL := sqlutils.MakeSQLRunner(sqlDB)
	serverSQL.Exec(t, fmt.Sprintf(`SET application_name = "%s"`, appName))
	queryCtx, cancel := context.WithCancel(context.Background())

	// Run a sleep query as root in another goroutine to make sure that root's session has one
	// active query while we list sessions. This sleep query will be cancelled at the end of the
	// test.
	var g errgroup.Group
	g.Go(func() error {
		_, err := serverSQL.DB.ExecContext(queryCtx, sleepQuery)
		if strings.Contains(err.Error(), "canceled") && strings.Contains(queryCtx.Err().Error(), "canceled") {
			// Both errors contain the "canceled" substring, this means the query was
			// canceled as expected.
			return nil
		}
		t.Errorf("unexpected error: %v", err)
		return err
	})

	// We test all combinations of VIEWACTIVITY and VIEWACTIVITYREDACTED. We could also start
	// by granting all privileges and then revoking them one by one, but we want to keep the
	// tests as isolated as possible. If a non-admin user has neither privilege, they should
	// not see root's session. If a non-admin user has VIEWACTIVITY, they should see root's
	// session with the full query. If a non-admin user has VIEWACTIVITYREDACTED, they should
	// see root's session with the redacted query. If a non-admin user has both privileges,
	// VIEWACTIVITYREDACTED should take precedence.
	testCases := []struct {
		grantViewActivity         bool
		grantViewActivityRedacted bool
		expectedQuery             string
	}{
		{false, false, ""},
		{false, true, sleepQueryRedacted},
		{true, false, sleepQuery},
		{true, true, sleepQueryRedacted},
	}

	// Filters sessions by appName.
	filterSessions := func(sessions []serverpb.Session) []serverpb.Session {
		var filteredSessions []serverpb.Session
		for _, s := range sessions {
			if s.ApplicationName == appName {
				filteredSessions = append(filteredSessions, s)
			}
		}
		return filteredSessions
	}

	for _, tc := range testCases {
		if tc.grantViewActivity {
			serverSQL.Exec(t, fmt.Sprintf(`GRANT SYSTEM VIEWACTIVITY TO %s`, user))
		}
		if tc.grantViewActivityRedacted {
			serverSQL.Exec(t, fmt.Sprintf(`GRANT SYSTEM VIEWACTIVITYREDACTED TO %s`, user))
		}

		var response serverpb.ListSessionsResponse
		err := srvtestutils.GetStatusJSONProtoWithAdminOption(ts, endpoint, &response, false)

		if err != nil {
			t.Errorf("unexpected failure listing sessions from %s; error: %v; response errors: %v",
				endpoint, err, response.Errors)
		}

		filteredSessions := filterSessions(response.Sessions)
		numberOfSessions := len(filteredSessions)

		// A non-admin user with no privileges should not see any other users' sessions.
		if !tc.grantViewActivity && !tc.grantViewActivityRedacted {
			if numberOfSessions != 0 {
				t.Errorf("expected 0 sessions, but got %d", numberOfSessions)
			}
			continue
		}

		// A non-admin user with at least one of the privileges should see other users' sessions.
		if numberOfSessions != 1 {
			t.Errorf("expected 1 session, but got %d", numberOfSessions)
		} else {
			session := filteredSessions[0]
			numberOfActiveQueries := len(session.ActiveQueries)
			if numberOfActiveQueries != 1 {
				t.Errorf("expected 1 active query, but got %d", numberOfActiveQueries)
			} else {
				activeQuery := session.ActiveQueries[0].Sql
				if activeQuery != tc.expectedQuery {
					t.Errorf("expected active query to be %s, but got %s", tc.expectedQuery, activeQuery)
				}
			}
		}

		// Only revoke the privilege if we granted it in this test case.
		if tc.grantViewActivity {
			serverSQL.Exec(t, fmt.Sprintf(`REVOKE SYSTEM VIEWACTIVITY FROM %s`, user))
		}
		if tc.grantViewActivityRedacted {
			serverSQL.Exec(t, fmt.Sprintf(`REVOKE SYSTEM VIEWACTIVITYREDACTED FROM %s`, user))
		}
	}

	// Cancel the query so that the test can finish.
	cancel()
	_ = g.Wait()
}

func TestStatusCancelSessionGatewayMetadataPropagation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	s0 := testCluster.Server(0).ApplicationLayer()
	s1 := testCluster.Server(1).ApplicationLayer()

	// Start a SQL session as admin on node 1.
	sql0 := sqlutils.MakeSQLRunner(s0.SQLConn(t))
	results := sql0.QueryStr(t, "SELECT session_id FROM [SHOW SESSIONS] LIMIT 1")
	sessionID, err := hex.DecodeString(results[0][0])
	require.NoError(t, err)

	// Attempt to cancel that SQL session as non-admin over HTTP on node 2.
	req := &serverpb.CancelSessionRequest{
		SessionID: sessionID,
	}
	resp := &serverpb.CancelSessionResponse{}
	err = srvtestutils.PostStatusJSONProtoWithAdminOption(s1, "cancel_session/1", req, resp, false)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "status: 403 Forbidden")
}

func TestStatusAPIListSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCluster := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	s0 := testCluster.Server(0).ApplicationLayer()
	serverSQL := sqlutils.MakeSQLRunner(s0.SQLConn(t))

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
	err := srvtestutils.GetStatusJSONProtoWithAdminOption(s0, "sessions", &resp, false)
	require.NoError(t, err)

	// Grant VIEWACTIVITYREDACTED.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITYREDACTED", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 1")
	err = srvtestutils.GetStatusJSONProtoWithAdminOption(s0, "sessions", &resp, false)
	require.NoError(t, err)
	session := getSessionWithTestAppName(&resp)
	require.Equal(t, session.LastActiveQuery, session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT _", session.LastActiveQueryNoConstants)

	// Grant VIEWACTIVITY, VIEWACTIVITYREDACTED should take precedence.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s VIEWACTIVITY", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 1, 1")
	err = srvtestutils.GetStatusJSONProtoWithAdminOption(s0, "sessions", &resp, false)
	require.NoError(t, err)
	session = getSessionWithTestAppName(&resp)
	require.Equal(t, appName, session.ApplicationName)
	require.Equal(t, session.LastActiveQuery, session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT _, _", session.LastActiveQueryNoConstants)

	// Remove VIEWACTIVITYREDCATED. User should now see full query.
	serverSQL.Exec(t, fmt.Sprintf("ALTER USER %s NOVIEWACTIVITYREDACTED", userNoAdmin.Normalized()))
	serverSQL.Exec(t, "SELECT 2")
	err = srvtestutils.GetStatusJSONProtoWithAdminOption(s0, "sessions", &resp, false)
	require.NoError(t, err)
	session = getSessionWithTestAppName(&resp)
	require.Equal(t, "SELECT _", session.LastActiveQueryNoConstants)
	require.Equal(t, "SELECT 2", session.LastActiveQuery)
}

func TestListClosedSessions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The active sessions might close before the stress race can finish.
	skip.UnderRace(t, "active sessions")

	ctx := context.Background()
	testCluster := serverutils.StartCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(ctx)

	server := testCluster.Server(0).ApplicationLayer()

	doSessionsRequest := func(username string) serverpb.ListSessionsResponse {
		var resp serverpb.ListSessionsResponse
		path := "/_status/sessions?username=" + username
		err := serverutils.GetJSONProto(server, path, &resp)
		require.NoError(t, err)
		return resp
	}

	getUserConn := func(t *testing.T, username string, server serverutils.ApplicationLayerInterface) *gosql.DB {
		return server.SQLConn(
			t, serverutils.UserPassword(username, "hunter2"), serverutils.ClientCerts(false),
		)
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
			targetDB := getUserConn(t, user, server)
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
				targetDB := getUserConn(t, user, server)
				defer targetDB.Close()
				defer wg.Done()
				sqlutils.MakeSQLRunner(targetDB).Exec(t, `SELECT pg_sleep(30)`)
			}(user)
		}
	}

	// Open 3 sessions for the user and leave them idle by running version().
	for _, user := range users {
		for i := 0; i < 3; i++ {
			targetDB := getUserConn(t, user, server)
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
