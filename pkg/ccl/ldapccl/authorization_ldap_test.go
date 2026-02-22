// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestLDAPAuthorization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function
	mockLDAP, newMockLDAPUtil := LDAPMocks()
	defer testutils.TestingHook(
		&NewLDAPUtil,
		newMockLDAPUtil)()
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	manager := ConfigureLDAPAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())
	hbaEntryBase := "host all all all ldap "
	hbaConfLDAPDefaultOpts := map[string]string{
		"ldapserver": "localhost", "ldapport": "636", "ldapbasedn": "dc=localhost", "ldapbinddn": "cn=readonly,dc=localhost",
		"ldapbindpasswd": "readonly_pwd", "ldapgrouplistfilter": "(objectCategory=cn=Group,cn=Schema,cn=Configuration,DC=crlcloud,DC=dev)",
	}
	testCases := []struct {
		testName               string
		hbaConfLDAPOpts        map[string]string
		user                   string
		authZSuccess           bool
		ldapGroups             []string
		expectedErr            string
		expectedErrDetails     string
		expectedDetailedErrMsg string
	}{
		{testName: "proper hba conf and valid user cred",
			user: "cn=foo", authZSuccess: true, ldapGroups: []string{"cn=parent_role"}},
		{testName: "proper hba conf and invalid distinguished name",
			user: "cn=invalid", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when fetching groups for user dn ‹"cn=invalid"› in LDAP server: LDAP groups list failed: invalid user DN ‹"cn=invalid"› provided`},
		{testName: "invalid ldap option",
			hbaConfLDAPOpts: map[string]string{"invalidOpt": "invalidVal"}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to parse hba conf options",
			expectedDetailedErrMsg: `error parsing hba conf options for LDAP: invalid LDAP option provided in hba conf: ‹invalidOpt›`},
		{testName: "invalid server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": invalidParam}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap server provided"},
		{testName: "invalid port",
			hbaConfLDAPOpts: map[string]string{"ldapport": invalidParam}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap port provided"},
		{testName: "invalid base dn",
			hbaConfLDAPOpts: map[string]string{"ldapbasedn": invalidParam}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when fetching groups for user dn ‹"cn=foo"› in LDAP server: LDAP groups list failed: invalid base DN ‹"invalid"› provided`},
		{testName: "invalid bind dn",
			hbaConfLDAPOpts: map[string]string{"ldapbinddn": invalidParam}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: error binding as LDAP service user with configured credentials",
			expectedErrDetails:     "",
			expectedDetailedErrMsg: `error binding ldap service account: LDAP bind failed: invalid username provided`},
		{testName: "invalid bind pwd",
			hbaConfLDAPOpts: map[string]string{"ldapbindpasswd": invalidParam}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: error binding as LDAP service user with configured credentials",
			expectedErrDetails:     "",
			expectedDetailedErrMsg: `error binding ldap service account: LDAP bind failed: invalid password provided`},
		{testName: "invalid group list filter",
			hbaConfLDAPOpts: map[string]string{"ldapgrouplistfilter": invalidParam}, user: "cn=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when fetching groups for user dn ‹"cn=foo"› in LDAP server: LDAP groups list failed: invalid group list filter ‹"invalid"› provided`},
		{testName: "no matching ldap groups",
			user: "", authZSuccess: false, expectedErr: "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when fetching groups for user dn ‹""› in LDAP server: LDAP groups list failed: user dn ‹""› does not belong to any groups`},
		{testName: "more than 1 matching ldap groups",
			user: "o=foo,ou=foo2,cn=foo3", authZSuccess: true, ldapGroups: []string{"o=foo", "ou=foo2", "cn=foo3"}},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: testName:%v hbConfOpts:%v user:%v", i, tc.testName, tc.hbaConfLDAPOpts, tc.user), func(t *testing.T) {
			mockLDAP.SetGroups(tc.user, tc.ldapGroups)
			hbaEntry := constructHBAEntry(t, hbaEntryBase, hbaConfLDAPDefaultOpts, tc.hbaConfLDAPOpts)
			userDN, err := distinguishedname.ParseDN(tc.user)
			if err != nil {
				t.Fatalf("error parsing DN string for user DN %s: %v", tc.user, err)
			}

			retrievedLDAPGroups, detailedErrorMsg, err := manager.FetchLDAPGroups(
				ctx, s.ClusterSettings(), userDN, username.MakeSQLUsernameFromPreNormalizedString("foo"), &hbaEntry, nil)

			if (err == nil) != tc.authZSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.authZSuccess, err)
			}
			if err != nil {
				require.Equal(t, tc.expectedErr, err.Error())
				require.Equal(t, tc.expectedErrDetails, errors.FlattenDetails(err))
				require.Equal(t, redact.RedactableString(tc.expectedDetailedErrMsg), detailedErrorMsg)
			} else {
				require.Equal(t, len(tc.ldapGroups), len(retrievedLDAPGroups))
				for idx := range retrievedLDAPGroups {
					require.Equal(t, tc.ldapGroups[idx], retrievedLDAPGroups[idx].String())
				}
			}
		})
	}
}

func TestLDAPRolesAreGranted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function
	mockLDAP, newMockLDAPUtil := LDAPMocks()
	defer testutils.TestingHook(
		&NewLDAPUtil,
		newMockLDAPUtil)()
	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	hbaEntryBase := "host all all all ldap "
	hbaConfLDAPDefaultOpts := map[string]string{
		"ldapserver":          "localhost",
		"ldapport":            "636",
		"ldapbasedn":          "dc=localhost",
		"ldapbinddn":          "cn=readonly,dc=localhost",
		"ldapbindpasswd":      "readonly_pwd",
		"ldapsearchattribute": "uid",
		"ldapsearchfilter":    "(memberOf=cn=users,ou=groups,dc=localhost)",
		"ldapgrouplistfilter": "(objectCategory=cn=Group,cn=Schema,cn=Configuration,DC=example,DC=com)",
	}
	hbaEntry := constructHBAEntry(t, hbaEntryBase, hbaConfLDAPDefaultOpts, nil)
	_, err := db.Exec("SET CLUSTER SETTING server.auth_log.sql_connections.enabled = true")
	require.NoError(t, err)
	_, err = db.Exec("SET CLUSTER SETTING server.auth_log.sql_sessions.enabled = true")
	require.NoError(t, err)
	_, err = db.Exec("SET CLUSTER SETTING server.host_based_authentication.configuration = $1", hbaEntry.String())
	require.NoError(t, err)

	_, err = db.Exec("CREATE USER foo")
	require.NoError(t, err)
	_, err = db.Exec("CREATE ROLE foo_parent_1")
	require.NoError(t, err)
	_, err = db.Exec("CREATE ROLE foo_parent_2")
	require.NoError(t, err)

	var hasRole bool
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_1', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.False(t, hasRole)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_2', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.False(t, hasRole)

	connURL, cleanup := s.PGUrl(t)
	defer cleanup()
	connURL.User = url.UserPassword("foo", "readonly_pwd")

	fooDB, err := gosql.Open("postgres", connURL.String())
	require.NoError(t, err)
	defer fooDB.Close()

	// Add one parent role, connect, and check the parent roles.
	mockLDAP.SetGroups(mockLDAP.GetLdapDN("foo"), []string{"cn=foo_parent_1"})
	_, err = fooDB.Conn(ctx)
	require.NoError(t, err)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_1', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.True(t, hasRole)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_2', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.False(t, hasRole)

	// Add a new parent role, reconnect, and verify the role was added.
	mockLDAP.SetGroups(mockLDAP.GetLdapDN("foo"), []string{"cn=foo_parent_1", "cn=foo_parent_2"})
	_, err = fooDB.Conn(ctx)
	require.NoError(t, err)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_1', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.True(t, hasRole)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_2', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.True(t, hasRole)

	// Remove one of the parent role, reconnect, and verify the role was revoked.
	mockLDAP.SetGroups(mockLDAP.GetLdapDN("foo"), []string{"cn=foo_parent_2"})
	_, err = fooDB.Conn(ctx)
	require.NoError(t, err)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_1', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.False(t, hasRole)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_2', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.True(t, hasRole)

	// Verify that a group without a CN does not prevent login.
	mockLDAP.SetGroups("cn=foo", []string{"cn=foo_parent_1", "o=irrelevant_field"})
	_, err = fooDB.Conn(ctx)
	require.NoError(t, err)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_1', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.True(t, hasRole)
	err = db.QueryRow("SELECT pg_has_role('foo', 'foo_parent_2', 'MEMBER')").Scan(&hasRole)
	require.NoError(t, err)
	require.False(t, hasRole)

	// Sanity check to make sure the correct authentication_method was chosen
	// for the connections.
	var authMethod string
	var foundSession bool
	rows, err := db.Query("SELECT distinct(authentication_method) FROM [SHOW SESSIONS] WHERE user_name = 'foo'")
	require.NoError(t, err)
	for rows.Next() {
		foundSession = true
		require.NoError(t, rows.Scan(&authMethod))
		require.Equal(t, "ldap", authMethod)
	}
	require.True(t, foundSession)

	// Add a group that does not have a corresponding CRDB role, and verify that
	// the user can still login via partial groups mapping.
	mockLDAP.SetGroups("cn=foo", []string{"cn=foo_parent_2", "cn=nonexistent_role"})
	_, err = fooDB.Conn(ctx)
	require.NoError(t, err)
}

func TestLDAPAuthorizationDBConsole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function.
	mockLDAP, newMockLDAPUtil := LDAPMocks()
	defer testutils.TestingHook(
		&NewLDAPUtil,
		newMockLDAPUtil,
	)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)

	const hbaConf = `host all all all ldap ldapserver=localhost ldapport=636 ldapbasedn="dc=crdb,dc=io" ldapbinddn="cn=admin,dc=crdb,dc=io" ldapbindpasswd=admin ldapsearchattribute=cn "ldapsearchfilter=(objectClass=*)" "ldapgrouplistfilter=(objectClass=groupOfNames)"`
	tdb.Exec(t, "SET CLUSTER SETTING server.host_based_authentication.configuration = $1", hbaConf)

	// Create user and roles. Note that 'nonexistent_role' is NOT created.
	tdb.Exec(t, "CREATE USER alice; CREATE ROLE db_admins; GRANT admin TO db_admins;")

	// Helper function to get the current roles of a user.
	getRoles := func(user username.SQLUsername) []string {
		rows := tdb.QueryStr(t, `SELECT role FROM system.role_members WHERE member = $1 ORDER BY role`, user.Normalized())
		actualRoles := make([]string, len(rows))
		for i, r := range rows {
			actualRoles[i] = r[0]
		}
		return actualRoles
	}

	// Helper function to create a username.SQLUsername from a string.
	makeRole := func(name string) username.SQLUsername {
		r, err := username.MakeSQLUsernameFromUserInput(name, username.PurposeValidation)
		require.NoError(t, err)
		return r
	}

	// Test cases for DB Console LDAP login.
	testCases := []struct {
		name          string
		username      string
		password      string
		setupMock     func(mock *mockLDAPUtil)
		expectSuccess bool
		checkRoles    func(t *testing.T)
	}{
		{
			name:     "successful login with partial role grant",
			username: "alice",
			password: "password",
			setupMock: func(mock *mockLDAPUtil) {
				// The mock's Search function will automatically generate the correct DN for "alice".
				mock.SetGroups("cn=alice", []string{"cn=db_admins,ou=groups,dc=crdb,dc=io", "cn=nonexistent_role,ou=groups,dc=crdb,dc=io"})
			},
			expectSuccess: true,
			checkRoles: func(t *testing.T) {
				rows := tdb.QueryStr(t, `SELECT role FROM system.role_members WHERE member = 'alice'`)
				require.Len(t, rows, 1, "alice should only have one role")
				require.Equal(t, "db_admins", rows[0][0])
			},
		},
		{
			name:          "invalid username",
			username:      "invalid", // The mock is configured to fail search for this user.
			password:      "password",
			setupMock:     nil, // No specific setup needed for this case.
			expectSuccess: false,
		},
		{
			name:     "invalid password",
			username: "alice",
			password: "invalid", // The mock is configured to fail bind for this password.
			setupMock: func(mock *mockLDAPUtil) {
				// Ensure the user exists in the mock so that search succeeds.
				mock.SetGroups("cn=alice", []string{})
			},
			expectSuccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Reset user roles before each test run by revoking any existing roles.
			currentRoles := getRoles(makeRole("alice"))
			if len(currentRoles) > 0 {
				tdb.Exec(t, fmt.Sprintf("REVOKE %s FROM alice", strings.Join(currentRoles, ",")))
			}

			if tc.setupMock != nil {
				tc.setupMock(mockLDAP)
			}

			// Attempt to log in via the DB Console's HTTP endpoint.
			req := &serverpb.UserLoginRequest{
				Username: tc.username,
				Password: tc.password,
			}
			var resp serverpb.UserLoginResponse
			httpClient, err := ts.GetUnauthenticatedHTTPClient()
			require.NoError(t, err)
			err = httputil.PostJSON(httpClient, ts.AdminURL().WithPath(authserver.LoginPath).String(), req, &resp)

			if tc.expectSuccess {
				require.NoError(t, err, "DB Console login should succeed")
				if tc.checkRoles != nil {
					tc.checkRoles(t)
				}
			} else {
				require.Error(t, err, "DB Console login should fail")
				// The error from httputil will contain the status code for auth failures.
				require.Contains(t, err.Error(), "401 Unauthorized", "expected an authentication error")
			}
		})
	}

	// test the role revocation logic.
	t.Run("grant and revoke", func(t *testing.T) {
		// First, log in successfully to ensure 'alice' has the 'db_admins' role.
		mockLDAP.SetGroups("cn=alice", []string{"cn=db_admins"})
		req := &serverpb.UserLoginRequest{Username: "alice", Password: "password"}
		var resp serverpb.UserLoginResponse
		httpClient, err := ts.GetUnauthenticatedHTTPClient()
		require.NoError(t, err)
		err = httputil.PostJSON(httpClient, ts.AdminURL().WithPath(authserver.LoginPath).String(), req, &resp)
		require.NoError(t, err)

		// Verify the role was granted.
		require.Equal(t, []string{"db_admins"}, getRoles(username.MakeSQLUsernameFromPreNormalizedString("alice")))

		// Now, update the mock so that 'alice' is no longer in any groups and log in again.
		mockLDAP.SetGroups("cn=alice", []string{}) // Alice is now in zero groups.
		err = httputil.PostJSON(httpClient, ts.AdminURL().WithPath(authserver.LoginPath).String(), req, &resp)
		require.NoError(t, err)

		// Verify the role was successfully revoked.
		require.Empty(t, getRoles(username.MakeSQLUsernameFromPreNormalizedString("alice")), "alice should have no roles after revocation")
	})
}

func TestLDAPProvisioningDBConsole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function.
	mockLDAP, newMockLDAPUtil := LDAPMocks()
	defer testutils.TestingHook(
		&NewLDAPUtil,
		newMockLDAPUtil,
	)()

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)

	const hbaConf = `host all all all ldap ldapserver=localhost ldapport=636 ` +
		`ldapbasedn="dc=crdb,dc=io" ldapbinddn="cn=admin,dc=crdb,dc=io" ` +
		`ldapbindpasswd=admin ldapsearchattribute=cn ` +
		`"ldapsearchfilter=(objectClass=*)" ` +
		`"ldapgrouplistfilter=(objectClass=groupOfNames)"`
	tdb.Exec(t, "SET CLUSTER SETTING server.host_based_authentication.configuration = $1", hbaConf)
	tdb.Exec(t, "CREATE ROLE db_admins")

	httpLogin := func(user, password string) (*http.Response, error) {
		req := &serverpb.UserLoginRequest{
			Username: user,
			Password: password,
		}
		var resp serverpb.UserLoginResponse
		httpClient, err := ts.GetUnauthenticatedHTTPClient()
		if err != nil {
			return nil, err
		}
		return httputil.PostJSONWithRequest(
			httpClient,
			ts.AdminURL().WithPath(authserver.LoginPath).String(),
			req,
			&resp,
		)
	}

	t.Run("provisioning disabled, new user fails login", func(t *testing.T) {
		tdb.Exec(t, "SET CLUSTER SETTING security.provisioning.ldap.enabled = false")
		mockLDAP.SetGroups("cn=newuser1", []string{"cn=db_admins"})

		resp, err := httpLogin("newuser1", "password")
		require.Error(t, err)
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		// Verify the user was NOT created.
		rows := tdb.QueryStr(t,
			`SELECT username FROM system.users WHERE username = 'newuser1'`)
		require.Empty(t, rows,
			"user should not have been provisioned when setting is disabled")
	})

	t.Run("provisioning enabled, new user succeeds login", func(t *testing.T) {
		tdb.Exec(t, "SET CLUSTER SETTING security.provisioning.ldap.enabled = true")
		mockLDAP.SetGroups("cn=newuser2", []string{"cn=db_admins"})

		resp, err := httpLogin("newuser2", "password")
		require.NoError(t, err, "DB Console login should succeed with provisioning enabled")
		require.Equal(t, http.StatusOK, resp.StatusCode,
			"expected 200 OK for successful LDAP login")
		require.NotEmpty(t, resp.Cookies(),
			"expected session cookie in successful login response")

		// Expose internal tables for debugging
		tdb.Exec(t, "SET allow_unsafe_internals = true")

		// Verify the user was created.
		rows := tdb.QueryStr(t,
			`SELECT username FROM system.users WHERE username = 'newuser2'`)
		require.Len(t, rows, 1, "user should have been provisioned")

		// Verify PROVISIONSRC is set.
		rows = tdb.QueryStr(t,
			`SELECT options FROM [SHOW USERS] WHERE username = 'newuser2'`)
		require.Len(t, rows, 1)
		require.Contains(t, rows[0][0], "PROVISIONSRC=ldap:localhost")
	})

	t.Run("provisioning enabled, existing user", func(t *testing.T) {
		tdb.Exec(t, "SET CLUSTER SETTING security.provisioning.ldap.enabled = true")
		tdb.Exec(t, "CREATE USER existinguser")
		mockLDAP.SetGroups("cn=existinguser", []string{"cn=db_admins"})

		resp, err := httpLogin("existinguser", "password")
		require.NoError(t, err, "DB Console login should succeed for existing user")
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("provisioning enabled, invalid ldap credentials", func(t *testing.T) {
		tdb.Exec(t, "SET CLUSTER SETTING security.provisioning.ldap.enabled = true")
		mockLDAP.SetGroups("cn=badcreds", []string{"cn=db_admins"})

		// The mock rejects passwords containing "invalid".
		resp, err := httpLogin("badcreds", "invalid")
		require.Error(t, err)
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		// Verify the user was NOT created since LDAP auth failed.
		rows := tdb.QueryStr(t,
			`SELECT username FROM system.users WHERE username = 'badcreds'`)
		require.Empty(t, rows,
			"user should not have been provisioned with invalid LDAP credentials")
	})
}
