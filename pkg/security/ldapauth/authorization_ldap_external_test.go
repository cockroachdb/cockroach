// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapauth_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/ldapauth"
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
	"github.com/stretchr/testify/require"
)

func TestLDAPAuthorizationDBConsole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function.
	mockLDAP, newMockLDAPUtil := ldapauth.LDAPMocks()
	defer testutils.TestingHook(
		&ldapauth.NewLDAPUtil,
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
		setupMock     func()
		expectSuccess bool
		checkRoles    func(t *testing.T)
	}{
		{
			name:     "successful login with partial role grant",
			username: "alice",
			password: "password",
			setupMock: func() {
				// The mock's Search function will automatically generate the correct DN for "alice".
				mockLDAP.SetGroups("cn=alice", []string{"cn=db_admins,ou=groups,dc=crdb,dc=io", "cn=nonexistent_role,ou=groups,dc=crdb,dc=io"})
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
			setupMock: func() {
				// Ensure the user exists in the mock so that search succeeds.
				mockLDAP.SetGroups("cn=alice", []string{})
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
				tc.setupMock()
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
	mockLDAP, newMockLDAPUtil := ldapauth.LDAPMocks()
	defer testutils.TestingHook(
		&ldapauth.NewLDAPUtil,
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

	t.Run("provisioning disabled, existing user login does not set last login time", func(t *testing.T) {
		tdb.Exec(t, "SET CLUSTER SETTING security.provisioning.ldap.enabled = false")
		tdb.Exec(t, "CREATE USER existinguser2")
		mockLDAP.SetGroups("cn=existinguser2", []string{"cn=db_admins"})

		resp, err := httpLogin("existinguser2", "password")
		require.NoError(t, err, "DB Console login should succeed for existing user")
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// estimated_last_login_time should remain NULL since provisioning is
		// disabled.
		rows := tdb.QueryStr(t,
			`SELECT count(*) FROM system.users WHERE username = 'existinguser2' AND estimated_last_login_time IS NOT NULL`)
		require.Equal(t, "0", rows[0][0],
			"estimated_last_login_time should not be populated when provisioning is disabled")
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

		// Verify estimated_last_login_time is populated.
		testutils.SucceedsSoon(t, func() error {
			rows := tdb.QueryStr(t,
				`SELECT count(*) FROM system.users WHERE username = 'newuser2' AND estimated_last_login_time IS NOT NULL`)
			if len(rows) == 0 || rows[0][0] != "1" {
				return errors.New("estimated_last_login_time not yet updated for newuser2")
			}
			return nil
		})
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
