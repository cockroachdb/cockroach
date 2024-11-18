// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
