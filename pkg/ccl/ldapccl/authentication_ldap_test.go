// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"
	"fmt"
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

func TestLDAPFetchUser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function
	_, newMockLDAPUtil := LDAPMocks()
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
		"ldapbindpasswd": "readonly_pwd", "ldapsearchattribute": "uid", "ldapsearchfilter": "(memberOf=cn=users,ou=groups,dc=localhost)",
	}
	testCases := []struct {
		testName               string
		hbaConfLDAPOpts        map[string]string
		user                   string
		fetchUserSuccess       bool
		expectedErr            string
		expectedErrDetails     string
		expectedDetailedErrMsg string
	}{
		{testName: "proper hba conf and valid user cred",
			user: "foo", fetchUserSuccess: true},
		{testName: "proper hba conf and root user cred",
			user: "root", fetchUserSuccess: false,
			expectedErr:        "LDAP authentication: invalid identity",
			expectedErrDetails: "cannot use LDAP auth to login to a reserved user root"},
		{testName: "proper hba conf and node user cred",
			user: "node", fetchUserSuccess: false, expectedErr: "LDAP authentication: invalid identity",
			expectedErrDetails: "cannot use LDAP auth to login to a reserved user node"},
		{testName: "invalid ldap option",
			hbaConfLDAPOpts: map[string]string{"invalidOpt": "invalidVal"}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to parse hba conf options",
			expectedDetailedErrMsg: `error parsing hba conf options for LDAP: invalid LDAP option provided in hba conf: ‹invalidOpt›`},
		{testName: "invalid server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap server provided"},
		{testName: "invalid port",
			hbaConfLDAPOpts: map[string]string{"ldapport": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap port provided"},
		{testName: "invalid base dn",
			hbaConfLDAPOpts: map[string]string{"ldapbasedn": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: invalid base DN ‹"invalid"› provided`},
		{testName: "invalid bind dn",
			hbaConfLDAPOpts: map[string]string{"ldapbinddn": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: error binding as LDAP service user with configured credentials",
			expectedErrDetails:     "",
			expectedDetailedErrMsg: "error binding ldap service account: LDAP bind failed: invalid username provided"},
		{testName: "invalid bind pwd",
			hbaConfLDAPOpts: map[string]string{"ldapbindpasswd": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: error binding as LDAP service user with configured credentials",
			expectedErrDetails:     "",
			expectedDetailedErrMsg: "error binding ldap service account: LDAP bind failed: invalid password provided"},
		{testName: "invalid search attribute",
			hbaConfLDAPOpts: map[string]string{"ldapsearchattribute": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: invalid search attribute ‹"invalid"› provided`},
		{testName: "invalid search filter",
			hbaConfLDAPOpts: map[string]string{"ldapsearchfilter": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: invalid search filter ‹"invalid"› provided`},
		{testName: "invalid ldap user",
			user: invalidParam, fetchUserSuccess: false, expectedErr: "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user invalid on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: invalid search value ‹"invalid"› provided`},
		{testName: "no such ldap user",
			user: "", fetchUserSuccess: false, expectedErr: "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user  on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: user ‹""› does not exist`},
		{testName: "too many matching ldap users",
			user: "foo,foo2,foo3", fetchUserSuccess: false, expectedErr: "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo,foo2,foo3 on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: too many matching entries returned for user ‹"foo,foo2,foo3"›`},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: testName:%v hbConfOpts:%v user:%v fetchUserSuccess:%v", i, tc.testName, tc.hbaConfLDAPOpts, tc.user, tc.fetchUserSuccess), func(t *testing.T) {
			hbaEntry := constructHBAEntry(t, hbaEntryBase, hbaConfLDAPDefaultOpts, tc.hbaConfLDAPOpts)
			_, detailedErrorMsg, err := manager.FetchLDAPUserDN(
				ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(tc.user), &hbaEntry, nil)

			if (err == nil) != tc.fetchUserSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.fetchUserSuccess, err)
			}
			if err != nil {
				require.Equal(t, tc.expectedErr, err.Error())
				require.Equal(t, tc.expectedErrDetails, errors.FlattenDetails(err))
				require.Equal(t, redact.RedactableString(tc.expectedDetailedErrMsg), detailedErrorMsg)
			}
		})
	}
}

func TestLDAPAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Intercept the call to NewLDAPUtil and return the mocked NewLDAPUtil function
	_, newMockLDAPUtil := LDAPMocks()
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
		"ldapbindpasswd": "readonly_pwd", "ldapsearchattribute": "uid", "ldapsearchfilter": "(memberOf=cn=users,ou=groups,dc=localhost)",
	}
	testCases := []struct {
		testName               string
		hbaConfLDAPOpts        map[string]string
		user                   string
		pwd                    string
		ldapAuthSuccess        bool
		expectedErr            string
		expectedErrDetails     string
		expectedDetailedErrMsg string
	}{
		{testName: "proper hba conf and valid user cred",
			user: "foo", pwd: "bar", ldapAuthSuccess: true},
		{testName: "invalid ldap option",
			hbaConfLDAPOpts: map[string]string{"invalidOpt": "invalidVal"}, user: "foo", pwd: "bar", ldapAuthSuccess: false,
			expectedErr:            "LDAP authentication: unable to parse hba conf options",
			expectedDetailedErrMsg: `error parsing hba conf options for LDAP: invalid LDAP option provided in hba conf: ‹invalidOpt›`},
		{testName: "invalid server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": invalidParam}, user: "foo", pwd: "bar", ldapAuthSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap server provided"},
		{testName: "invalid port",
			hbaConfLDAPOpts: map[string]string{"ldapport": invalidParam}, user: "foo", pwd: "bar", ldapAuthSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap port provided"},
		{testName: "invalid ldap password",
			user: "foo", pwd: invalidParam, ldapAuthSuccess: false, expectedErr: "LDAP authentication: unable to bind as LDAP user",
			expectedErrDetails:     "credentials invalid for LDAP server user foo",
			expectedDetailedErrMsg: `error when binding as user ‹foo› with DN(‹cn=foo›) in LDAP server: LDAP bind failed: invalid password provided`},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: testName:%v hbConfOpts:%v user:%v password:%v", i, tc.testName, tc.hbaConfLDAPOpts, tc.user, tc.pwd), func(t *testing.T) {
			hbaEntry := constructHBAEntry(t, hbaEntryBase, hbaConfLDAPDefaultOpts, tc.hbaConfLDAPOpts)
			ldapUserDN, err := distinguishedname.ParseDN("cn=" + tc.user)
			if err != nil {
				t.Fatalf("error parsing DN string for user %s: %v", tc.user, err)
			}
			detailedErrorMsg, err := manager.ValidateLDAPLogin(
				ctx, s.ClusterSettings(), ldapUserDN, username.MakeSQLUsernameFromPreNormalizedString(tc.user), tc.pwd, &hbaEntry, nil)

			if (err == nil) != tc.ldapAuthSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.ldapAuthSuccess, err)
			}
			if err != nil {
				require.Equal(t, tc.expectedErr, err.Error())
				require.Equal(t, tc.expectedErrDetails, errors.FlattenDetails(err))
				require.Equal(t, redact.RedactableString(tc.expectedDetailedErrMsg), detailedErrorMsg)
			}
		})
	}
}

func TestLDAPConnectionReset(t *testing.T) {
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
		"ldapserver":          "localhost",
		"ldapport":            "636",
		"ldapbasedn":          "dc=localhost",
		"ldapbinddn":          "cn=readonly,dc=localhost",
		"ldapbindpasswd":      "readonly_pwd",
		"ldapsearchattribute": "uid",
		"ldapsearchfilter":    "(memberOf=cn=users,ou=groups,dc=localhost)",
		"ldapgrouplistfilter": "(cn=ldap_parent_1)",
	}
	hbaEntry := constructHBAEntry(t, hbaEntryBase, hbaConfLDAPDefaultOpts, nil)

	if _, _, err := manager.FetchLDAPUserDN(
		ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("foo"), &hbaEntry, nil); err != nil {
		t.Fatalf("expected success, got err=%v", err)
	}
	ldapConnection1 := mockLDAP.getLDAPsConn()

	mockLDAP.resetLDAPsConn()

	if _, _, err := manager.FetchLDAPUserDN(
		ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString("foo"), &hbaEntry, nil); err != nil {
		t.Fatalf("expected success, got err=%v", err)
	}
	ldapConnection2 := mockLDAP.getLDAPsConn()

	require.Falsef(t, ldapConnection1 == ldapConnection2,
		"expected a different ldap connection as previous connection was reset by server, conn1: %v, conn2: %v",
		ldapConnection1, ldapConnection2)
}
