// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ldapccl

import (
	"context"
	"crypto/tls"
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
	defer testutils.TestingHook(
		&NewLDAPUtil,
		func(ctx context.Context, conf ldapConfig) (ILDAPUtil, error) {
			return &mockLDAPUtil{tlsConfig: &tls.Config{}}, nil
		})()
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
		{testName: "empty server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing ldap server"},
		{testName: "invalid server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap server provided"},
		{testName: "empty port",
			hbaConfLDAPOpts: map[string]string{"ldapport": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing ldap port"},
		{testName: "invalid port",
			hbaConfLDAPOpts: map[string]string{"ldapport": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap port provided"},
		{testName: "empty base dn",
			hbaConfLDAPOpts: map[string]string{"ldapbasedn": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing base DN"},
		{testName: "invalid base dn",
			hbaConfLDAPOpts: map[string]string{"ldapbasedn": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: invalid base DN ‹"invalid"› provided`},
		{testName: "empty bind dn",
			hbaConfLDAPOpts: map[string]string{"ldapbinddn": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing bind DN"},
		{testName: "invalid bind dn",
			hbaConfLDAPOpts: map[string]string{"ldapbinddn": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: "error when searching for user in LDAP server: LDAP search failed: LDAP bind failed: invalid username provided"},
		{testName: "empty bind pwd",
			hbaConfLDAPOpts: map[string]string{"ldapbindpasswd": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing bind password"},
		{testName: "invalid bind pwd",
			hbaConfLDAPOpts: map[string]string{"ldapbindpasswd": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: "error when searching for user in LDAP server: LDAP search failed: LDAP bind failed: invalid password provided"},
		{testName: "empty search attribute",
			hbaConfLDAPOpts: map[string]string{"ldapsearchattribute": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager authentication options",
			expectedDetailedErrMsg: "error validating authentication hba conf options for LDAP: ldap authentication params in HBA conf missing search attribute"},
		{testName: "invalid search attribute",
			hbaConfLDAPOpts: map[string]string{"ldapsearchattribute": invalidParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to find LDAP user distinguished name",
			expectedErrDetails:     "cannot find provided user foo on LDAP server",
			expectedDetailedErrMsg: `error when searching for user in LDAP server: LDAP search failed: invalid search attribute ‹"invalid"› provided`},
		{testName: "empty search filter",
			hbaConfLDAPOpts: map[string]string{"ldapsearchfilter": emptyParam}, user: "foo", fetchUserSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager authentication options",
			expectedDetailedErrMsg: "error validating authentication hba conf options for LDAP: ldap authentication params in HBA conf missing search filter"},
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
	defer testutils.TestingHook(
		&NewLDAPUtil,
		func(ctx context.Context, conf ldapConfig) (ILDAPUtil, error) {
			return &mockLDAPUtil{tlsConfig: &tls.Config{}}, nil
		})()
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
		{testName: "empty server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": emptyParam}, user: "foo", pwd: "bar", ldapAuthSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing ldap server"},
		{testName: "invalid server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": invalidParam}, user: "foo", pwd: "bar", ldapAuthSuccess: false,
			expectedErr:            "LDAP authentication: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap server provided"},
		{testName: "empty port",
			hbaConfLDAPOpts: map[string]string{"ldapport": emptyParam}, user: "foo", pwd: "bar", ldapAuthSuccess: false,
			expectedErr:            "LDAP authentication: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing ldap port"},
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
