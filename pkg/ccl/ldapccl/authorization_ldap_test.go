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
		"ldapbindpasswd": "readonly_pwd", "ldapgrouplistfilter": "(objectCategory=CN=Group,CN=Schema,CN=Configuration,DC=crlcloud,DC=dev)",
	}
	testCases := []struct {
		testName               string
		hbaConfLDAPOpts        map[string]string
		userDN                 string
		authZSuccess           bool
		ldapGroups             []string
		expectedErr            string
		expectedErrDetails     string
		expectedDetailedErrMsg string
	}{
		{testName: "proper hba conf and valid user cred",
			userDN: "CN=foo", authZSuccess: true, ldapGroups: []string{"CN=foo"}},
		{testName: "proper hba conf and invalid distinguished name",
			userDN: "root", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate provided ldap DN",
			expectedDetailedErrMsg: "error validating provided ldap DN ‹\"root\"› for LDAP: failed to parse distinguished name ‹root›: ‹DN ended with incomplete type, value pair›"},
		{testName: "invalid ldap option",
			hbaConfLDAPOpts: map[string]string{"invalidOpt": "invalidVal"}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to parse hba conf options",
			expectedDetailedErrMsg: `error parsing hba conf options for LDAP: invalid LDAP option provided in hba conf: ‹invalidOpt›`},
		{testName: "empty server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": emptyParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing ldap server"},
		{testName: "invalid server",
			hbaConfLDAPOpts: map[string]string{"ldapserver": invalidParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap server provided"},
		{testName: "empty port",
			hbaConfLDAPOpts: map[string]string{"ldapport": emptyParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing ldap port"},
		{testName: "invalid port",
			hbaConfLDAPOpts: map[string]string{"ldapport": invalidParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to establish LDAP connection",
			expectedDetailedErrMsg: "error when trying to create LDAP connection: LDAPs connection failed: invalid ldap port provided"},
		{testName: "empty base dn",
			hbaConfLDAPOpts: map[string]string{"ldapbasedn": emptyParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing base DN"},
		{testName: "invalid base dn",
			hbaConfLDAPOpts: map[string]string{"ldapbasedn": invalidParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when searching for user dn ‹"CN=foo"› in LDAP server: LDAP groups list failed: invalid base DN ‹"invalid"› provided`},
		{testName: "empty bind dn",
			hbaConfLDAPOpts: map[string]string{"ldapbinddn": emptyParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing bind DN"},
		{testName: "invalid bind dn",
			hbaConfLDAPOpts: map[string]string{"ldapbinddn": invalidParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when searching for user dn ‹"CN=foo"› in LDAP server: LDAP groups list failed: LDAP bind failed: invalid username provided`},
		{testName: "empty bind pwd",
			hbaConfLDAPOpts: map[string]string{"ldapbindpasswd": emptyParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate authManager base options",
			expectedDetailedErrMsg: "error validating base hba conf options for LDAP: ldap params in HBA conf missing bind password"},
		{testName: "invalid bind pwd",
			hbaConfLDAPOpts: map[string]string{"ldapbindpasswd": invalidParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when searching for user dn ‹"CN=foo"› in LDAP server: LDAP groups list failed: LDAP bind failed: invalid password provided`},
		{testName: "empty group list filter",
			hbaConfLDAPOpts: map[string]string{"ldapgrouplistfilter": emptyParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to validate authManager authorization options",
			expectedDetailedErrMsg: "error validating authorization hba conf options for LDAP: ldap authorization params in HBA conf missing group list attribute"},
		{testName: "invalid group list filter",
			hbaConfLDAPOpts: map[string]string{"ldapgrouplistfilter": invalidParam}, userDN: "CN=foo", authZSuccess: false,
			expectedErr:            "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when searching for user dn ‹"CN=foo"› in LDAP server: LDAP groups list failed: invalid group list filter ‹"invalid"› provided`},
		{testName: "no matching ldap groups",
			userDN: "", authZSuccess: false, expectedErr: "LDAP authorization: unable to fetch groups for user",
			expectedErrDetails:     "cannot find groups for which user is a member",
			expectedDetailedErrMsg: `error when searching for user dn ‹""› in LDAP server: LDAP groups list failed: user dn ‹""› does not belong to any groups`},
		{testName: "more than 1 matching ldap groups",
			userDN: "CN=foo,CN=foo2,CN=foo3", authZSuccess: true, ldapGroups: []string{"CN=foo", "CN=foo2", "CN=foo3"}},
	}
	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: testName:%v hbConfOpts:%v userDN:%v", i, tc.testName, tc.hbaConfLDAPOpts, tc.userDN), func(t *testing.T) {
			hbaEntry := constructHBAEntry(t, hbaEntryBase, hbaConfLDAPDefaultOpts, tc.hbaConfLDAPOpts)
			retrievedLDAPGroups, detailedErrorMsg, err := manager.FetchLDAPGroups(
				ctx, s.ClusterSettings(), username.MakeSQLUsernameFromPreNormalizedString(tc.userDN), &hbaEntry, nil)

			if (err == nil) != tc.authZSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.authZSuccess, err)
			}
			if err != nil {
				require.Equal(t, tc.expectedErr, err.Error())
				require.Equal(t, tc.expectedErrDetails, errors.FlattenDetails(err))
				require.Equal(t, redact.RedactableString(tc.expectedDetailedErrMsg), detailedErrorMsg)
			} else {
				require.Equal(t, tc.ldapGroups, retrievedLDAPGroups)
			}
		})
	}
}
