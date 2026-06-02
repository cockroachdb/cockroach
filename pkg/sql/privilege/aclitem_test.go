// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package privilege_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestACLItemString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name         string
		grantee      username.SQLUsername
		grantor      username.SQLUsername
		privs        privilege.List
		grantOptions privilege.List
		expected     string
	}{
		{
			name:     "basic privileges",
			grantee:  username.MakeSQLUsernameFromPreNormalizedString("user1"),
			grantor:  username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:    privilege.List{privilege.SELECT, privilege.INSERT},
			expected: "user1=ra/root",
		},
		{
			name:         "with grant options",
			grantee:      username.MakeSQLUsernameFromPreNormalizedString("user1"),
			grantor:      username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:        privilege.List{privilege.SELECT, privilege.INSERT},
			grantOptions: privilege.List{privilege.SELECT},
			expected:     "user1=r*a/root",
		},
		{
			name:     "PUBLIC grantee",
			grantee:  username.PublicRoleName(),
			grantor:  username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:    privilege.List{privilege.CONNECT, privilege.TEMPORARY},
			expected: "=cT/root",
		},
		{
			name:     "special characters in names",
			grantee:  username.MakeSQLUsernameFromPreNormalizedString("user-1"),
			grantor:  username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:    privilege.List{privilege.SELECT},
			expected: `"user-1"=r/root`,
		},
		{
			name:     "all table privileges",
			grantee:  username.MakeSQLUsernameFromPreNormalizedString("admin"),
			grantor:  username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:    privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.TRIGGER, privilege.MAINTAIN},
			expected: "admin=Cradwtm/root",
		},
		{
			name:     "no privileges",
			grantee:  username.MakeSQLUsernameFromPreNormalizedString("user1"),
			grantor:  username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:    privilege.List{},
			expected: "user1=/root",
		},
		{
			name:     "privileges sorted by bit position",
			grantee:  username.MakeSQLUsernameFromPreNormalizedString("user1"),
			grantor:  username.MakeSQLUsernameFromPreNormalizedString("root"),
			privs:    privilege.List{privilege.UPDATE, privilege.SELECT, privilege.CREATE},
			expected: "user1=Crw/root",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			item := privilege.NewACLItem(tc.grantee, tc.grantor, tc.privs, tc.grantOptions)
			require.Equal(t, tc.expected, item.String())
		})
	}
}

func TestParseACLItem(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		input        string
		wantGrantee  string
		wantGrantor  string
		wantPrivs    privilege.List
		wantGrantOpt privilege.List
		wantErr      string
	}{
		// Valid cases.
		{
			input:       "user1=r/root",
			wantGrantee: "user1",
			wantGrantor: "root",
			wantPrivs:   privilege.List{privilege.SELECT},
		},
		{
			input:        "user1=r*w/root",
			wantGrantee:  "user1",
			wantGrantor:  "root",
			wantPrivs:    privilege.List{privilege.SELECT, privilege.UPDATE},
			wantGrantOpt: privilege.List{privilege.SELECT},
		},
		{
			input:       "=cT/root",
			wantGrantee: "public",
			wantGrantor: "root",
			wantPrivs:   privilege.List{privilege.CONNECT, privilege.TEMPORARY},
		},
		{
			input:       `"user-1"=rw/root`,
			wantGrantee: "user-1",
			wantGrantor: "root",
			wantPrivs:   privilege.List{privilege.SELECT, privilege.UPDATE},
		},
		{
			input:       `user1=r/"grantor-1"`,
			wantGrantee: "user1",
			wantGrantor: "grantor-1",
			wantPrivs:   privilege.List{privilege.SELECT},
		},
		{
			input:       "user1=r/",
			wantGrantee: "user1",
			wantGrantor: "",
			wantPrivs:   privilege.List{privilege.SELECT},
		},
		{
			input:       "user1=/root",
			wantGrantee: "user1",
			wantGrantor: "root",
		},
		// Missing '/' — treat as missing grantor.
		{
			input:       "user1=r",
			wantGrantee: "user1",
			wantGrantor: "",
			wantPrivs:   privilege.List{privilege.SELECT},
		},

		// Invalid cases.
		{input: "user1=z/", wantErr: `invalid mode character`},
		{input: "user1rw/", wantErr: `missing "=" sign`},
		{input: "user1=*r/", wantErr: `"*" must follow a privilege character`},
		{input: "user1=r/grantor extra", wantErr: "extra characters after aclitem"},
		{input: `"unterminated=r/`, wantErr: "unterminated quoted identifier"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			item, err := privilege.ParseACLItem(tc.input)
			if tc.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantGrantee, item.Grantee.Normalized())
			require.Equal(t, tc.wantGrantor, item.Grantor.Normalized())
			require.Equal(t, tc.wantPrivs, item.Privileges)
			require.Equal(t, tc.wantGrantOpt, item.GrantOptions)
		})
	}
}

func TestACLItemRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	inputs := []string{
		"user1=r/root",
		"user1=r*aw*/root",
		"=cT/root",
		`"user-1"=rw/root`,
		"admin=Cradwtm/myuser",
		"user1=/root",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			item, err := privilege.ParseACLItem(input)
			require.NoError(t, err)
			require.Equal(t, input, item.String())
		})
	}
}

func TestACLItem_SafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	item := privilege.NewACLItem(
		username.MakeSQLUsernameFromPreNormalizedString("myuser"),
		username.MakeSQLUsernameFromPreNormalizedString("root"),
		privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE},
		privilege.List{privilege.SELECT},
	)
	redacted := string(redact.Sprint(item))
	echotest.Require(t, redacted, datapathutils.TestDataPath(t, t.Name()))
}

func TestDefaultACLItems(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		objectType privilege.ObjectType
		owner      username.SQLUsername
		expected   []string
	}{
		{
			objectType: privilege.Table,
			owner:      username.RootUserName(),
			expected:   []string{"admin=C*r*a*d*w*D*t*m*x*/root", "root=C*r*a*d*w*D*t*m*x*/root"},
		},
		{
			objectType: privilege.Table,
			owner:      username.MakeSQLUsernameFromPreNormalizedString("myuser"),
			expected:   []string{"admin=C*r*a*d*w*D*t*m*x*/myuser", "root=C*r*a*d*w*D*t*m*x*/myuser", "myuser=CradwDtmx/myuser"},
		},
		{
			objectType: privilege.Sequence,
			owner:      username.RootUserName(),
			expected:   []string{"admin=C*r*a*d*w*U*/root", "root=C*r*a*d*w*U*/root"},
		},
		{
			objectType: privilege.Database,
			owner:      username.RootUserName(),
			expected:   []string{"=cT/root", "admin=C*c*T*/root", "root=C*c*T*/root"},
		},
		{
			objectType: privilege.Schema,
			owner:      username.RootUserName(),
			expected:   []string{"admin=C*U*/root", "root=C*U*/root"},
		},
		{
			objectType: privilege.Routine,
			owner:      username.RootUserName(),
			expected:   []string{"=X/root", "admin=X*/root", "root=X*/root"},
		},
		{
			objectType: privilege.Type,
			owner:      username.RootUserName(),
			expected:   []string{"=U/root", "admin=U*/root", "root=U*/root"},
		},
		{
			objectType: privilege.Database,
			owner:      username.MakeSQLUsernameFromPreNormalizedString("myuser"),
			expected:   []string{"=cT/myuser", "admin=C*c*T*/myuser", "root=C*c*T*/myuser", "myuser=CcT/myuser"},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s", tc.objectType, tc.owner), func(t *testing.T) {
			items, err := privilege.DefaultACLItems(tc.objectType, tc.owner)
			require.NoError(t, err)
			strs := make([]string, len(items))
			for i, item := range items {
				strs[i] = item.String()
			}
			require.Equal(t, tc.expected, strs)
		})
	}
}

func TestIsDefaultACL(t *testing.T) {
	defer leaktest.AfterTest(t)()

	owner := username.MakeSQLUsernameFromPreNormalizedString("myuser")

	// Get the defaults, verify they are detected as defaults.
	defaults, err := privilege.DefaultACLItems(privilege.Table, owner)
	require.NoError(t, err)

	isDefault, err := privilege.IsDefaultACL(defaults, privilege.Table, owner)
	require.NoError(t, err)
	require.True(t, isDefault)

	// Add an extra item — should no longer be default.
	extra := append(defaults, privilege.NewACLItem(
		username.MakeSQLUsernameFromPreNormalizedString("other"),
		owner,
		privilege.List{privilege.SELECT},
		nil,
	))
	isDefault, err = privilege.IsDefaultACL(extra, privilege.Table, owner)
	require.NoError(t, err)
	require.False(t, isDefault)
}
