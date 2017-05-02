// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	descriptor := NewDefaultPrivilegeDescriptor()

	testCases := []struct {
		grantee       string // User to grant/revoke privileges on.
		grant, revoke privilege.List
		show          []UserPrivilegeString
	}{
		{"", nil, nil,
			[]UserPrivilegeString{{security.RootUser, []string{"ALL"}}},
		},
		{security.RootUser, privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{{security.RootUser, []string{"ALL"}}},
		},
		{security.RootUser, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{{security.RootUser, []string{"ALL"}}},
		},
		{"foo", privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{{"foo", []string{"DROP", "INSERT"}}, {security.RootUser, []string{"ALL"}}},
		},
		{"bar", nil, privilege.List{privilege.INSERT, privilege.ALL},
			[]UserPrivilegeString{{"foo", []string{"DROP", "INSERT"}}, {security.RootUser, []string{"ALL"}}},
		},
		{"foo", privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{{"foo", []string{"ALL"}}, {security.RootUser, []string{"ALL"}}},
		},
		{"foo", nil, privilege.List{privilege.SELECT, privilege.INSERT},
			[]UserPrivilegeString{{"foo", []string{"CREATE", "DELETE", "DROP", "GRANT", "UPDATE"}}, {security.RootUser, []string{"ALL"}}},
		},
		{"foo", nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{{security.RootUser, []string{"ALL"}}},
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{security.RootUser, nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{},
		},
	}

	for tcNum, tc := range testCases {
		if tc.grantee != "" {
			if tc.grant != nil {
				descriptor.Grant(tc.grantee, tc.grant)
			}
			if tc.revoke != nil {
				descriptor.Revoke(tc.grantee, tc.revoke)
			}
		}
		show := descriptor.Show()
		if len(show) != len(tc.show) {
			t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
				tcNum, descriptor, show, tc.show)
		}
		for i := 0; i < len(show); i++ {
			if show[i].User != tc.show[i].User || show[i].PrivilegeString() != tc.show[i].PrivilegeString() {
				t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
					tcNum, descriptor, show, tc.show)
			}
		}
	}
}

func TestCheckPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user string
		priv privilege.Kind
		exp  bool
	}{
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"bar", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"bar", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", privilege.ALL, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}),
			"foo", privilege.ALL, true},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", privilege.ALL, false},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"foo", privilege.UPDATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"foo", privilege.DROP, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.ALL}),
			"foo", privilege.DROP, true},
	}

	for tcNum, tc := range testCases {
		if found := tc.pd.CheckPrivilege(tc.user, tc.priv); found != tc.exp {
			t.Errorf("#%d: CheckPrivilege(%s, %v) for descriptor %+v = %t, expected %t",
				tcNum, tc.user, tc.priv, tc.pd, found, tc.exp)
		}
	}
}

func TestAnyPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user string
		exp  bool
	}{
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"bar", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"bar", false},
	}

	for tcNum, tc := range testCases {
		if found := tc.pd.AnyPrivilege(tc.user); found != tc.exp {
			t.Errorf("#%d: AnyPrivilege(%s) for descriptor %+v = %t, expected %t",
				tcNum, tc.user, tc.pd, found, tc.exp)
		}
	}
}

// TestPrivilegeValidate exercises validation for non-system descriptors.
func TestPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	id := ID(keys.MaxReservedDescID + 1)
	descriptor := NewDefaultPrivilegeDescriptor()
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant("foo", privilege.List{privilege.ALL})
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke(security.RootUser, privilege.List{privilege.ALL})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
}

// TestSystemPrivilegeValidate exercises validation for system config
// descriptors. We use a dummy system table installed for testing
// purposes.
func TestSystemPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	id := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[id]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", id)
	}
	SystemAllowedPrivileges[id] = privilege.Lists{
		{privilege.SELECT},
		{privilege.SELECT, privilege.GRANT},
		{privilege.ALL},
	}
	defer delete(SystemAllowedPrivileges, id)

	fooNoGrantPrivilegeErr := "user foo must not have GRANT privileges on this system object"
	rootWrongPrivilegesErr := "user root must have exactly {SELECT} or {SELECT, GRANT} or {ALL} " +
		"privileges on this system object"

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewPrivilegeDescriptor(security.RootUser, privilege.List{privilege.SELECT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has the same privileges as root.
		descriptor.Grant("foo", privilege.List{privilege.SELECT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Invalid: foo has more privileges than root.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); !testutils.IsError(err, fooNoGrantPrivilegeErr) {
			t.Fatalf("expected err=%s, got err=%v", fooNoGrantPrivilegeErr, err)
		}
	}

	{
		// Valid: root user has a different allowable privilege set.
		descriptor := NewPrivilegeDescriptor(security.RootUser,
			privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke("foo", privilege.List{privilege.GRANT, privilege.UPDATE, privilege.ALL})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(security.RootUser, privilege.List{privilege.UPDATE})
		if err := descriptor.Validate(id); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := NewPrivilegeDescriptor(security.RootUser, privilege.List{privilege.UPDATE})
		if err := descriptor.Validate(id); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Valid: root's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(security.RootUser, privilege.List{privilege.UPDATE})
		descriptor.Grant(security.RootUser, privilege.List{privilege.ALL})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// TODO(marc): validate fails here because we do not aggregate
		// privileges into ALL when all are set.
		descriptor.Revoke(security.RootUser, privilege.List{privilege.SELECT})
		descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT})
		if err := descriptor.Validate(id); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}
}
