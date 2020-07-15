// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

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
	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRole)

	testCases := []struct {
		grantee       string // User to grant/revoke privileges on.
		grant, revoke privilege.List
		show          []UserPrivilegeString
		objectType    privilege.ObjectType
	}{
		{"", nil, nil,
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{security.RootUser, privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{security.RootUser, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{"foo", privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{"foo", []string{"DROP", "INSERT"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{"bar", nil, privilege.List{privilege.INSERT, privilege.ALL},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{"foo", []string{"DROP", "INSERT"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{"foo", privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{"foo", []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{"foo", nil, privilege.List{privilege.SELECT, privilege.INSERT},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{"foo", []string{"CREATE", "DELETE", "DROP", "GRANT", "UPDATE", "ZONECONFIG"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{"foo", nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{security.RootUser, nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
			},
			privilege.Table,
		},
		// Ensure revoking USAGE from a user with ALL privilege on a type
		// leaves the user with only GRANT privilege.
		{"foo", privilege.List{privilege.ALL}, privilege.List{privilege.USAGE},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
				{"foo", []string{"GRANT"}},
			},
			privilege.Type,
		},
		// Ensure revoking USAGE, GRANT from a user with ALL privilege on a type
		// leaves the user with no privileges.
		{"foo",
			privilege.List{privilege.ALL}, privilege.List{privilege.USAGE, privilege.GRANT},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
			},
			privilege.Type,
		},
		// Ensure revoking CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG
		// from a user with ALL privilege on a table leaves the user with no privileges.
		{"foo",
			privilege.List{privilege.ALL}, privilege.List{privilege.CREATE, privilege.DROP,
				privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE,
				privilege.ZONECONFIG},
			[]UserPrivilegeString{
				{security.AdminRole, []string{"ALL"}},
			},
			privilege.Table,
		},
	}

	for tcNum, tc := range testCases {
		if tc.grantee != "" {
			if tc.grant != nil {
				descriptor.Grant(tc.grantee, tc.grant)
			}
			if tc.revoke != nil {
				descriptor.Revoke(tc.grantee, tc.revoke, tc.objectType)
			}
		}
		show := descriptor.Show(tc.objectType)
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
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"bar", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"bar", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"foo", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}, security.AdminRole),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"foo", privilege.ALL, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}, security.AdminRole),
			"foo", privilege.ALL, true},
		{NewPrivilegeDescriptor("foo", privilege.List{}, security.AdminRole),
			"foo", privilege.ALL, false},
		{NewPrivilegeDescriptor("foo", privilege.List{}, security.AdminRole),
			"foo", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRole),
			"foo", privilege.UPDATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRole),
			"foo", privilege.DROP, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.ALL},
			security.AdminRole),
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
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}, security.AdminRole),
			"bar", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}, security.AdminRole),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{}, security.AdminRole),
			"foo", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRole),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRole),
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
	id := ID(keys.MinUserDescID)
	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRole)
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
	descriptor.Revoke(
		security.RootUser, privilege.List{privilege.SELECT}, privilege.Table)
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke(security.RootUser, privilege.List{privilege.ALL}, privilege.Table)
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
	SystemAllowedPrivileges[id] = privilege.List{
		privilege.SELECT,
		privilege.GRANT,
	}
	defer delete(SystemAllowedPrivileges, id)

	rootWrongPrivilegesErr := "user root must have exactly SELECT, GRANT " +
		"privileges on system object with ID=.*"
	adminWrongPrivilegesErr := "user admin must have exactly SELECT, GRANT " +
		"privileges on system object with ID=.*"

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
			security.AdminRole,
		)
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant("foo", privilege.List{privilege.SELECT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has exactly the allowed privileges.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Valid: root has exactly the allowed privileges.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
			security.AdminRole,
		)

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke(
			"foo", privilege.List{privilege.GRANT, privilege.UPDATE, privilege.ALL}, privilege.Table)
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
		descriptor := NewCustomSuperuserPrivilegeDescriptor(privilege.List{privilege.UPDATE},
			security.AdminRole)
		if err := descriptor.Validate(id); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		descriptor.Revoke(security.RootUser, privilege.List{privilege.UPDATE}, privilege.Table)
		descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(security.AdminRole, privilege.List{privilege.UPDATE}, privilege.Table)
		descriptor.Grant(security.AdminRole, privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFixPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system ID.
	userID := ID(keys.MinUserDescID)
	userPrivs := privilege.List{privilege.ALL}

	// And create an entry for a fake system table.
	systemID := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[systemID]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", systemID)
	}
	systemPrivs := privilege.List{
		privilege.SELECT,
		privilege.GRANT,
	}
	SystemAllowedPrivileges[systemID] = systemPrivs
	defer delete(SystemAllowedPrivileges, systemID)

	type userPrivileges map[string]privilege.List

	testCases := []struct {
		id       ID
		input    userPrivileges
		modified bool
		output   userPrivileges
	}{
		{
			// Empty privileges for system ID.
			systemID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUser:  systemPrivs,
				security.AdminRole: systemPrivs,
			},
		},
		{
			// Valid requirements for system ID.
			systemID,
			userPrivileges{
				security.RootUser:  systemPrivs,
				security.AdminRole: systemPrivs,
				"foo":              privilege.List{privilege.SELECT},
				"bar":              privilege.List{privilege.GRANT},
				"baz":              privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUser:  systemPrivs,
				security.AdminRole: systemPrivs,
				"foo":              privilege.List{privilege.SELECT},
				"bar":              privilege.List{privilege.GRANT},
				"baz":              privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// Too many privileges for system ID.
			systemID,
			userPrivileges{
				security.RootUser:  privilege.List{privilege.ALL},
				security.AdminRole: privilege.List{privilege.ALL},
				"foo":              privilege.List{privilege.ALL},
				"bar":              privilege.List{privilege.SELECT, privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUser:  systemPrivs,
				security.AdminRole: systemPrivs,
				"foo":              privilege.List{},
				"bar":              privilege.List{privilege.SELECT},
			},
		},
		{
			// Empty privileges for non-system ID.
			userID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUser:  userPrivs,
				security.AdminRole: userPrivs,
			},
		},
		{
			// Valid requirements for non-system ID.
			userID,
			userPrivileges{
				security.RootUser:  userPrivs,
				security.AdminRole: userPrivs,
				"foo":              privilege.List{privilege.SELECT},
				"bar":              privilege.List{privilege.GRANT},
				"baz":              privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUser:  userPrivs,
				security.AdminRole: userPrivs,
				"foo":              privilege.List{privilege.SELECT},
				"bar":              privilege.List{privilege.GRANT},
				"baz":              privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// All privileges are allowed for non-system ID, but we need super users.
			userID,
			userPrivileges{
				"foo": privilege.List{privilege.ALL},
				"bar": privilege.List{privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUser:  privilege.List{privilege.ALL},
				security.AdminRole: privilege.List{privilege.ALL},
				"foo":              privilege.List{privilege.ALL},
				"bar":              privilege.List{privilege.UPDATE},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(u, p)
		}

		if a, e := MaybeFixPrivileges(testCase.id, desc), testCase.modified; a != e {
			t.Errorf("#%d: expected modified=%t, got modified=%t", num, e, a)
			continue
		}

		if a, e := len(desc.Users), len(testCase.output); a != e {
			t.Errorf("#%d: expected %d users (%v), got %d (%v)", num, e, testCase.output, a, desc.Users)
			continue
		}

		for u, p := range testCase.output {
			outputUser, ok := desc.findUser(u)
			if !ok {
				t.Fatalf("#%d: expected user %s in output, but not found (%v)", num, u, desc.Users)
			}
			if a, e := privilege.ListFromBitField(outputUser.Privileges, privilege.Any), p; a.ToBitField() != e.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v", num, u, e, a)
			}
		}
	}
}

func TestValidateOwnership(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system id.
	id := ID(keys.MinUserDescID)

	// A privilege descriptor with a version before OwnerVersion can have
	// no owner.
	privs := PrivilegeDescriptor{
		Users: []UserPrivileges{
			{
				User:       security.AdminRole,
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				User:       security.RootUser,
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		}}
	err := privs.Validate(id)
	if err != nil {
		t.Fatal(err)
	}

	// A privilege descriptor with version OwnerVersion and onwards should
	// have an owner.
	privs = PrivilegeDescriptor{
		Users: []UserPrivileges{
			{
				User:       security.AdminRole,
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				User:       security.RootUser,
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		},
		Version: OwnerVersion,
	}

	err = privs.Validate(id)
	if err == nil {
		t.Fatal("unexpected success")
	}

	privs = PrivilegeDescriptor{
		Owner: security.RootUser,
		Users: []UserPrivileges{
			{
				User:       security.AdminRole,
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				User:       security.RootUser,
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		},
		Version: OwnerVersion,
	}

	err = privs.Validate(id)
	if err != nil {
		t.Fatal(err)
	}
}
