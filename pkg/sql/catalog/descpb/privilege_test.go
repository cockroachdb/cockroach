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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/privilegepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRoleName())

	testUser := security.TestUserName()
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		grantee       security.SQLUsername // User to grant/revoke privileges on.
		grant, revoke privilegepb.List
		show          []UserPrivilegeString
		objectType    privilegepb.ObjectType
	}{
		{security.SQLUsername{}, nil, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilegepb.Table,
		},
		{security.RootUserName(), privilegepb.List{privilegepb.Privilege_ALL}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilegepb.Table,
		},
		{security.RootUserName(), privilegepb.List{privilegepb.Privilege_INSERT, privilegepb.Privilege_DROP_PRIVILEGE}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilegepb.Table,
		},
		{testUser, privilegepb.List{privilegepb.Privilege_INSERT, privilegepb.Privilege_DROP_PRIVILEGE}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"DROP", "INSERT"}},
			},
			privilegepb.Table,
		},
		{barUser, nil, privilegepb.List{privilegepb.Privilege_INSERT, privilegepb.Privilege_ALL},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"DROP", "INSERT"}},
			},
			privilegepb.Table,
		},
		{testUser, privilegepb.List{privilegepb.Privilege_ALL}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"ALL"}},
			},
			privilegepb.Table,
		},
		{testUser, nil, privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_INSERT},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"CREATE", "DELETE", "DROP", "GRANT", "UPDATE", "ZONECONFIG"}},
			},
			privilegepb.Table,
		},
		{testUser, nil, privilegepb.List{privilegepb.Privilege_ALL},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilegepb.Table,
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{security.RootUserName(), nil, privilegepb.List{privilegepb.Privilege_ALL},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilegepb.Table,
		},
		// Ensure revoking USAGE from a user with ALL privilege on a type
		// leaves the user with only GRANT privilege.
		{testUser, privilegepb.List{privilegepb.Privilege_ALL}, privilegepb.List{privilegepb.Privilege_USAGE},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{testUser, []string{"GRANT"}},
			},
			privilegepb.Type,
		},
		// Ensure revoking USAGE, GRANT from a user with ALL privilege on a type
		// leaves the user with no privileges.
		{testUser,
			privilegepb.List{privilegepb.Privilege_ALL}, privilegepb.List{privilegepb.Privilege_USAGE, privilegepb.Privilege_GRANT},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilegepb.Type,
		},
		// Ensure revoking CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG
		// from a user with ALL privilege on a table leaves the user with no privileges.
		{testUser,
			privilegepb.List{privilegepb.Privilege_ALL}, privilegepb.List{privilegepb.Privilege_CREATE, privilegepb.Privilege_DROP_PRIVILEGE,
				privilegepb.Privilege_GRANT, privilegepb.Privilege_SELECT, privilegepb.Privilege_INSERT, privilegepb.Privilege_DELETE, privilegepb.Privilege_UPDATE,
				privilegepb.Privilege_ZONECONFIG},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilegepb.Table,
		},
		// Ensure revoking CONNECT, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG
		// from a user with ALL privilege on a database leaves the user with no privileges.
		{testUser,
			privilegepb.List{privilegepb.Privilege_ALL}, privilegepb.List{privilegepb.Privilege_CONNECT, privilegepb.Privilege_CREATE,
				privilegepb.Privilege_DROP_PRIVILEGE, privilegepb.Privilege_GRANT, privilegepb.Privilege_SELECT, privilegepb.Privilege_INSERT, privilegepb.Privilege_DELETE,
				privilegepb.Privilege_UPDATE, privilegepb.Privilege_ZONECONFIG},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilegepb.Database,
		},
	}

	for tcNum, tc := range testCases {
		if !tc.grantee.Undefined() {
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

	testUser := security.TestUserName()
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user security.SQLUsername
		priv privilegepb.Privilege
		exp  bool
	}{
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_CREATE, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			barUser, privilegepb.Privilege_CREATE, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			barUser, privilegepb.Privilege_DROP_PRIVILEGE, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_DROP_PRIVILEGE, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_ALL}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_CREATE, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_ALL, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_ALL}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_ALL, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_ALL, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{}, security.AdminRoleName()),
			testUser, privilegepb.Privilege_CREATE, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE, privilegepb.Privilege_DROP_PRIVILEGE},
			security.AdminRoleName()),
			testUser, privilegepb.Privilege_UPDATE, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE, privilegepb.Privilege_DROP_PRIVILEGE},
			security.AdminRoleName()),
			testUser, privilegepb.Privilege_DROP_PRIVILEGE, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE, privilegepb.Privilege_ALL},
			security.AdminRoleName()),
			testUser, privilegepb.Privilege_DROP_PRIVILEGE, true},
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

	testUser := security.TestUserName()
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user security.SQLUsername
		exp  bool
	}{
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			testUser, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE}, security.AdminRoleName()),
			barUser, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_ALL}, security.AdminRoleName()),
			testUser, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{}, security.AdminRoleName()),
			testUser, false},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE, privilegepb.Privilege_DROP_PRIVILEGE},
			security.AdminRoleName()),
			testUser, true},
		{NewPrivilegeDescriptor(testUser, privilegepb.List{privilegepb.Privilege_CREATE, privilegepb.Privilege_DROP_PRIVILEGE},
			security.AdminRoleName()),
			barUser, false},
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

	testUser := security.TestUserName()

	id := ID(keys.MinUserDescID)
	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
	if err := descriptor.Validate(id, privilegepb.Table); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(testUser, privilegepb.List{privilegepb.Privilege_ALL})
	if err := descriptor.Validate(id, privilegepb.Table); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(security.RootUserName(), privilegepb.List{privilegepb.Privilege_SELECT})
	if err := descriptor.Validate(id, privilegepb.Table); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke(security.RootUserName(), privilegepb.List{privilegepb.Privilege_SELECT},
		privilegepb.Table)
	if err := descriptor.Validate(id, privilegepb.Table); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(security.RootUserName(), privilegepb.List{privilegepb.Privilege_SELECT})
	if err := descriptor.Validate(id, privilegepb.Table); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke(security.RootUserName(), privilegepb.List{privilegepb.Privilege_ALL}, privilegepb.Table)
	if err := descriptor.Validate(id, privilegepb.Table); err == nil {
		t.Fatal("unexpected success")
	}
}

func TestValidPrivilegesForObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	id := ID(keys.MinUserDescID)

	testUser := security.TestUserName()

	testCases := []struct {
		objectType      privilegepb.ObjectType
		validPrivileges privilegepb.List
	}{
		{privilegepb.Table, privilegepb.TablePrivileges},
		{privilegepb.Database, privilegepb.DBPrivileges},
		{privilegepb.Schema, privilegepb.SchemaPrivileges},
		{privilegepb.Type, privilegepb.TypePrivileges},
	}

	for _, tc := range testCases {
		for _, priv := range tc.validPrivileges {
			privDesc := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			privDesc.Grant(testUser, privilegepb.List{priv})
			err := privDesc.Validate(id, tc.objectType)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Derive invalidPrivileges from the validPrivileges.
		invalidPrivileges := privilegepb.List{}
		for _, priv := range privilegepb.AllPrivileges {
			if priv.Mask() & ^tc.validPrivileges.ToBitField() != 0 {
				invalidPrivileges = append(invalidPrivileges, priv)
			}
		}

		for _, priv := range invalidPrivileges {
			privDesc := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			privDesc.Grant(testUser, privilegepb.List{priv})
			err := privDesc.Validate(id, tc.objectType)
			if err == nil {
				t.Fatalf("unexpected success, %s should not be a valid privilege for a %s",
					priv, tc.objectType)
			}
		}
	}
}

// TestSystemPrivilegeValidate exercises validation for system config
// descriptors. We use a dummy system table installed for testing
// purposes.
func TestSystemPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := security.TestUserName()

	id := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[id]; exists {
		t.Fatalf("system table with maximum id %d already exists--is the reserved id space full?", id)
	}
	SystemAllowedPrivileges[id] = privilegepb.List{
		privilegepb.Privilege_SELECT,
		privilegepb.Privilege_GRANT,
	}
	defer delete(SystemAllowedPrivileges, id)

	rootWrongPrivilegesErr := "user root must have exactly SELECT, GRANT " +
		"privileges on (system )?table with ID=.*"
	adminWrongPrivilegesErr := "user admin must have exactly SELECT, GRANT " +
		"privileges on (system )?table with ID=.*"

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT},
			security.AdminRoleName(),
		)
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant(testUser, privilegepb.List{privilegepb.Privilege_SELECT})
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has exactly the allowed privileges.
		descriptor.Grant(testUser, privilegepb.List{privilegepb.Privilege_GRANT})
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Valid: root has exactly the allowed privileges.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT},
			security.AdminRoleName(),
		)

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant(testUser, privilegepb.List{privilegepb.Privilege_GRANT})
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke(
			testUser, privilegepb.List{privilegepb.Privilege_GRANT, privilegepb.Privilege_UPDATE, privilegepb.Privilege_ALL}, privilegepb.Table)
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(security.RootUserName(), privilegepb.List{privilegepb.Privilege_UPDATE})
		if err := descriptor.Validate(id, privilegepb.Table); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(privilegepb.List{privilegepb.Privilege_UPDATE},
			security.AdminRoleName())
		if err := descriptor.Validate(id, privilegepb.Table); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		descriptor.Revoke(security.RootUserName(), privilegepb.List{privilegepb.Privilege_UPDATE}, privilegepb.Table)
		descriptor.Grant(security.RootUserName(), privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT})
		if err := descriptor.Validate(id, privilegepb.Table); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(security.AdminRoleName(), privilegepb.List{privilegepb.Privilege_UPDATE}, privilegepb.Table)
		descriptor.Grant(security.AdminRoleName(), privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT})
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant(testUser, privilegepb.List{privilegepb.Privilege_GRANT})
		if err := descriptor.Validate(id, privilegepb.Table); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFixPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system ID.
	userID := ID(keys.MinUserDescID)
	userPrivs := privilegepb.List{privilegepb.Privilege_ALL}

	// And create an entry for a fake system table.
	systemID := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[systemID]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", systemID)
	}
	systemPrivs := privilegepb.List{
		privilegepb.Privilege_SELECT,
		privilegepb.Privilege_GRANT,
	}
	SystemAllowedPrivileges[systemID] = systemPrivs
	defer delete(SystemAllowedPrivileges, systemID)

	type userPrivileges map[security.SQLUsername]privilegepb.List

	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := security.MakeSQLUsernameFromPreNormalizedString("baz")

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
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
			},
		},
		{
			// Valid requirements for system ID.
			systemID,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				fooUser:                  privilegepb.List{privilegepb.Privilege_SELECT},
				barUser:                  privilegepb.List{privilegepb.Privilege_GRANT},
				bazUser:                  privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT},
			},
			false,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				fooUser:                  privilegepb.List{privilegepb.Privilege_SELECT},
				barUser:                  privilegepb.List{privilegepb.Privilege_GRANT},
				bazUser:                  privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT},
			},
		},
		{
			// Too many privileges for system ID.
			systemID,
			userPrivileges{
				security.RootUserName():  privilegepb.List{privilegepb.Privilege_ALL},
				security.AdminRoleName(): privilegepb.List{privilegepb.Privilege_ALL},
				fooUser:                  privilegepb.List{privilegepb.Privilege_ALL},
				barUser:                  privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_UPDATE},
			},
			true,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				fooUser:                  privilegepb.List{},
				barUser:                  privilegepb.List{privilegepb.Privilege_SELECT},
			},
		},
		{
			// Empty privileges for non-system ID.
			userID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
			},
		},
		{
			// Valid requirements for non-system ID.
			userID,
			userPrivileges{
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
				fooUser:                  privilegepb.List{privilegepb.Privilege_SELECT},
				barUser:                  privilegepb.List{privilegepb.Privilege_GRANT},
				bazUser:                  privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT},
			},
			false,
			userPrivileges{
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
				fooUser:                  privilegepb.List{privilegepb.Privilege_SELECT},
				barUser:                  privilegepb.List{privilegepb.Privilege_GRANT},
				bazUser:                  privilegepb.List{privilegepb.Privilege_SELECT, privilegepb.Privilege_GRANT},
			},
		},
		{
			// All privileges are allowed for non-system ID, but we need super users.
			userID,
			userPrivileges{
				fooUser: privilegepb.List{privilegepb.Privilege_ALL},
				barUser: privilegepb.List{privilegepb.Privilege_UPDATE},
			},
			true,
			userPrivileges{
				security.RootUserName():  privilegepb.List{privilegepb.Privilege_ALL},
				security.AdminRoleName(): privilegepb.List{privilegepb.Privilege_ALL},
				fooUser:                  privilegepb.List{privilegepb.Privilege_ALL},
				barUser:                  privilegepb.List{privilegepb.Privilege_UPDATE},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(u, p)
		}

		if a, e := MaybeFixPrivileges(testCase.id, &desc), testCase.modified; a != e {
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
			if a, e := privilegepb.ListFromBitField(outputUser.Privileges, privilegepb.Any), p; a.ToBitField() != e.ToBitField() {
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
				UserProto:  security.AdminRoleName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  security.RootUserName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		}}
	err := privs.Validate(id, privilegepb.Table)
	if err != nil {
		t.Fatal(err)
	}

	// A privilege descriptor with version OwnerVersion and onwards should
	// have an owner.
	privs = PrivilegeDescriptor{
		Users: []UserPrivileges{
			{
				UserProto:  security.AdminRoleName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  security.RootUserName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		},
		Version: OwnerVersion,
	}

	err = privs.Validate(id, privilegepb.Table)
	if err == nil {
		t.Fatal("unexpected success")
	}

	privs = PrivilegeDescriptor{
		OwnerProto: security.RootUserName().EncodeProto(),
		Users: []UserPrivileges{
			{
				UserProto:  security.AdminRoleName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  security.RootUserName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		},
		Version: OwnerVersion,
	}

	err = privs.Validate(id, privilegepb.Table)
	if err != nil {
		t.Fatal(err)
	}
}
