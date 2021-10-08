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
	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRoleName())

	testUser := security.TestUserName()
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		grantee       security.SQLUsername // User to grant/revoke privileges on.
		grant, revoke privilege.List
		show          []UserPrivilegeString
		objectType    privilege.ObjectType
	}{
		{security.SQLUsername{}, nil, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilege.Table,
		},
		{security.RootUserName(), privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilege.Table,
		},
		{security.RootUserName(), privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilege.Table,
		},
		{testUser, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"DROP", "INSERT"}},
			},
			privilege.Table,
		},
		{barUser, nil, privilege.List{privilege.INSERT, privilege.ALL},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"DROP", "INSERT"}},
			},
			privilege.Table,
		},
		{testUser, privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"ALL"}},
			},
			privilege.Table,
		},
		{testUser, nil, privilege.List{privilege.SELECT, privilege.INSERT},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
				{testUser, []string{"CREATE", "DELETE", "DROP", "GRANT", "UPDATE", "ZONECONFIG"}},
			},
			privilege.Table,
		},
		{testUser, nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{security.RootUserName(), []string{"ALL"}},
			},
			privilege.Table,
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{security.RootUserName(), nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilege.Table,
		},
		// Ensure revoking USAGE from a user with ALL privilege on a type
		// leaves the user with only GRANT privilege.
		{testUser, privilege.List{privilege.ALL}, privilege.List{privilege.USAGE},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
				{testUser, []string{"GRANT"}},
			},
			privilege.Type,
		},
		// Ensure revoking USAGE, GRANT from a user with ALL privilege on a type
		// leaves the user with no privileges.
		{testUser,
			privilege.List{privilege.ALL}, privilege.List{privilege.USAGE, privilege.GRANT},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilege.Type,
		},
		// Ensure revoking CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG
		// from a user with ALL privilege on a table leaves the user with no privileges.
		{testUser,
			privilege.List{privilege.ALL}, privilege.List{privilege.CREATE, privilege.DROP,
				privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE,
				privilege.ZONECONFIG},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilege.Table,
		},
		// Ensure revoking CONNECT, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG
		// from a user with ALL privilege on a database leaves the user with no privileges.
		{testUser,
			privilege.List{privilege.ALL}, privilege.List{privilege.CONNECT, privilege.CREATE,
				privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE,
				privilege.UPDATE, privilege.ZONECONFIG},
			[]UserPrivilegeString{
				{security.AdminRoleName(), []string{"ALL"}},
			},
			privilege.Database,
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
		priv privilege.Kind
		exp  bool
	}{
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.CREATE, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			barUser, privilege.CREATE, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			barUser, privilege.DROP, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.DROP, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, security.AdminRoleName()),
			testUser, privilege.CREATE, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.ALL, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, security.AdminRoleName()),
			testUser, privilege.ALL, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{}, security.AdminRoleName()),
			testUser, privilege.ALL, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{}, security.AdminRoleName()),
			testUser, privilege.CREATE, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRoleName()),
			testUser, privilege.UPDATE, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRoleName()),
			testUser, privilege.DROP, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.ALL},
			security.AdminRoleName()),
			testUser, privilege.DROP, true},
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
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			barUser, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, security.AdminRoleName()),
			testUser, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{}, security.AdminRoleName()),
			testUser, false},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP},
			security.AdminRoleName()),
			testUser, true},
		{NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP},
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

	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
	validate := func() error {
		return descriptor.Validate(ID(keys.MinUserDescID), privilege.Table, "whatever", DefaultSuperuserPrivileges)
	}

	if err := validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(testUser, privilege.List{privilege.ALL})
	if err := validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(security.RootUserName(), privilege.List{privilege.SELECT})
	if err := validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke(security.RootUserName(), privilege.List{privilege.SELECT}, privilege.Table)
	if err := validate(); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(security.RootUserName(), privilege.List{privilege.SELECT})
	if err := validate(); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke(security.RootUserName(), privilege.List{privilege.ALL}, privilege.Table)
	if err := validate(); err == nil {
		t.Fatal("unexpected success")
	}
}

func TestValidPrivilegesForObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	id := ID(keys.MinUserDescID)

	testUser := security.TestUserName()

	testCases := []struct {
		objectType      privilege.ObjectType
		validPrivileges privilege.List
	}{
		{privilege.Table, privilege.TablePrivileges},
		{privilege.Database, privilege.DBPrivileges},
		{privilege.Schema, privilege.SchemaPrivileges},
		{privilege.Type, privilege.TypePrivileges},
	}

	for _, tc := range testCases {
		for _, priv := range tc.validPrivileges {
			privDesc := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			privDesc.Grant(testUser, privilege.List{priv})
			err := privDesc.Validate(id, tc.objectType, "whatever", DefaultSuperuserPrivileges)
			if err != nil {
				t.Fatal(err)
			}
		}

		// Derive invalidPrivileges from the validPrivileges.
		invalidPrivileges := privilege.List{}
		for _, priv := range privilege.AllPrivileges {
			if priv.Mask() & ^tc.validPrivileges.ToBitField() != 0 {
				invalidPrivileges = append(invalidPrivileges, priv)
			}
		}

		for _, priv := range invalidPrivileges {
			privDesc := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
			privDesc.Grant(testUser, privilege.List{priv})
			err := privDesc.Validate(id, tc.objectType, "whatever", DefaultSuperuserPrivileges)
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

	validate := func(descriptor *PrivilegeDescriptor) error {
		return descriptor.Validate(keys.SystemDatabaseID, privilege.Table, "whatever", privilege.ReadData)
	}

	rootWrongPrivilegesErr := "user root must have exactly GRANT, SELECT " +
		`privileges on (system )?table "whatever"`
	adminWrongPrivilegesErr := "user admin must have exactly GRANT, SELECT " +
		`privileges on (system )?table "whatever"`

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
			security.AdminRoleName(),
		)
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant(testUser, privilege.List{privilege.SELECT})
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has exactly the allowed privileges.
		descriptor.Grant(testUser, privilege.List{privilege.GRANT})
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Valid: root has exactly the allowed privileges.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
			security.AdminRoleName(),
		)

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant(testUser, privilege.List{privilege.GRANT})
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke(
			testUser, privilege.List{privilege.GRANT, privilege.UPDATE, privilege.ALL}, privilege.Table)
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(security.RootUserName(), privilege.List{privilege.UPDATE})
		if err := validate(descriptor); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(privilege.List{privilege.UPDATE},
			security.AdminRoleName())
		if err := validate(descriptor); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		descriptor.Revoke(security.RootUserName(), privilege.List{privilege.UPDATE}, privilege.Table)
		descriptor.Grant(security.RootUserName(), privilege.List{privilege.SELECT, privilege.GRANT})
		if err := validate(descriptor); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(security.AdminRoleName(), privilege.List{privilege.UPDATE}, privilege.Table)
		descriptor.Grant(security.AdminRoleName(), privilege.List{privilege.SELECT, privilege.GRANT})
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant(testUser, privilege.List{privilege.GRANT})
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateOwnership(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system id.
	id := ID(keys.MinUserDescID)
	validate := func(privs PrivilegeDescriptor) error {
		return privs.Validate(id, privilege.Table, "whatever", DefaultSuperuserPrivileges)
	}

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
	err := validate(privs)
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

	err = validate(privs)
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

	err = validate(privs)
	if err != nil {
		t.Fatal(err)
	}
}
