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

	id := ID(keys.MinUserDescID)
	descriptor := NewDefaultPrivilegeDescriptor(security.AdminRoleName())
	if err := descriptor.Validate(id, privilege.Table); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(testUser, privilege.List{privilege.ALL})
	if err := descriptor.Validate(id, privilege.Table); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(security.RootUserName(), privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id, privilege.Table); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke(security.RootUserName(), privilege.List{privilege.SELECT},
		privilege.Table)
	if err := descriptor.Validate(id, privilege.Table); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(security.RootUserName(), privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id, privilege.Table); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke(security.RootUserName(), privilege.List{privilege.ALL}, privilege.Table)
	if err := descriptor.Validate(id, privilege.Table); err == nil {
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
			err := privDesc.Validate(id, tc.objectType)
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
	SystemAllowedPrivileges[id] = privilege.List{
		privilege.SELECT,
		privilege.GRANT,
	}
	defer delete(SystemAllowedPrivileges, id)

	rootWrongPrivilegesErr := "user root must have exactly SELECT, GRANT " +
		"privileges on (system )?table with ID=.*"
	adminWrongPrivilegesErr := "user admin must have exactly SELECT, GRANT " +
		"privileges on (system )?table with ID=.*"

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
			security.AdminRoleName(),
		)
		if err := descriptor.Validate(id, privilege.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant(testUser, privilege.List{privilege.SELECT})
		if err := descriptor.Validate(id, privilege.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has exactly the allowed privileges.
		descriptor.Grant(testUser, privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id, privilege.Table); err != nil {
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
		if err := descriptor.Validate(id, privilege.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke(
			testUser, privilege.List{privilege.GRANT, privilege.UPDATE, privilege.ALL}, privilege.Table)
		if err := descriptor.Validate(id, privilege.Table); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(security.RootUserName(), privilege.List{privilege.UPDATE})
		if err := descriptor.Validate(id, privilege.Table); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(privilege.List{privilege.UPDATE},
			security.AdminRoleName())
		if err := descriptor.Validate(id, privilege.Table); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		descriptor.Revoke(security.RootUserName(), privilege.List{privilege.UPDATE}, privilege.Table)
		descriptor.Grant(security.RootUserName(), privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id, privilege.Table); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(security.AdminRoleName(), privilege.List{privilege.UPDATE}, privilege.Table)
		descriptor.Grant(security.AdminRoleName(), privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id, privilege.Table); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant(testUser, privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id, privilege.Table); err != nil {
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

	type userPrivileges map[security.SQLUsername]privilege.List

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
				fooUser:                  privilege.List{privilege.SELECT},
				barUser:                  privilege.List{privilege.GRANT},
				bazUser:                  privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				fooUser:                  privilege.List{privilege.SELECT},
				barUser:                  privilege.List{privilege.GRANT},
				bazUser:                  privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// Too many privileges for system ID.
			systemID,
			userPrivileges{
				security.RootUserName():  privilege.List{privilege.ALL},
				security.AdminRoleName(): privilege.List{privilege.ALL},
				fooUser:                  privilege.List{privilege.ALL},
				barUser:                  privilege.List{privilege.SELECT, privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				fooUser:                  privilege.List{},
				barUser:                  privilege.List{privilege.SELECT},
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
				fooUser:                  privilege.List{privilege.SELECT},
				barUser:                  privilege.List{privilege.GRANT},
				bazUser:                  privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
				fooUser:                  privilege.List{privilege.SELECT},
				barUser:                  privilege.List{privilege.GRANT},
				bazUser:                  privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// All privileges are allowed for non-system ID, but we need super users.
			userID,
			userPrivileges{
				fooUser: privilege.List{privilege.ALL},
				barUser: privilege.List{privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUserName():  privilege.List{privilege.ALL},
				security.AdminRoleName(): privilege.List{privilege.ALL},
				fooUser:                  privilege.List{privilege.ALL},
				barUser:                  privilege.List{privilege.UPDATE},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(u, p)
		}

		MaybeFixPrivileges(testCase.id, testCase.id, &desc, privilege.Any)

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
				UserProto:  security.AdminRoleName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  security.RootUserName().EncodeProto(),
				Privileges: DefaultSuperuserPrivileges.ToBitField(),
			},
		}}
	err := privs.Validate(id, privilege.Table)
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

	err = privs.Validate(id, privilege.Table)
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

	err = privs.Validate(id, privilege.Table)
	if err != nil {
		t.Fatal(err)
	}
}

// TestMaybeFixUsageAndZoneConfigPrivilege checks that Table and DB descriptors
// on created on v20.1 or prior (PrivilegeDescVersion InitialVersion) with
// USAGE privilege have its privilege correctly updated.
// The bit representing USAGE privilege in 20.2 for Tables/DBs should actually
// be ZONECONFIG privilege and should be updated.
func TestMaybeFixUsageAndZoneConfigPrivilege(t *testing.T) {

	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := security.MakeSQLUsernameFromPreNormalizedString("baz")

	type userPrivileges map[security.SQLUsername]privilege.List

	testCases := []struct {
		input           userPrivileges
		modified        bool
		output          userPrivileges
		objectType      privilege.ObjectType
		privDescVersion PrivilegeDescVersion
		description     string
		isValid         bool
	}{
		// Cases for Tables and Databases.
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			privilege.Table,
			InitialVersion,
			"A privilege descriptor from a table created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			privilege.Database,
			InitialVersion,
			"A privilege descriptor from a database created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.ALL},
			},
			false,
			userPrivileges{
				fooUser: privilege.List{privilege.ALL},
			},
			privilege.Table,
			InitialVersion,
			"ALL should stay as ALL",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			false,
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			privilege.Table,
			OwnerVersion,
			"A privilege descriptor from a table created in v20.2 onwards " +
				"(OwnerVersion) should not be modified.",
			false,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			false,
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			privilege.Database,
			OwnerVersion,
			"A privilege descriptor from a Database created in v20.2 onwards " +
				"(OwnerVersion) should not be modified.",
			false,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			false,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			privilege.Table,
			OwnerVersion,
			"A privilege descriptor from a table created in v20.2 onwards " +
				"(OwnerVersion) should not be modified.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			false,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			privilege.Database,
			OwnerVersion,
			"A privilege descriptor from a Database created in v20.2 onwards " +
				"(OwnerVersion) should not be modified.",
			true,
		},
		// Fix privileges for multiple users.
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
				barUser: privilege.List{privilege.USAGE, privilege.CREATE, privilege.SELECT},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
				barUser: privilege.List{privilege.ZONECONFIG, privilege.CREATE, privilege.SELECT},
			},
			privilege.Table,
			InitialVersion,
			"A privilege descriptor from a table created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
				barUser: privilege.List{privilege.USAGE, privilege.CREATE, privilege.SELECT},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
				barUser: privilege.List{privilege.ZONECONFIG, privilege.CREATE, privilege.SELECT},
			},
			privilege.Database,
			InitialVersion,
			"A privilege descriptor from a table created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
				barUser: privilege.List{privilege.USAGE, privilege.CREATE, privilege.GRANT},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
				barUser: privilege.List{privilege.ZONECONFIG, privilege.CREATE, privilege.GRANT},
			},
			privilege.Database,
			InitialVersion,
			"A privilege descriptor from a database created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
				barUser: privilege.List{privilege.USAGE, privilege.CREATE, privilege.SELECT},
				bazUser: privilege.List{privilege.ALL, privilege.USAGE},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
				barUser: privilege.List{privilege.ZONECONFIG, privilege.CREATE, privilege.SELECT},
				bazUser: privilege.List{privilege.ALL},
			},
			privilege.Database,
			InitialVersion,
			"A privilege descriptor from a table created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		// Test case where the privilege descriptor has ZONECONFIG and USAGE.
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE, privilege.ZONECONFIG},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
			},
			privilege.Table,
			InitialVersion,
			"If the descriptor has USAGE and ZONECONFIG, it should become just " +
				"ZONECONFIG",
			true,
		},
	}

	for num, tc := range testCases {
		desc := &PrivilegeDescriptor{Version: tc.privDescVersion}
		for u, p := range tc.input {
			desc.Grant(u, p)
		}
		modified := MaybeFixUsagePrivForTablesAndDBs(&desc)

		if tc.modified != modified {
			t.Errorf("expected modifed to be %v, was %v", tc.modified, modified)
		}

		for u, p := range tc.output {
			outputUser, ok := desc.findUser(u)
			if !ok {
				t.Errorf("#%d: expected user %s in output, but not found (%v)\n%s",
					num, u, desc.Users, tc.description,
				)
			}
			if a, e := privilege.ListFromBitField(outputUser.Privileges, privilege.Any), p; a.ToBitField() != e.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v\n%s",
					num, u, e, a, tc.description,
				)
			}

			err := privilege.ValidatePrivileges(p, tc.objectType)
			if tc.isValid && err != nil {
				t.Errorf("%s\n%s", err.Error(), tc.description)
			}
		}

	}

}

// TestMaybeFixSchemaPrivileges ensures that invalid privileges are removed
// from a schema's privilege descriptor.
func TestMaybeFixSchemaPrivileges(t *testing.T) {
	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")

	type userPrivileges map[security.SQLUsername]privilege.List

	testCases := []struct {
		input  userPrivileges
		output userPrivileges
	}{
		{
			userPrivileges{
				fooUser: privilege.List{
					privilege.ALL,
					privilege.CONNECT,
					privilege.CREATE,
					privilege.DROP,
					privilege.GRANT,
					privilege.SELECT,
					privilege.INSERT,
					privilege.DELETE,
					privilege.UPDATE,
					privilege.USAGE,
					privilege.ZONECONFIG,
				},
				barUser: privilege.List{
					privilege.CONNECT,
					privilege.CREATE,
					privilege.DROP,
					privilege.GRANT,
					privilege.SELECT,
					privilege.INSERT,
					privilege.DELETE,
					privilege.UPDATE,
					privilege.USAGE,
					privilege.ZONECONFIG,
				},
			},
			userPrivileges{
				fooUser: privilege.List{privilege.ALL},
				barUser: privilege.List{
					privilege.GRANT,
					privilege.CREATE,
					privilege.USAGE,
				},
			},
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.GRANT},
			},
			userPrivileges{
				fooUser: privilege.List{privilege.GRANT},
			},
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.CREATE},
			},
			userPrivileges{
				fooUser: privilege.List{privilege.CREATE},
			},
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
			},
		},
	}

	for num, tc := range testCases {
		desc := &PrivilegeDescriptor{}
		for u, p := range tc.input {
			desc.Grant(u, p)
		}
		testID := ID(keys.MaxReservedDescID + 1)
		MaybeFixPrivileges(testID, testID, &desc, privilege.Schema)

		for u, p := range tc.output {
			outputUser, ok := desc.findUser(u)
			if !ok {
				t.Errorf("#%d: expected user %s in output, but not found (%v)",
					num, u, desc.Users,
				)
			}
			if a, e := privilege.ListFromBitField(outputUser.Privileges, privilege.Any), p; a.ToBitField() != e.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v",
					num, u, e, a,
				)
			}

			err := privilege.ValidatePrivileges(p, privilege.Schema)
			if err != nil {
				t.Errorf("%s\n", err.Error())
			}
		}

	}
}
