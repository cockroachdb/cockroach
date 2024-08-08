// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	descriptor := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())

	testUser := username.TestUserName()
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		grantee       username.SQLUsername // User to grant/revoke privileges on.
		grant, revoke privilege.List
		show          []catpb.UserPrivilege
		objectType    privilege.ObjectType
	}{
		{username.SQLUsername{}, nil, nil,
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Table,
		},
		{username.RootUserName(), privilege.List{privilege.ALL}, nil,
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Table,
		},
		{username.RootUserName(), privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Table,
		},
		{testUser, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: testUser, Privileges: []privilege.Privilege{{Kind: privilege.DROP}, {Kind: privilege.INSERT}}},
			},
			privilege.Table,
		},
		{barUser, nil, privilege.List{privilege.INSERT, privilege.ALL},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: testUser, Privileges: []privilege.Privilege{{Kind: privilege.DROP}, {Kind: privilege.INSERT}}},
			},
			privilege.Table,
		},
		{testUser, privilege.List{privilege.ALL}, nil,
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: testUser, Privileges: []privilege.Privilege{{Kind: privilege.ALL}}},
			},
			privilege.Table,
		},
		{testUser, nil, privilege.List{privilege.SELECT, privilege.INSERT},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: testUser, Privileges: []privilege.Privilege{
					{Kind: privilege.BACKUP},
					{Kind: privilege.CHANGEFEED},
					{Kind: privilege.CREATE},
					{Kind: privilege.DELETE},
					{Kind: privilege.DROP},
					{Kind: privilege.TRIGGER},
					{Kind: privilege.UPDATE},
					{Kind: privilege.ZONECONFIG},
				}},
			},
			privilege.Table,
		},
		{testUser, nil, privilege.List{privilege.ALL},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
				{User: username.RootUserName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Table,
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{username.RootUserName(), nil, privilege.List{privilege.ALL},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Table,
		},
		// Ensure revoking USAGE from a user with ALL privilege on a type
		// leaves the user with no privileges.
		{testUser, privilege.List{privilege.ALL}, privilege.List{privilege.USAGE},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Type,
		},
		// Ensure revoking USAGE from a user with ALL privilege on a type
		// leaves the user with no privileges.
		{testUser,
			privilege.List{privilege.ALL}, privilege.List{privilege.USAGE},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Type,
		},
		// Ensure revoking BACKUP, CHANGEFEED, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG
		// from a user with ALL privilege on a table leaves the user with no privileges.
		{testUser,
			privilege.List{privilege.ALL},
			privilege.List{privilege.BACKUP, privilege.CHANGEFEED, privilege.CREATE, privilege.DROP, privilege.SELECT, privilege.INSERT,
				privilege.DELETE, privilege.TRIGGER, privilege.UPDATE, privilege.ZONECONFIG},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Table,
		},
		// Ensure revoking BACKUP, CONNECT, CREATE, DROP, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG, RESTORE
		// from a user with ALL privilege on a database leaves the user with no privileges.
		{testUser,
			privilege.List{privilege.ALL},
			privilege.List{privilege.BACKUP, privilege.CONNECT, privilege.CREATE, privilege.DROP, privilege.SELECT,
				privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG, privilege.RESTORE},
			[]catpb.UserPrivilege{
				{User: username.AdminRoleName(), Privileges: []privilege.Privilege{{Kind: privilege.ALL, GrantOption: true}}},
			},
			privilege.Database,
		},
	}

	for tcNum, tc := range testCases {
		if !tc.grantee.Undefined() {
			if tc.grant != nil {
				descriptor.Grant(tc.grantee, tc.grant, false)
			}
			if tc.revoke != nil {
				if err := descriptor.Revoke(tc.grantee, tc.revoke, tc.objectType, false); err != nil {
					t.Fatal(err)
				}
			}
		}
		show, err := descriptor.Show(tc.objectType, true /* showImplicitOwnerPrivs */)
		if err != nil {
			t.Fatal(err)
		}
		if len(show) != len(tc.show) {
			t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
				tcNum, descriptor, show, tc.show)
		}
		for i := 0; i < len(show); i++ {
			if !reflect.DeepEqual(show[i], tc.show[i]) {
				t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
					tcNum, descriptor, show, tc.show)
			}
		}
	}
}

func TestCheckPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := username.TestUserName()
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		pd   *catpb.PrivilegeDescriptor
		user username.SQLUsername
		priv privilege.Kind
		exp  bool
	}{
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.CREATE, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			barUser, privilege.CREATE, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			barUser, privilege.DROP, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.DROP, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.CREATE, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.ALL, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.ALL, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.ALL, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.CREATE, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP}, privilege.List{},
			username.AdminRoleName()),
			testUser, privilege.UPDATE, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP}, privilege.List{},
			username.AdminRoleName()),
			testUser, privilege.DROP, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.ALL}, privilege.List{},
			username.AdminRoleName()),
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

	testUser := username.TestUserName()
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")

	testCases := []struct {
		pd   *catpb.PrivilegeDescriptor
		user username.SQLUsername
		exp  bool
	}{
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			testUser, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			barUser, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{}, username.AdminRoleName()),
			testUser, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{}, privilege.List{}, username.AdminRoleName()),
			testUser, false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP}, privilege.List{},
			username.AdminRoleName()),
			testUser, true},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.DROP}, privilege.List{},
			username.AdminRoleName()),
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

	testUser := username.TestUserName()

	descriptor := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
	validate := func() error {
		id := catid.DescID(bootstrap.TestingMinUserDescID())
		return descriptor.Validate(id, privilege.Table, "whatever", catpb.DefaultSuperuserPrivileges)
	}

	if err := validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(testUser, privilege.List{privilege.ALL}, false)
	if err := validate(); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(username.RootUserName(), privilege.List{privilege.SELECT}, false)
	if err := validate(); err != nil {
		t.Fatal(err)
	}
	if err := descriptor.Revoke(username.RootUserName(), privilege.List{privilege.SELECT}, privilege.Table, false); err != nil {
		t.Fatal(err)
	}
	if err := validate(); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(username.RootUserName(), privilege.List{privilege.SELECT}, false)
	if err := validate(); err == nil {
		t.Fatal("unexpected success")
	}
	if err := descriptor.Revoke(username.RootUserName(), privilege.List{privilege.ALL}, privilege.Table, false); err != nil {
		t.Fatal(err)
	}
	if err := validate(); err == nil {
		t.Fatal("unexpected success")
	}
}

func TestValidPrivilegesForObjects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	id := catid.DescID(bootstrap.TestingMinUserDescID())

	testUser := username.TestUserName()

	testCases := []struct {
		objectType      privilege.ObjectType
		validPrivileges privilege.List
	}{
		{privilege.Table, privilege.TablePrivileges},
		{privilege.Database, privilege.DBPrivileges},
		{privilege.Schema, privilege.SchemaPrivileges},
		{privilege.Type, privilege.TypePrivileges},
		{privilege.Sequence, privilege.SequencePrivileges},
	}

	for _, tc := range testCases {
		for _, priv := range tc.validPrivileges {
			privDesc := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
			privDesc.Grant(testUser, privilege.List{priv}, false)
			err := privDesc.Validate(id, tc.objectType, "whatever", catpb.DefaultSuperuserPrivileges)
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
			privDesc := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
			privDesc.Grant(testUser, privilege.List{priv}, false)
			err := privDesc.Validate(id, tc.objectType, "whatever", catpb.DefaultSuperuserPrivileges)
			if err == nil {
				t.Fatalf("unexpected success, %s should not be a valid privilege for a %s",
					priv.DisplayName(), tc.objectType)
			}
		}
	}
}

func TestSystemPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := username.TestUserName()

	validate := func(descriptor *catpb.PrivilegeDescriptor) error {
		return descriptor.Validate(keys.SystemDatabaseID, privilege.Table, "whatever", privilege.ReadData)
	}

	rootWrongPrivilegesErr := "user root must have exactly \\[SELECT\\] " +
		`privileges on (system )?table "whatever"`
	adminWrongPrivilegesErr := "user admin must have exactly \\[SELECT\\] " +
		`privileges on (system )?table "whatever"`

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := catpb.NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT},
			username.AdminRoleName(),
		)
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has exactly the allowed privileges.
		descriptor.Grant(testUser, privilege.List{privilege.SELECT}, false)
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Valid: root has exactly the allowed privileges.
		descriptor := catpb.NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT},
			username.AdminRoleName(),
		)

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		if err := descriptor.Revoke(
			testUser, privilege.List{privilege.UPDATE, privilege.ALL}, privilege.Table, false,
		); err != nil {
			t.Fatal(err)
		}
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(username.RootUserName(), privilege.List{privilege.UPDATE}, false)
		if err := validate(descriptor); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := catpb.NewCustomSuperuserPrivilegeDescriptor(privilege.List{privilege.UPDATE},
			username.AdminRoleName())
		if err := validate(descriptor); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		if err := descriptor.Revoke(
			username.RootUserName(), privilege.List{privilege.UPDATE}, privilege.Table, false,
		); err != nil {
			t.Fatal(err)
		}
		descriptor.Grant(username.RootUserName(), privilege.List{privilege.SELECT}, false)
		if err := validate(descriptor); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		if err := descriptor.Revoke(
			username.AdminRoleName(), privilege.List{privilege.UPDATE}, privilege.Table, false,
		); err != nil {
			t.Fatal(err)
		}
		descriptor.Grant(username.AdminRoleName(), privilege.List{privilege.SELECT}, false)
		if err := validate(descriptor); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateOwnership(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system id.
	id := catid.DescID(bootstrap.TestingMinUserDescID())
	validate := func(privs catpb.PrivilegeDescriptor) error {
		return privs.Validate(id, privilege.Table, "whatever", catpb.DefaultSuperuserPrivileges)
	}

	// A privilege descriptor with a version before OwnerVersion can have
	// no owner.
	privs := catpb.PrivilegeDescriptor{
		Users: []catpb.UserPrivileges{
			{
				UserProto:  username.AdminRoleName().EncodeProto(),
				Privileges: catpb.DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  username.RootUserName().EncodeProto(),
				Privileges: catpb.DefaultSuperuserPrivileges.ToBitField(),
			},
		}}
	err := validate(privs)
	if err != nil {
		t.Fatal(err)
	}

	// A privilege descriptor with version OwnerVersion and onwards should
	// have an owner.
	privs = catpb.PrivilegeDescriptor{
		Users: []catpb.UserPrivileges{
			{
				UserProto:  username.AdminRoleName().EncodeProto(),
				Privileges: catpb.DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  username.RootUserName().EncodeProto(),
				Privileges: catpb.DefaultSuperuserPrivileges.ToBitField(),
			},
		},
		Version: catpb.OwnerVersion,
	}

	err = validate(privs)
	if err == nil {
		t.Fatal("unexpected success")
	}

	privs = catpb.PrivilegeDescriptor{
		OwnerProto: username.RootUserName().EncodeProto(),
		Users: []catpb.UserPrivileges{
			{
				UserProto:  username.AdminRoleName().EncodeProto(),
				Privileges: catpb.DefaultSuperuserPrivileges.ToBitField(),
			},
			{
				UserProto:  username.RootUserName().EncodeProto(),
				Privileges: catpb.DefaultSuperuserPrivileges.ToBitField(),
			},
		},
		Version: catpb.OwnerVersion,
	}

	err = validate(privs)
	if err != nil {
		t.Fatal(err)
	}
}

// TestGrantWithGrantOption tests whether granting with grant option changes the
// privilege bits and grant option bits as expected.
func TestGrantWithGrantOption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := username.TestUserName()

	testCases := []struct {
		pd                  *catpb.PrivilegeDescriptor
		user                username.SQLUsername
		objectType          privilege.ObjectType
		grantPrivileges     privilege.List
		expectedPrivileges  privilege.List
		expectedGrantOption privilege.List
	}{
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.ALL, privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.INSERT}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{privilege.ALL}, username.AdminRoleName()),
			testUser, privilege.Schema,
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, username.AdminRoleName()),
			testUser, privilege.Schema,
			privilege.List{privilege.ALL, privilege.CREATE},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
	}

	for tcNum, tc := range testCases {
		tc.pd.Grant(tc.user, tc.grantPrivileges, true)
		if tc.pd.Users[0].Privileges != tc.expectedPrivileges.ToBitField() {
			actualPrivs, err := privilege.ListFromBitField(tc.pd.Users[0].Privileges, tc.objectType)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("#%d: Incorrect privileges, returned %v, expected %v",
				tcNum, actualPrivs, tc.expectedPrivileges)
		}
		if tc.pd.Users[0].WithGrantOption != tc.expectedGrantOption.ToBitField() {
			actualGrantOptions, err := privilege.ListFromBitField(tc.pd.Users[0].WithGrantOption, tc.objectType)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("#%d: Incorrect grant option, returned %v, expected %v",
				tcNum, actualGrantOptions, tc.expectedGrantOption)
		}
	}
}

// TestRevokeWithGrantOption tests whether revoking grant option for changes the
// privilege bits and grant option bits as expected.
func TestRevokeWithGrantOption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := username.TestUserName()

	testCases := []struct {
		pd                  *catpb.PrivilegeDescriptor
		user                username.SQLUsername
		objectType          privilege.ObjectType
		grantOptionFor      bool
		revokePrivileges    privilege.List
		expectedPrivileges  privilege.List
		expectedGrantOption privilege.List
		shouldBeEmpty       bool
	}{
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.Table,
			true,
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{privilege.ALL}, username.AdminRoleName()),
			testUser, privilege.Table,
			true,
			privilege.List{privilege.CREATE},
			privilege.List{privilege.ALL},
			privilege.List{privilege.BACKUP, privilege.CHANGEFEED, privilege.DROP, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.TRIGGER, privilege.UPDATE, privilege.ZONECONFIG},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{privilege.ALL}, username.AdminRoleName()),
			testUser, privilege.Table,
			true,
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{privilege.ALL}, username.AdminRoleName()),
			testUser, privilege.Table,
			true,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.SELECT},
			privilege.List{privilege.ALL},
			privilege.List{},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT}, privilege.List{privilege.SELECT}, username.AdminRoleName()),
			testUser, privilege.Table,
			true,
			privilege.List{privilege.CREATE, privilege.DROP, privilege.SELECT},
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT},
			privilege.List{},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT}, privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT}, username.AdminRoleName()),
			testUser, privilege.Table,
			false,
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.CREATE},
			privilege.List{privilege.CREATE},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.ALL}, privilege.List{privilege.ALL}, username.AdminRoleName()),
			testUser, privilege.Table,
			false,
			privilege.List{privilege.CREATE},
			privilege.List{privilege.BACKUP, privilege.CHANGEFEED, privilege.DROP, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.TRIGGER, privilege.UPDATE, privilege.ZONECONFIG},
			privilege.List{privilege.BACKUP, privilege.CHANGEFEED, privilege.DROP, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.TRIGGER, privilege.UPDATE, privilege.ZONECONFIG},
			false},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.SELECT, privilege.INSERT}, privilege.List{privilege.INSERT}, username.AdminRoleName()),
			testUser, privilege.Table,
			false,
			privilege.List{privilege.ALL},
			privilege.List{},
			privilege.List{},
			true},
	}

	for tcNum, tc := range testCases {
		if err := tc.pd.Revoke(tc.user, tc.revokePrivileges, tc.objectType, tc.grantOptionFor); err != nil {
			t.Error(err)
		}
		if tc.shouldBeEmpty {
			if len(tc.pd.Users) == 0 {
				continue
			}
			t.Errorf("#%d: Descriptor exists when it should be deleted",
				tcNum)
		}
		if tc.pd.Users[0].Privileges != tc.expectedPrivileges.ToBitField() {
			actualPrivs, err := privilege.ListFromBitField(tc.pd.Users[0].Privileges, tc.objectType)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("#%d: Incorrect privileges, returned %v, expected %v",
				tcNum, actualPrivs, tc.expectedPrivileges)
		}
		if tc.pd.Users[0].WithGrantOption != tc.expectedGrantOption.ToBitField() {
			actualGrantOption, err := privilege.ListFromBitField(tc.pd.Users[0].WithGrantOption, tc.objectType)
			if err != nil {
				t.Fatal(err)
			}
			t.Errorf("#%d: Incorrect grant option, returned %v, expected %v",
				tcNum, actualGrantOption, tc.expectedGrantOption)
		}
	}
}
