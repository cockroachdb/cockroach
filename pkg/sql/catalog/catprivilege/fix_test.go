// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catprivilege_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestFixPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system database.
	userDatabaseName := "userdb"
	userPrivs := privilege.List{privilege.ALL}

	// And create an entry for a fake system database.
	systemDatabaseName := "system"
	systemPrivs := privilege.List{
		privilege.CONNECT,
	}

	type userPrivileges map[username.SQLUsername]privilege.List

	fooUser := username.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := username.MakeSQLUsernameFromPreNormalizedString("baz")

	testCases := []struct {
		name     string
		input    userPrivileges
		modified bool
		output   userPrivileges
	}{
		{
			// 0.
			// Empty privileges for system ID.
			systemDatabaseName,
			userPrivileges{},
			true,
			userPrivileges{
				username.RootUserName():  systemPrivs,
				username.AdminRoleName(): systemPrivs,
			},
		},
		{
			// 1.
			// Valid requirements for system ID.
			systemDatabaseName,
			userPrivileges{
				username.RootUserName():  systemPrivs,
				username.AdminRoleName(): systemPrivs,
				barUser:                  privilege.List{privilege.CONNECT},
			},
			false,
			userPrivileges{
				username.RootUserName():  systemPrivs,
				username.AdminRoleName(): systemPrivs,
				barUser:                  privilege.List{privilege.CONNECT},
			},
		},
		{
			// 2.
			// Too many privileges for system ID.
			systemDatabaseName,
			userPrivileges{
				username.RootUserName():  privilege.List{privilege.ALL},
				username.AdminRoleName(): privilege.List{privilege.ALL},
				fooUser:                  privilege.List{privilege.ALL},
				barUser:                  privilege.List{privilege.CONNECT, privilege.UPDATE},
			},
			true,
			userPrivileges{
				username.RootUserName():  systemPrivs,
				username.AdminRoleName(): systemPrivs,
				fooUser:                  privilege.List{},
				barUser:                  privilege.List{privilege.CONNECT},
			},
		},
		{
			// 3.
			// Empty privileges for non-system ID.
			userDatabaseName,
			userPrivileges{},
			true,
			userPrivileges{
				username.RootUserName():  userPrivs,
				username.AdminRoleName(): userPrivs,
			},
		},
		{
			// 4.
			// Valid requirements for non-system ID.
			userDatabaseName,
			userPrivileges{
				username.RootUserName():  userPrivs,
				username.AdminRoleName(): userPrivs,
				barUser:                  privilege.List{},
				bazUser:                  privilege.List{},
			},
			false,
			userPrivileges{
				username.RootUserName():  userPrivs,
				username.AdminRoleName(): userPrivs,
				barUser:                  privilege.List{},
				bazUser:                  privilege.List{},
			},
		},
		{
			// 5.
			// All privileges are allowed for non-system ID, but we need super users.
			userDatabaseName,
			userPrivileges{
				fooUser: privilege.List{privilege.ALL},
			},
			true,
			userPrivileges{
				username.RootUserName():  privilege.List{privilege.ALL},
				username.AdminRoleName(): privilege.List{privilege.ALL},
				fooUser:                  privilege.List{privilege.ALL},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &catpb.PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(u, p, false /* withGrantOption */)
		}

		if _, err := catprivilege.MaybeFixPrivileges(
			&desc,
			descpb.InvalidID,
			descpb.InvalidID,
			privilege.Database,
			testCase.name,
		); err != nil {
			t.Fatal(err)
		}

		if a, e := len(desc.Users), len(testCase.output); a != e {
			t.Errorf("#%d: expected %d users (%v), got %d (%v)", num, e, testCase.output, a, desc.Users)
			continue
		}

		for u, expected := range testCase.output {
			outputUser, ok := desc.FindUser(u)
			if !ok {
				t.Fatalf("#%d: expected user %s in output, but not found (%v)", num, u, desc.Users)
			}
			actual, err := privilege.ListFromBitField(outputUser.Privileges, privilege.Any)
			if err != nil {
				t.Fatal(err)
			}
			if actual.ToBitField() != expected.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v", num, u, expected, actual)
			}
		}
	}
}

// TestMaybeFixSchemaPrivileges ensures that invalid privileges are removed
// from a schema's privilege descriptor.
func TestMaybeFixSchemaPrivileges(t *testing.T) {
	fooUser := username.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")

	type userPrivileges map[username.SQLUsername]privilege.List

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
					privilege.CREATE,
					privilege.USAGE,
				},
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
		desc := &catpb.PrivilegeDescriptor{}
		for u, p := range tc.input {
			desc.Grant(u, p, false /* withGrantOption */)
		}
		testParentID := descpb.ID(bootstrap.TestingMinNonDefaultUserDescID())
		if _, err := catprivilege.MaybeFixPrivileges(&desc,
			testParentID,
			descpb.InvalidID,
			privilege.Schema,
			"whatever",
		); err != nil {
			t.Fatal(err)
		}

		for u, expected := range tc.output {
			outputUser, ok := desc.FindUser(u)
			if !ok {
				t.Errorf("#%d: expected user %s in output, but not found (%v)",
					num, u, desc.Users,
				)
			}
			actual, err := privilege.ListFromBitField(outputUser.Privileges, privilege.Any)
			if err != nil {
				t.Fatal(err)
			}
			if actual.ToBitField() != expected.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v",
					num, u, expected, actual,
				)
			}

			if err := privilege.ValidatePrivileges(expected, privilege.Schema); err != nil {
				t.Errorf("%s\n", err.Error())
			}
		}

	}
}
