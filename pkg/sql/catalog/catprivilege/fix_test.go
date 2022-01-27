// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catprivilege_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
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

	type userPrivileges map[security.SQLUsername]privilege.List

	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := security.MakeSQLUsernameFromPreNormalizedString("baz")

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
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
			},
		},
		{
			// 1.
			// Valid requirements for system ID.
			systemDatabaseName,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				barUser:                  privilege.List{privilege.CONNECT},
			},
			false,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
				barUser:                  privilege.List{privilege.CONNECT},
			},
		},
		{
			// 2.
			// Too many privileges for system ID.
			systemDatabaseName,
			userPrivileges{
				security.RootUserName():  privilege.List{privilege.ALL},
				security.AdminRoleName(): privilege.List{privilege.ALL},
				fooUser:                  privilege.List{privilege.ALL},
				barUser:                  privilege.List{privilege.CONNECT, privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUserName():  systemPrivs,
				security.AdminRoleName(): systemPrivs,
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
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
			},
		},
		{
			// 4.
			// Valid requirements for non-system ID.
			userDatabaseName,
			userPrivileges{
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
				barUser:                  privilege.List{privilege.GRANT},
				bazUser:                  privilege.List{privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUserName():  userPrivs,
				security.AdminRoleName(): userPrivs,
				barUser:                  privilege.List{privilege.GRANT},
				bazUser:                  privilege.List{privilege.GRANT},
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
				security.RootUserName():  privilege.List{privilege.ALL},
				security.AdminRoleName(): privilege.List{privilege.ALL},
				fooUser:                  privilege.List{privilege.ALL},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &catpb.PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(u, p, false /* withGrantOption */)
		}

		catprivilege.MaybeFixPrivileges(
			&desc,
			descpb.InvalidID,
			descpb.InvalidID,
			privilege.Database,
			testCase.name)

		if a, e := len(desc.Users), len(testCase.output); a != e {
			t.Errorf("#%d: expected %d users (%v), got %d (%v)", num, e, testCase.output, a, desc.Users)
			continue
		}

		for u, p := range testCase.output {
			outputUser, ok := desc.FindUser(u)
			if !ok {
				t.Fatalf("#%d: expected user %s in output, but not found (%v)", num, u, desc.Users)
			}
			if a, e := privilege.ListFromBitField(outputUser.Privileges, privilege.Any), p; a.ToBitField() != e.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v", num, u, e, a)
			}
		}
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
		privDescVersion catpb.PrivilegeDescVersion
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
			catpb.InitialVersion,
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
			catpb.InitialVersion,
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
			catpb.InitialVersion,
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
			catpb.OwnerVersion,
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
			catpb.OwnerVersion,
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
			catpb.OwnerVersion,
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
			catpb.OwnerVersion,
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
			catpb.InitialVersion,
			"A privilege descriptor from a table created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
				barUser: privilege.List{privilege.USAGE, privilege.CREATE},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
				barUser: privilege.List{privilege.ZONECONFIG, privilege.CREATE},
			},
			privilege.Database,
			catpb.InitialVersion,
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
			catpb.InitialVersion,
			"A privilege descriptor from a database created in v20.1 or prior " +
				"(InitialVersion) with USAGE should have the privilege converted to ZONECONFIG.",
			true,
		},
		{
			userPrivileges{
				fooUser: privilege.List{privilege.USAGE},
				barUser: privilege.List{privilege.USAGE, privilege.CREATE},
				bazUser: privilege.List{privilege.ALL, privilege.USAGE},
			},
			true,
			userPrivileges{
				fooUser: privilege.List{privilege.ZONECONFIG},
				barUser: privilege.List{privilege.ZONECONFIG, privilege.CREATE},
				bazUser: privilege.List{privilege.ALL},
			},
			privilege.Database,
			catpb.InitialVersion,
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
			catpb.InitialVersion,
			"If the descriptor has USAGE and ZONECONFIG, it should become just " +
				"ZONECONFIG",
			true,
		},
	}

	for num, tc := range testCases {
		desc := &catpb.PrivilegeDescriptor{Version: tc.privDescVersion}
		for u, p := range tc.input {
			desc.Grant(u, p, false /* withGrantOption */)
		}
		modified := catprivilege.MaybeFixUsagePrivForTablesAndDBs(&desc)

		if tc.modified != modified {
			t.Errorf("expected modifed to be %v, was %v", tc.modified, modified)
		}

		for u, p := range tc.output {
			outputUser, ok := desc.FindUser(u)
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
		desc := &catpb.PrivilegeDescriptor{}
		for u, p := range tc.input {
			desc.Grant(u, p, false /* withGrantOption */)
		}
		testParentID := descpb.ID(bootstrap.TestingMinNonDefaultUserDescID())
		catprivilege.MaybeFixPrivileges(&desc,
			testParentID,
			descpb.InvalidID,
			privilege.Schema,
			"whatever",
		)

		for u, p := range tc.output {
			outputUser, ok := desc.FindUser(u)
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
