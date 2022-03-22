// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catprivilege

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

const nonSystemDatabaseID = 51

func TestGrantDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := security.MakeSQLUsernameFromPreNormalizedString("baz")
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")

	testCases := []struct {
		defaultPrivilegesRole catpb.DefaultPrivilegesRole
		privileges            privilege.List
		grantees              []security.SQLUsername
		targetObject          tree.AlterDefaultPrivilegesTargetObject
		objectCreator         security.SQLUsername
	}{
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser},
			targetObject:          tree.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Schemas,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Schemas,
			objectCreator:         creatorUser,
		},
		/* Test cases for ForAllRoles */
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Schemas,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          tree.Schemas,
			objectCreator:         creatorUser,
		},
	}

	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)

		defaultPrivileges.GrantDefaultPrivileges(tc.defaultPrivilegesRole, tc.privileges, tc.grantees, tc.targetObject, false /* withGrantOption */, false /*deprecateGrant*/)

		newPrivileges := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, tc.objectCreator, tc.targetObject, &catpb.PrivilegeDescriptor{},
		)

		for _, grantee := range tc.grantees {
			for _, privilege := range tc.privileges {
				if !newPrivileges.CheckPrivilege(grantee, privilege) {
					t.Errorf("expected %s to have %s privilege", grantee, privilege)
				}
			}
		}
	}
}

func TestRevokeDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := security.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := security.MakeSQLUsernameFromPreNormalizedString("baz")
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")

	testCases := []struct {
		defaultPrivilegesRole                                 catpb.DefaultPrivilegesRole
		grantPrivileges, revokePrivileges, expectedPrivileges privilege.List
		grantees                                              []security.SQLUsername
		targetObject                                          tree.AlterDefaultPrivilegesTargetObject
		objectCreator                                         security.SQLUsername
	}{
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.SELECT},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.INSERT,
				privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Tables,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.SELECT},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.INSERT,
				privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Sequences,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges: privilege.List{
				privilege.GRANT,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Types,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.GRANT,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Schemas,
			objectCreator: creatorUser,
		},
		/* Test cases for ForAllRoles */
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.SELECT},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.INSERT,
				privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Sequences,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges: privilege.List{
				privilege.GRANT,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Types,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.GRANT,
			},
			grantees:      []security.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  tree.Schemas,
			objectCreator: creatorUser,
		},
	}

	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)

		defaultPrivileges.GrantDefaultPrivileges(tc.defaultPrivilegesRole, tc.grantPrivileges, tc.grantees, tc.targetObject, false /* withGrantOption */, false /*deprecateGrant*/)
		defaultPrivileges.RevokeDefaultPrivileges(tc.defaultPrivilegesRole, tc.revokePrivileges, tc.grantees, tc.targetObject, false /* grantOptionFor */, false /*deprecateGrant*/)

		newPrivileges := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, tc.objectCreator, tc.targetObject, &catpb.PrivilegeDescriptor{},
		)

		for _, grantee := range tc.grantees {
			for _, privilege := range tc.expectedPrivileges {
				if !newPrivileges.CheckPrivilege(grantee, privilege) {
					t.Errorf("expected %s to have %s privilege", grantee, privilege)
				}
			}
		}
	}
}

func TestRevokeDefaultPrivilegesFromEmptyList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")
	fooUser := security.MakeSQLUsernameFromPreNormalizedString("foo")
	defaultPrivileges.RevokeDefaultPrivileges(catpb.DefaultPrivilegesRole{
		Role: creatorUser,
	}, privilege.List{privilege.ALL}, []security.SQLUsername{fooUser}, tree.Tables, false /* grantOptionFor */, false /*deprecateGrant*/)

	newPrivileges := CreatePrivilegesFromDefaultPrivileges(
		defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
		nonSystemDatabaseID, creatorUser, tree.Tables, &catpb.PrivilegeDescriptor{},
	)

	if newPrivileges.AnyPrivilege(fooUser) {
		t.Errorf("expected %s to not have any privileges", fooUser)
	}
}

func TestCreatePrivilegesFromDefaultPrivilegesForSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")
	newPrivileges := CreatePrivilegesFromDefaultPrivileges(
		defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
		keys.SystemDatabaseID, creatorUser, tree.Tables, &catpb.PrivilegeDescriptor{},
	)

	if !newPrivileges.Owner().IsNodeUser() {
		t.Errorf("expected owner to be node, owner was %s", newPrivileges.Owner())
	}
}

func TestPresetDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")

	targetObjectTypes := tree.GetAlterDefaultPrivilegesTargetObjects()
	for _, targetObject := range targetObjectTypes {
		newPrivileges := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, creatorUser, targetObject, &catpb.PrivilegeDescriptor{},
		)

		if !newPrivileges.CheckPrivilege(creatorUser, privilege.ALL) {
			t.Errorf("expected creator to have ALL privileges on %s", targetObject)
		}

		if targetObject == tree.Types {
			if !newPrivileges.CheckPrivilege(security.PublicRoleName(), privilege.USAGE) {
				t.Errorf("expected %s to have %s on types", security.PublicRoleName(), privilege.USAGE)
			}
		}
	}
}

func TestPresetDefaultPrivilegesInSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")

	targetObjectTypes := tree.GetAlterDefaultPrivilegesTargetObjects()
	for _, targetObject := range targetObjectTypes {
		newPrivileges := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, creatorUser, targetObject, &catpb.PrivilegeDescriptor{},
		)

		// There are no preset privileges on a default privilege descriptor defined
		// for a schema.
		if newPrivileges.CheckPrivilege(creatorUser, privilege.ALL) {
			t.Errorf("creator should not have ALL privileges on %s", targetObject)
		}

		if targetObject == tree.Types {
			if newPrivileges.CheckPrivilege(security.PublicRoleName(), privilege.USAGE) {
				t.Errorf("%s should not have %s on types", security.PublicRoleName(), privilege.USAGE)
			}
		}
	}
}

func TestDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The ID chosen doesn't matter as long as it's not the system db ID.
	defaultDatabaseID := descpb.ID(50)

	type userAndGrants struct {
		user   security.SQLUsername
		grants privilege.List
	}
	testCases := []struct {
		objectCreator          security.SQLUsername
		defaultPrivilegesRole  security.SQLUsername
		dbID                   descpb.ID
		targetObject           tree.AlterDefaultPrivilegesTargetObject
		userAndGrants          []userAndGrants
		userAndGrantsInSchema  []userAndGrants
		expectedGrantsOnObject []userAndGrants
	}{
		{
			// Altering default privileges on the system database normally wouldn't
			// be possible but we do it here via directly altering the default
			// privilege descriptor here.
			// The purpose of this test however is to show that even after altering
			// the default privileges, if we create an object in the system database,
			// the only privileges on the object are ALL privileges for root and
			// admin.
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  keys.SystemDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   security.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("bar"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("bar"),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			// In this case, we ALTER DEFAULT PRIVILEGES for the role foo.
			// However the default privileges are retrieved for bar, thus
			// we don't expect any privileges on the object.
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("foo"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("bar"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("bar"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{},
		},
		// Test cases where we also grant on schemas.
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			userAndGrantsInSchema: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.CREATE},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   security.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user: security.MakeSQLUsernameFromPreNormalizedString("foo"),
					// Should be the union of the default privileges on the db and schema.
					grants: privilege.List{privilege.SELECT, privilege.CREATE},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			userAndGrantsInSchema: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   security.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user: security.MakeSQLUsernameFromPreNormalizedString("foo"),
					// Should be the union of the default privileges on the db and schema.
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         security.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: security.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          tree.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			userAndGrantsInSchema: []userAndGrants{
				{
					user:   security.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   security.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   security.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user: security.MakeSQLUsernameFromPreNormalizedString("foo"),
					// Should be the union of the default privileges on the db and schema.
					grants: privilege.List{privilege.ALL},
				},
			},
		},
	}
	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)

		schemaPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA)
		schemaDefaultPrivileges := NewMutableDefaultPrivileges(schemaPrivilegeDescriptor)

		for _, userAndGrant := range tc.userAndGrants {
			defaultPrivileges.GrantDefaultPrivileges(
				catpb.DefaultPrivilegesRole{Role: tc.defaultPrivilegesRole},
				userAndGrant.grants,
				[]security.SQLUsername{userAndGrant.user},
				tc.targetObject, false, /* withGrantOption */
				false, /*deprecateGrant*/
			)
		}

		for _, userAndGrant := range tc.userAndGrantsInSchema {
			schemaDefaultPrivileges.GrantDefaultPrivileges(
				catpb.DefaultPrivilegesRole{Role: tc.defaultPrivilegesRole},
				userAndGrant.grants,
				[]security.SQLUsername{userAndGrant.user},
				tc.targetObject,
				false, /* withGrantOption */
				false, /*deprecateGrant*/
			)
		}

		createdPrivileges := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges,
			schemaDefaultPrivileges,
			tc.dbID,
			tc.objectCreator,
			tc.targetObject,
			&catpb.PrivilegeDescriptor{},
		)

		for _, userAndGrant := range tc.expectedGrantsOnObject {
			for _, grant := range userAndGrant.grants {
				if !createdPrivileges.CheckPrivilege(userAndGrant.user, grant) {
					t.Errorf("expected to find %s privilege for %s", grant.String(), userAndGrant.user)
				}
			}
		}
	}
}

func TestModifyDefaultDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		targetObject             tree.AlterDefaultPrivilegesTargetObject
		revokeAndGrantPrivileges privilege.List
	}{
		{
			targetObject:             tree.Tables,
			revokeAndGrantPrivileges: privilege.List{privilege.SELECT},
		},
		{
			targetObject:             tree.Sequences,
			revokeAndGrantPrivileges: privilege.List{privilege.SELECT},
		},
		{
			targetObject:             tree.Types,
			revokeAndGrantPrivileges: privilege.List{privilege.USAGE},
		},
		{
			targetObject:             tree.Schemas,
			revokeAndGrantPrivileges: privilege.List{privilege.USAGE},
		},
	}

	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
		creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")

		defaultPrivilegesForCreator := defaultPrivileges.defaultPrivilegeDescriptor.
			FindOrCreateUser(catpb.DefaultPrivilegesRole{
				Role: creatorUser,
			})

		defaultPrivileges.RevokeDefaultPrivileges(
			catpb.DefaultPrivilegesRole{Role: creatorUser},
			tc.revokeAndGrantPrivileges,
			[]security.SQLUsername{creatorUser},
			tc.targetObject, false, /* grantOptionFor */
			false, /*deprecateGrant*/
		)
		if GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForCreator, tc.targetObject) {
			t.Errorf("expected role to not have ALL privileges on %s", tc.targetObject)
		}
		defaultPrivileges.GrantDefaultPrivileges(
			catpb.DefaultPrivilegesRole{Role: creatorUser},
			tc.revokeAndGrantPrivileges,
			[]security.SQLUsername{creatorUser},
			tc.targetObject, false, /* withGrantOption */
			false, /*deprecateGrant*/
		)
		if !GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForCreator, tc.targetObject) {
			t.Errorf("expected role to have ALL privileges on %s", tc.targetObject)
		}
	}
}

func TestModifyDefaultDefaultPrivilegesForPublic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := security.MakeSQLUsernameFromPreNormalizedString("creator")

	defaultPrivilegesForCreator := defaultPrivileges.defaultPrivilegeDescriptor.
		FindOrCreateUser(catpb.DefaultPrivilegesRole{
			Role: creatorUser,
		})

	defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]security.SQLUsername{security.PublicRoleName()},
		tree.Types, false, /* grantOptionFor */
		false, /*deprecateGrant*/
	)
	if GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have USAGE privilege on types")
	}
	defaultPrivileges.GrantDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]security.SQLUsername{security.PublicRoleName()},
		tree.Types, false, /* withGrantOption */
		false, /*deprecateGrant*/
	)
	if !GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to have USAGE privilege on types")
	}

	// Test granting when withGrantOption is true.
	defaultPrivileges.GrantDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]security.SQLUsername{security.PublicRoleName()},
		tree.Types, true, /* withGrantOption */
		false, /*deprecateGrant*/
	)

	privDesc := defaultPrivilegesForCreator.DefaultPrivilegesPerObject[tree.Types]
	user, found := privDesc.FindUser(security.PublicRoleName())
	if !found {
		t.Errorf("public not found on privilege descriptor when expected")
	}
	if !privilege.USAGE.IsSetIn(user.WithGrantOption) {
		t.Errorf("expected public to have USAGE grant options on types")
	}
	// This flag should not be true since there is a "modification" - i.e. grant option bits for USAGE are active,
	// so do not remove that user from the descriptor.
	if GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have USAGE privilege on types")
	}

	// Test revoking when grantOptionFor is true.
	defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]security.SQLUsername{security.PublicRoleName()},
		tree.Types, true, /* grantOptionFor */
		false, /*deprecateGrant*/
	)

	privDesc = defaultPrivilegesForCreator.DefaultPrivilegesPerObject[tree.Types]
	_, found = privDesc.FindUser(security.PublicRoleName())
	if found {
		t.Errorf("public found on privilege descriptor when it was supposed to be removed")
	}
	// Public still has usage on types since only the grant option for USAGE was revoked, not the privilege itself.
	if !GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to have USAGE privilege on types")
	}

	// Test a complete revoke afterwards.
	defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]security.SQLUsername{security.PublicRoleName()},
		tree.Types, false, /* grantOptionFor */
		false, /*deprecateGrant*/
	)
	if GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have USAGE privilege on types")
	}
}

// TestApplyDefaultPrivileges tests whether granting potentially different privileges and grant options
// changes the privilege bits and grant option bits as expected.
func TestApplyDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := security.TestUserName()

	testCases := []struct {
		pd                  *catpb.PrivilegeDescriptor
		user                security.SQLUsername
		objectType          privilege.ObjectType
		grantPrivileges     privilege.List
		grantGrantOptions   privilege.List
		expectedPrivileges  privilege.List
		expectedGrantOption privilege.List
	}{
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{}, privilege.List{}, security.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.INSERT}, privilege.List{privilege.INSERT}, security.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL},
			privilege.List{privilege.CREATE, privilege.SELECT},
			privilege.List{privilege.ALL},
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.INSERT}, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
			privilege.List{privilege.SELECT},
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
			privilege.List{privilege.CREATE, privilege.SELECT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
			privilege.List{privilege.ALL, privilege.SELECT},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, security.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.CREATE},
			privilege.List{privilege.CREATE}},
	}

	for tcNum, tc := range testCases {
		applyDefaultPrivileges(tc.pd, tc.user, tc.grantPrivileges, tc.grantGrantOptions)
		if tc.pd.Users[0].Privileges != tc.expectedPrivileges.ToBitField() {
			t.Errorf("#%d: Incorrect privileges, returned %v, expected %v",
				tcNum, privilege.ListFromBitField(tc.pd.Users[0].Privileges, tc.objectType), tc.expectedPrivileges)
		}
		if tc.pd.Users[0].WithGrantOption != tc.expectedGrantOption.ToBitField() {
			t.Errorf("#%d: Incorrect grant option, returned %v, expected %v",
				tcNum, privilege.ListFromBitField(tc.pd.Users[0].WithGrantOption, tc.objectType), tc.expectedGrantOption)
		}
	}
}

func TestUnsetDefaultPrivilegeDescriptorType(t *testing.T) {
	emptyDefaultPrivilegeDescriptor := catpb.DefaultPrivilegeDescriptor{}

	// If Type is not set, it should resolve to
	// catpb.DefaultPrivilegeDescriptor_DATABASE by default.
	require.Equal(t, emptyDefaultPrivilegeDescriptor.Type, catpb.DefaultPrivilegeDescriptor_DATABASE)
}
