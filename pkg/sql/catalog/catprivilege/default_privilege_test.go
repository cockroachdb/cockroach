// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catprivilege

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

const nonSystemDatabaseID = 51

func TestGrantDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fooUser := username.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := username.MakeSQLUsernameFromPreNormalizedString("baz")
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")

	testCases := []struct {
		defaultPrivilegesRole catpb.DefaultPrivilegesRole
		privileges            privilege.List
		grantees              []username.SQLUsername
		targetObject          privilege.TargetObjectType
		objectCreator         username.SQLUsername
	}{
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser},
			targetObject:          privilege.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Schemas,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Schemas,
			objectCreator:         creatorUser,
		},
		/* Test cases for ForAllRoles */
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.ALL},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Schemas,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Tables,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.SELECT, privilege.DELETE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Sequences,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			privileges:            privilege.List{privilege.USAGE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Schemas,
			objectCreator:         creatorUser,
		},
	}

	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)

		if err := defaultPrivileges.GrantDefaultPrivileges(tc.defaultPrivilegesRole, tc.privileges, tc.grantees, tc.targetObject, false /* withGrantOption */); err != nil {
			t.Fatal(err)
		}

		newPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, tc.objectCreator, tc.targetObject,
		)
		if err != nil {
			t.Fatal(err)
		}

		for _, grantee := range tc.grantees {
			for _, privilege := range tc.privileges {
				if !newPrivileges.CheckPrivilege(grantee, privilege) {
					t.Errorf("expected %s to have %s privilege", grantee, privilege.DisplayName())
				}
			}
		}
	}
}

func TestRevokeDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fooUser := username.MakeSQLUsernameFromPreNormalizedString("foo")
	barUser := username.MakeSQLUsernameFromPreNormalizedString("bar")
	bazUser := username.MakeSQLUsernameFromPreNormalizedString("baz")
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")

	testCases := []struct {
		defaultPrivilegesRole                                 catpb.DefaultPrivilegesRole
		grantPrivileges, revokePrivileges, expectedPrivileges privilege.List
		grantees                                              []username.SQLUsername
		targetObject                                          privilege.TargetObjectType
		objectCreator                                         username.SQLUsername
	}{
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.SELECT},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.DROP, privilege.INSERT,
				privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG,
			},
			grantees:      []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  privilege.Tables,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.SELECT},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.DROP, privilege.INSERT,
				privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG,
			},
			grantees:      []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  privilege.Sequences,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges:    privilege.List{},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{Role: creatorUser},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges:    privilege.List{privilege.CREATE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Schemas,
			objectCreator:         creatorUser,
		},
		/* Test cases for ForAllRoles */
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.SELECT},
			expectedPrivileges: privilege.List{
				privilege.CREATE, privilege.DROP, privilege.INSERT,
				privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG,
			},
			grantees:      []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:  privilege.Sequences,
			objectCreator: creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges:    privilege.List{},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Types,
			objectCreator:         creatorUser,
		},
		{
			defaultPrivilegesRole: catpb.DefaultPrivilegesRole{ForAllRoles: true},
			grantPrivileges:       privilege.List{privilege.ALL},
			revokePrivileges:      privilege.List{privilege.USAGE},
			expectedPrivileges:    privilege.List{privilege.CREATE},
			grantees:              []username.SQLUsername{fooUser, barUser, bazUser},
			targetObject:          privilege.Schemas,
			objectCreator:         creatorUser,
		},
	}

	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)

		if err := defaultPrivileges.GrantDefaultPrivileges(
			tc.defaultPrivilegesRole, tc.grantPrivileges, tc.grantees, tc.targetObject, false, /* withGrantOption */
		); err != nil {
			t.Fatal(err)
		}
		if err := defaultPrivileges.RevokeDefaultPrivileges(
			tc.defaultPrivilegesRole, tc.revokePrivileges, tc.grantees, tc.targetObject, false, /* grantOptionFor */
		); err != nil {
			t.Fatal(err)
		}

		newPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, tc.objectCreator, tc.targetObject,
		)
		if err != nil {
			t.Fatal(err)
		}

		for _, grantee := range tc.grantees {
			for _, privilege := range tc.expectedPrivileges {
				if !newPrivileges.CheckPrivilege(grantee, privilege) {
					t.Errorf("expected %s to have %s privilege", grantee, privilege.DisplayName())
				}
			}
		}
	}
}

func TestRevokeDefaultPrivilegesFromEmptyList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")
	fooUser := username.MakeSQLUsernameFromPreNormalizedString("foo")
	if err := defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{
			Role: creatorUser,
		}, privilege.List{privilege.ALL}, []username.SQLUsername{fooUser}, privilege.Tables, false, /* grantOptionFor */
	); err != nil {
		t.Fatal(err)
	}

	newPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
		defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
		nonSystemDatabaseID, creatorUser, privilege.Tables,
	)
	if err != nil {
		t.Fatal(err)
	}

	if newPrivileges.AnyPrivilege(fooUser) {
		t.Errorf("expected %s to not have any privileges", fooUser)
	}
}

func TestCreatePrivilegesFromDefaultPrivilegesForSystemDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")
	newPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
		defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
		keys.SystemDatabaseID, creatorUser, privilege.Tables,
	)
	if err != nil {
		t.Fatal(err)
	}

	if !newPrivileges.Owner().IsNodeUser() {
		t.Errorf("expected owner to be node, owner was %s", newPrivileges.Owner())
	}
}

func TestPresetDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")

	targetObjectTypes := privilege.GetTargetObjectTypes()
	for _, targetObject := range targetObjectTypes {
		newPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, creatorUser, targetObject,
		)
		if err != nil {
			t.Fatal(err)
		}

		if !newPrivileges.CheckPrivilege(creatorUser, privilege.ALL) {
			t.Errorf("expected creator to have ALL privileges on %s", targetObject)
		}

		if targetObject == privilege.Types {
			if !newPrivileges.CheckPrivilege(username.PublicRoleName(), privilege.USAGE) {
				t.Errorf("expected %s to have %s on types", username.PublicRoleName(), privilege.USAGE.DisplayName())
			}
		}
	}
}

func TestPresetDefaultPrivilegesInSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_SCHEMA)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")

	targetObjectTypes := privilege.GetTargetObjectTypes()
	for _, targetObject := range targetObjectTypes {
		newPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges, nil, /* schemaDefaultPrivilegeDescriptor */
			nonSystemDatabaseID, creatorUser, targetObject,
		)
		if err != nil {
			t.Fatal(err)
		}

		// The owner always has ALL privileges.
		if !newPrivileges.CheckPrivilege(creatorUser, privilege.ALL) {
			t.Errorf("expected creator to have ALL privileges on %s", targetObject)
		}

		if targetObject == privilege.Types {
			if newPrivileges.CheckPrivilege(username.PublicRoleName(), privilege.USAGE) {
				t.Errorf("%s should not have %s on types", username.PublicRoleName(), privilege.USAGE.DisplayName())
			}
		}
	}
}

func TestDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The ID chosen doesn't matter as long as it's not the system db ID.
	defaultDatabaseID := descpb.ID(50)

	type userAndGrants struct {
		user   username.SQLUsername
		grants privilege.List
	}
	testCases := []struct {
		objectCreator          username.SQLUsername
		defaultPrivilegesRole  username.SQLUsername
		dbID                   descpb.ID
		targetObject           privilege.TargetObjectType
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
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  keys.SystemDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   username.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("bar"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("bar"),
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			// In this case, we ALTER DEFAULT PRIVILEGES for the role foo.
			// However the default privileges are retrieved for bar, thus
			// we don't expect any privileges on the object.
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("foo"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("bar"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("bar"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{},
		},
		// Test cases where we also grant on schemas.
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			userAndGrantsInSchema: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.CREATE},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   username.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user: username.MakeSQLUsernameFromPreNormalizedString("foo"),
					// Should be the union of the default privileges on the db and schema.
					grants: privilege.List{privilege.SELECT, privilege.CREATE},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			userAndGrantsInSchema: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   username.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user: username.MakeSQLUsernameFromPreNormalizedString("foo"),
					// Should be the union of the default privileges on the db and schema.
					grants: privilege.List{privilege.ALL},
				},
			},
		},
		{
			objectCreator:         username.MakeSQLUsernameFromPreNormalizedString("creator"),
			defaultPrivilegesRole: username.MakeSQLUsernameFromPreNormalizedString("creator"),
			targetObject:          privilege.Tables,
			dbID:                  defaultDatabaseID,
			userAndGrants: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.ALL},
				},
			},
			userAndGrantsInSchema: []userAndGrants{
				{
					user:   username.MakeSQLUsernameFromPreNormalizedString("foo"),
					grants: privilege.List{privilege.SELECT},
				},
			},
			expectedGrantsOnObject: []userAndGrants{
				{
					user:   username.RootUserName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user:   username.AdminRoleName(),
					grants: privilege.List{privilege.ALL},
				},
				{
					user: username.MakeSQLUsernameFromPreNormalizedString("foo"),
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
			if err := defaultPrivileges.GrantDefaultPrivileges(
				catpb.DefaultPrivilegesRole{Role: tc.defaultPrivilegesRole},
				userAndGrant.grants,
				[]username.SQLUsername{userAndGrant.user},
				tc.targetObject, false, /* withGrantOption */
			); err != nil {
				t.Fatal(err)
			}
		}

		for _, userAndGrant := range tc.userAndGrantsInSchema {
			if err := schemaDefaultPrivileges.GrantDefaultPrivileges(
				catpb.DefaultPrivilegesRole{Role: tc.defaultPrivilegesRole},
				userAndGrant.grants,
				[]username.SQLUsername{userAndGrant.user},
				tc.targetObject,
				false, /* withGrantOption */
			); err != nil {
				t.Fatal(err)
			}
		}

		createdPrivileges, err := CreatePrivilegesFromDefaultPrivileges(
			defaultPrivileges,
			schemaDefaultPrivileges,
			tc.dbID,
			tc.objectCreator,
			tc.targetObject,
		)
		if err != nil {
			t.Fatal(err)
		}

		for _, userAndGrant := range tc.expectedGrantsOnObject {
			for _, grant := range userAndGrant.grants {
				if !createdPrivileges.CheckPrivilege(userAndGrant.user, grant) {
					t.Errorf("expected to find %s privilege for %s", grant.DisplayName(), userAndGrant.user)
				}
			}
		}
	}
}

func TestModifyDefaultDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		targetObject             privilege.TargetObjectType
		revokeAndGrantPrivileges privilege.List
	}{
		{
			targetObject:             privilege.Tables,
			revokeAndGrantPrivileges: privilege.List{privilege.SELECT},
		},
		{
			targetObject:             privilege.Sequences,
			revokeAndGrantPrivileges: privilege.List{privilege.SELECT},
		},
		{
			targetObject:             privilege.Types,
			revokeAndGrantPrivileges: privilege.List{privilege.USAGE},
		},
		{
			targetObject:             privilege.Schemas,
			revokeAndGrantPrivileges: privilege.List{privilege.USAGE},
		},
	}

	for _, tc := range testCases {
		defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
		defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
		creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")

		defaultPrivilegesForCreator := defaultPrivileges.defaultPrivilegeDescriptor.
			FindOrCreateUser(catpb.DefaultPrivilegesRole{
				Role: creatorUser,
			})

		if err := defaultPrivileges.RevokeDefaultPrivileges(
			catpb.DefaultPrivilegesRole{Role: creatorUser},
			tc.revokeAndGrantPrivileges,
			[]username.SQLUsername{creatorUser},
			tc.targetObject, false, /* grantOptionFor */
		); err != nil {
			t.Fatal(err)
		}
		if GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForCreator, tc.targetObject) {
			t.Errorf("expected role to not have ALL privileges on %s", tc.targetObject)
		}
		if err := defaultPrivileges.GrantDefaultPrivileges(
			catpb.DefaultPrivilegesRole{Role: creatorUser},
			tc.revokeAndGrantPrivileges,
			[]username.SQLUsername{creatorUser},
			tc.targetObject, false, /* withGrantOption */
		); err != nil {
			t.Fatal(err)
		}
		if !GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForCreator, tc.targetObject) {
			t.Errorf("expected role to have ALL privileges on %s", tc.targetObject)
		}
	}
}

func TestModifyDefaultDefaultPrivilegesForPublic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defaultPrivilegeDescriptor := MakeDefaultPrivilegeDescriptor(catpb.DefaultPrivilegeDescriptor_DATABASE)
	defaultPrivileges := NewMutableDefaultPrivileges(defaultPrivilegeDescriptor)
	creatorUser := username.MakeSQLUsernameFromPreNormalizedString("creator")

	defaultPrivilegesForCreator := defaultPrivileges.defaultPrivilegeDescriptor.
		FindOrCreateUser(catpb.DefaultPrivilegesRole{
			Role: creatorUser,
		})

	if err := defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]username.SQLUsername{username.PublicRoleName()},
		privilege.Types,
		false, /* grantOptionFor */
	); err != nil {
		t.Fatal(err)
	}
	if GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have USAGE privilege on types")
	}
	if err := defaultPrivileges.GrantDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]username.SQLUsername{username.PublicRoleName()},
		privilege.Types,
		false, /* withGrantOption */
	); err != nil {
		t.Fatal(err)
	}
	if !GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to have USAGE privilege on types")
	}

	// Test a complete revoke afterwards.
	if err := defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.USAGE},
		[]username.SQLUsername{username.PublicRoleName()},
		privilege.Types,
		false, /* grantOptionFor */
	); err != nil {
		t.Fatal(err)
	}
	if GetPublicHasUsageOnTypes(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have USAGE privilege on types")
	}

	if err := defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.EXECUTE},
		[]username.SQLUsername{username.PublicRoleName()},
		privilege.Routines,
		false, /* grantOptionFor */
	); err != nil {
		t.Fatal(err)
	}
	if GetPublicHasExecuteOnFunctions(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have EXECUTE privilege on functions")
	}
	if err := defaultPrivileges.GrantDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.EXECUTE},
		[]username.SQLUsername{username.PublicRoleName()},
		privilege.Routines,
		false, /* withGrantOption */
	); err != nil {
		t.Fatal(err)
	}
	if !GetPublicHasExecuteOnFunctions(defaultPrivilegesForCreator) {
		t.Errorf("expected public to have EXECUTE privilege on functions")
	}

	// Test a complete revoke afterwards.
	if err := defaultPrivileges.RevokeDefaultPrivileges(
		catpb.DefaultPrivilegesRole{Role: creatorUser},
		privilege.List{privilege.EXECUTE},
		[]username.SQLUsername{username.PublicRoleName()},
		privilege.Routines,
		false, /* grantOptionFor */
	); err != nil {
		t.Fatal(err)
	}
	if GetPublicHasExecuteOnFunctions(defaultPrivilegesForCreator) {
		t.Errorf("expected public to not have EXECUTE privilege on functions")
	}
}

// TestApplyDefaultPrivileges tests whether granting potentially different privileges and grant options
// changes the privilege bits and grant option bits as expected.
func TestApplyDefaultPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testUser := username.TestUserName()

	testCases := []struct {
		pd                  *catpb.PrivilegeDescriptor
		user                username.SQLUsername
		objectType          privilege.ObjectType
		grantPrivileges     privilege.List
		grantGrantOptions   privilege.List
		expectedPrivileges  privilege.List
		expectedGrantOption privilege.List
	}{
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{}, privilege.List{}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.INSERT}, privilege.List{privilege.INSERT}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL},
			privilege.List{privilege.CREATE, privilege.SELECT},
			privilege.List{privilege.ALL},
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE, privilege.INSERT}, privilege.List{privilege.CREATE}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
			privilege.List{privilege.SELECT},
			privilege.List{privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
			privilege.List{privilege.CREATE, privilege.SELECT}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
			privilege.List{privilege.ALL, privilege.SELECT},
			privilege.List{privilege.ALL},
			privilege.List{privilege.ALL}},
		{catpb.NewPrivilegeDescriptor(testUser, privilege.List{privilege.CREATE}, privilege.List{privilege.CREATE}, username.AdminRoleName()),
			testUser, privilege.Table,
			privilege.List{privilege.SELECT},
			privilege.List{privilege.SELECT, privilege.INSERT},
			privilege.List{privilege.CREATE},
			privilege.List{privilege.CREATE}},
	}

	for tcNum, tc := range testCases {
		applyDefaultPrivileges(tc.pd, tc.user, tc.grantPrivileges, tc.grantGrantOptions)
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

func TestUnsetDefaultPrivilegeDescriptorType(t *testing.T) {
	emptyDefaultPrivilegeDescriptor := catpb.DefaultPrivilegeDescriptor{}

	// If Type is not set, it should resolve to
	// catpb.DefaultPrivilegeDescriptor_DATABASE by default.
	require.Equal(t, emptyDefaultPrivilegeDescriptor.Type, catpb.DefaultPrivilegeDescriptor_DATABASE)
}
