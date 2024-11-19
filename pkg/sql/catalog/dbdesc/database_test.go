// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dbdesc

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestSafeMessage(t *testing.T) {
	for _, tc := range []struct {
		desc catalog.DatabaseDescriptor
		exp  string
	}{
		{
			desc: NewBuilder(&descpb.DatabaseDescriptor{
				ID:            12,
				Version:       1,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "foo",
			}).BuildImmutableDatabase(),
			exp: "dbdesc.immutable: {ID: 12, Version: 1, ModificationTime: \"0,0\", State: OFFLINE, OfflineReason: \"foo\"}",
		},
		{
			desc: NewBuilder(&descpb.DatabaseDescriptor{
				ID:            42,
				Version:       2,
				State:         descpb.DescriptorState_OFFLINE,
				OfflineReason: "bar",
			}).BuildCreatedMutableDatabase(),
			exp: "dbdesc.Mutable: {ID: 42, Version: 2, IsUncommitted: true, ModificationTime: \"0,0\", State: OFFLINE, OfflineReason: \"bar\"}",
		},
	} {
		t.Run("", func(t *testing.T) {
			redacted := string(redact.Sprint(tc.desc).Redact())
			require.Equal(t, tc.exp, redacted)
			{
				var m map[string]interface{}
				require.NoError(t, yaml.UnmarshalStrict([]byte(redacted), &m))
			}
		})
	}
}

func TestMakeDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stmt, err := parser.ParseOne("CREATE DATABASE test")
	if err != nil {
		t.Fatal(err)
	}
	const id = 17
	desc := NewInitial(id, string(stmt.AST.(*tree.CreateDatabase).Name), username.AdminRoleName())
	if desc.GetName() != "test" {
		t.Fatalf("expected Name == test, got %s", desc.GetName())
	}
	// ID is not set yet.
	if desc.GetID() != id {
		t.Fatalf("expected ID == %d, got %d", id, desc.GetID())
	}
	if len(desc.GetPrivileges().Users) != 3 {
		t.Fatalf("wrong number of privilege users, expected 3, got: %d", len(desc.GetPrivileges().Users))
	}
}

func TestValidateDatabaseDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		err  string
		desc descpb.DatabaseDescriptor
	}{
		{`invalid database ID 0`,
			descpb.DatabaseDescriptor{
				Name:       "db",
				ID:         0,
				Privileges: catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName()),
			},
		},
		{
			`primary region unset on a multi-region db 200`,
			descpb.DatabaseDescriptor{
				Name:         "multi-region-db",
				ID:           200,
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{},
				Privileges:   catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName()),
			},
		},
	}
	for i, d := range testData {
		t.Run(d.err, func(t *testing.T) {
			desc := NewBuilder(&d.desc).BuildImmutable()
			expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), d.err)
			if err := validate.Self(clusterversion.TestingClusterVersion, desc); err == nil {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, d.desc)
			} else if expectedErr != err.Error() {
				t.Errorf("%d: expected \"%s\", but found \"%+v\"", i, expectedErr, err)
			}
		})
	}
}

func TestValidateCrossDatabaseReferences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	tests := []struct {
		err             string
		desc            descpb.DatabaseDescriptor
		multiRegionEnum descpb.TypeDescriptor
		schemaDescs     []descpb.SchemaDescriptor
	}{
		{ // 0
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
			},
		},
		{ // 1
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52},
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "schema1",
					ID:       52,
					ParentID: 51,
				},
			},
		},
		{ // 2
			err: `schema mapping entry "schema1" (500): referenced schema ID 500: referenced descriptor not found`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 500},
				},
			},
		},
		{ // 3
			err: `schema mapping entry "schema1" (52): schema name is actually "foo"`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52},
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "foo",
					ID:       52,
					ParentID: 51,
				},
			},
		},
		{ // 4
			err: `schema mapping entry "schema1" (52): schema parentID is actually 500`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 52},
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "schema1",
					ID:       52,
					ParentID: 500,
				},
			},
		},
		{ // 5
			err: `multi-region enum: referenced type ID 500: referenced descriptor not found`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					RegionEnumID:  500,
					PrimaryRegion: "us-east-1",
				},
			},
		},
		{ // 6
			err: `multi-region enum: parentID is actually 500`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					RegionEnumID:  52,
					PrimaryRegion: "us-east-1",
				},
			},
			multiRegionEnum: descpb.TypeDescriptor{
				ID:       52,
				ParentID: 500,
			},
		},
		{ // 7
			err: `schema mapping entry "schema1" (53): referenced schema ID 53: descriptor is a *typedesc.immutable: unexpected descriptor type`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 53},
				},
			},
			multiRegionEnum: descpb.TypeDescriptor{
				ID:       53,
				ParentID: 51,
			},
		},
		{ // 8
			err: `multi-region enum: referenced type ID 53: descriptor is a *schemadesc.immutable: unexpected descriptor type`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
					RegionEnumID:  53,
					PrimaryRegion: "us-east-1",
				},
			},
			schemaDescs: []descpb.SchemaDescriptor{
				{
					Name:     "schema1",
					ID:       53,
					ParentID: 51,
				},
			},
		},
	}

	for i, test := range tests {
		privilege := catpb.NewBasePrivilegeDescriptor(username.AdminRoleName())
		var cb nstree.MutableCatalog
		test.desc.Privileges = privilege
		desc := NewBuilder(&test.desc).BuildImmutable()
		cb.UpsertDescriptor(desc)
		test.multiRegionEnum.Privileges = privilege
		cb.UpsertDescriptor(typedesc.NewBuilder(&test.multiRegionEnum).BuildImmutable())
		for _, schemaDesc := range test.schemaDescs {
			schemaDesc.Privileges = privilege
			cb.UpsertDescriptor(schemadesc.NewBuilder(&schemaDesc).BuildImmutable())
		}
		_ = cb.ForEachDescriptor(func(desc catalog.Descriptor) error {
			cb.UpsertNamespaceEntry(desc, desc.GetID(), desc.GetModificationTime())
			return nil
		})
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		results := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, validate.Write, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
	}
}

func TestMaybeConvertIncompatibleDBPrivilegesToDefaultPrivileges(t *testing.T) {
	tests := []struct {
		privilegeDesc          catpb.PrivilegeDescriptor
		defaultPrivilegeDesc   catpb.DefaultPrivilegeDescriptor
		privileges             privilege.List
		incompatiblePrivileges privilege.List
		shouldChange           bool
		users                  []username.SQLUsername
	}{
		{ // 0
			privilegeDesc:          catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc:   catpb.DefaultPrivilegeDescriptor{},
			privileges:             privilege.List{privilege.SELECT},
			incompatiblePrivileges: privilege.List{privilege.SELECT},
			shouldChange:           true,
			users: []username.SQLUsername{
				username.MakeSQLUsernameFromPreNormalizedString("test"),
			},
		},
		{ // 1
			privilegeDesc:        catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc: catpb.DefaultPrivilegeDescriptor{},
			privileges: privilege.List{
				privilege.CONNECT, privilege.CREATE, privilege.DROP, privilege.ZONECONFIG,
			},
			incompatiblePrivileges: privilege.List{},
			shouldChange:           false,
			users: []username.SQLUsername{
				username.MakeSQLUsernameFromPreNormalizedString("test"),
			},
		},
		{ // 2
			privilegeDesc:          catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc:   catpb.DefaultPrivilegeDescriptor{},
			privileges:             privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE, privilege.DELETE},
			incompatiblePrivileges: privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE, privilege.DELETE},
			shouldChange:           true,
			users: []username.SQLUsername{
				username.MakeSQLUsernameFromPreNormalizedString("test"),
			},
		},
		{ // 3
			privilegeDesc:          catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc:   catpb.DefaultPrivilegeDescriptor{},
			privileges:             privilege.List{privilege.SELECT},
			incompatiblePrivileges: privilege.List{privilege.SELECT},
			shouldChange:           true,
			users: []username.SQLUsername{
				username.MakeSQLUsernameFromPreNormalizedString("test"),
				username.MakeSQLUsernameFromPreNormalizedString("foo"),
			},
		},
		{ // 4
			privilegeDesc:        catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc: catpb.DefaultPrivilegeDescriptor{},
			privileges: privilege.List{
				privilege.CONNECT, privilege.CREATE, privilege.DROP, privilege.ZONECONFIG,
			},
			incompatiblePrivileges: privilege.List{},
			shouldChange:           false,
			users: []username.SQLUsername{
				username.MakeSQLUsernameFromPreNormalizedString("test"),
				username.MakeSQLUsernameFromPreNormalizedString("foo"),
			},
		},
	}

	for _, test := range tests {
		for _, testUser := range test.users {
			test.privilegeDesc.Grant(testUser, test.privileges, false /* withGrantOption */)
		}

		shouldChange := maybeConvertIncompatibleDBPrivilegesToDefaultPrivileges(&test.privilegeDesc, &test.defaultPrivilegeDesc)
		require.Equal(t, shouldChange, test.shouldChange)

		for _, testUser := range test.users {
			for _, priv := range test.incompatiblePrivileges {
				// Check that the incompatible privileges are removed from the
				// PrivilegeDescriptor.
				if test.privilegeDesc.CheckPrivilege(testUser, priv) {
					t.Errorf("found incompatible privilege %s", priv.DisplayName())
				}

				forAllRoles := test.defaultPrivilegeDesc.
					FindOrCreateUser(catpb.DefaultPrivilegesRole{ForAllRoles: true})
				// Check that the incompatible privileges have been converted to the
				// equivalent default privileges.
				if !forAllRoles.DefaultPrivilegesPerObject[privilege.Tables].CheckPrivilege(testUser, priv) {
					t.Errorf(
						"expected incompatible privilege %s to be converted to a default privilege",
						priv.DisplayName(),
					)
				}
			}
		}
	}
}

func TestStripDanglingBackReferencesAndRoles(t *testing.T) {
	type testCase struct {
		name                           string
		input, expectedOutput          descpb.DatabaseDescriptor
		validIDs                       catalog.DescriptorIDSet
		strippedDanglingBackReferences bool
		strippedNonExistentRoles       bool
	}

	badOwnerPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.MakeSQLUsernameFromPreNormalizedString("dropped_user"))
	goodOwnerPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.AdminRoleName())
	badPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName())
	goodPrivilege := catpb.NewBaseDatabasePrivilegeDescriptor(username.RootUserName())
	badPrivilege.Users = append(badPrivilege.Users, catpb.UserPrivileges{
		UserProto: username.TestUserName().EncodeProto(),
	})
	testData := []testCase{
		{
			name: "schema",
			input: descpb.DatabaseDescriptor{
				Name: "foo",
				ID:   100,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"bar": {ID: 101},
					"baz": {ID: 12345},
				},
				Privileges: badPrivilege,
			},
			expectedOutput: descpb.DatabaseDescriptor{
				Name: "foo",
				ID:   100,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"bar": {ID: 101},
				},
				Privileges: goodPrivilege,
			},
			validIDs:                       catalog.MakeDescriptorIDSet(100, 101),
			strippedDanglingBackReferences: true,
			strippedNonExistentRoles:       true,
		},
		{
			name: "missing owner",
			input: descpb.DatabaseDescriptor{
				Name: "foo",
				ID:   100,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"bar": {ID: 101},
				},
				Privileges: badOwnerPrivilege,
			},
			expectedOutput: descpb.DatabaseDescriptor{
				Name: "foo",
				ID:   100,
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"bar": {ID: 101},
				},
				Privileges: goodOwnerPrivilege,
			},
			validIDs:                 catalog.MakeDescriptorIDSet(100, 101),
			strippedNonExistentRoles: true,
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			b := NewBuilder(&test.input)
			require.NoError(t, b.RunPostDeserializationChanges())
			out := NewBuilder(&test.expectedOutput)
			require.NoError(t, out.RunPostDeserializationChanges())
			require.NoError(t, b.StripDanglingBackReferences(test.validIDs.Contains, func(id jobspb.JobID) bool {
				return false
			}))
			require.NoError(t, b.StripNonExistentRoles(func(role username.SQLUsername) bool {
				return role.IsAdminRole() || role.IsPublicRole() || role.IsRootUser() || role.IsNodeUser()
			}))
			desc := b.BuildCreatedMutableDatabase()
			require.Equal(t, test.strippedDanglingBackReferences, desc.GetPostDeserializationChanges().Contains(catalog.StrippedDanglingBackReferences))
			require.Equal(t, test.strippedNonExistentRoles, desc.GetPostDeserializationChanges().Contains(catalog.StrippedNonExistentRoles))
			require.Equal(t, out.BuildCreatedMutableDatabase().DatabaseDesc(), desc.DatabaseDesc())
		})
	}
}
