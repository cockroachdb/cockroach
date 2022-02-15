// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dbdesc

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	desc := NewInitial(id, string(stmt.AST.(*tree.CreateDatabase).Name), security.AdminRoleName())
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
				Privileges: catpb.NewBaseDatabasePrivilegeDescriptor(security.RootUserName()),
			},
		},
		{
			`primary region unset on a multi-region db 200`,
			descpb.DatabaseDescriptor{
				Name:         "multi-region-db",
				ID:           200,
				RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{},
				Privileges:   catpb.NewBaseDatabasePrivilegeDescriptor(security.RootUserName()),
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
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 53, Dropped: true},
				},
			},
		},
		{ // 3
			err: `schema mapping entry "schema1" (500): referenced schema ID 500: referenced descriptor not found`,
			desc: descpb.DatabaseDescriptor{
				ID:   51,
				Name: "db1",
				Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
					"schema1": {ID: 500},
				},
			},
		},
		{ // 4
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
		{ // 5
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
		{ // 6
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
		{ // 7
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
		{ // 8
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
		{ // 9
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
		privilege := catpb.NewBasePrivilegeDescriptor(security.AdminRoleName())
		var cb nstree.MutableCatalog
		test.desc.Privileges = privilege
		desc := NewBuilder(&test.desc).BuildImmutable()
		cb.UpsertDescriptorEntry(desc)
		test.multiRegionEnum.Privileges = privilege
		cb.UpsertDescriptorEntry(typedesc.NewBuilder(&test.multiRegionEnum).BuildImmutable())
		for _, schemaDesc := range test.schemaDescs {
			schemaDesc.Privileges = privilege
			cb.UpsertDescriptorEntry(schemadesc.NewBuilder(&schemaDesc).BuildImmutable())
		}
		_ = cb.ForEachDescriptorEntry(func(desc catalog.Descriptor) error {
			cb.UpsertNamespaceEntry(desc, desc.GetID())
			return nil
		})
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		results := cb.Validate(ctx, clusterversion.TestingClusterVersion, catalog.NoValidationTelemetry, catalog.ValidationLevelAllPreTxnCommit, desc)
		if err := results.CombinedError(); err == nil {
			if test.err != "" {
				t.Errorf("%d: expected \"%s\", but found success: %+v", i, expectedErr, test.desc)
			}
		} else if expectedErr != err.Error() {
			t.Errorf("%d: expected \"%s\", but found \"%s\"", i, expectedErr, err.Error())
		}
	}
}

// TestFixDroppedSchemaName tests fixing a corrupted descriptor as part of
// RunPostDeserializationChanges. It tests for a particular corruption that
// happened when a schema was dropped that had the same name as its parent
// database name.
func TestFixDroppedSchemaName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const (
		dbName = "foo"
		dbID   = 1
	)
	dbDesc := descpb.DatabaseDescriptor{
		Name: dbName,
		ID:   dbID,
		Schemas: map[string]descpb.DatabaseDescriptor_SchemaInfo{
			dbName: {ID: dbID, Dropped: true},
		},
		Privileges: catpb.NewBasePrivilegeDescriptor(security.RootUserName()),
	}
	b := NewBuilder(&dbDesc)
	b.RunPostDeserializationChanges()
	desc := b.BuildCreatedMutableDatabase()
	require.Truef(t, desc.GetPostDeserializationChanges().HasChanges(),
		"expected changes in descriptor, found none")
	_, ok := desc.Schemas[dbName]
	require.Falsef(t, ok, "erroneous entry exists")
}

func TestMaybeConvertIncompatibleDBPrivilegesToDefaultPrivileges(t *testing.T) {
	tests := []struct {
		privilegeDesc          catpb.PrivilegeDescriptor
		defaultPrivilegeDesc   catpb.DefaultPrivilegeDescriptor
		privileges             privilege.List
		incompatiblePrivileges privilege.List
		shouldChange           bool
		users                  []security.SQLUsername
	}{
		{ // 0
			privilegeDesc:          catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc:   catpb.DefaultPrivilegeDescriptor{},
			privileges:             privilege.List{privilege.SELECT},
			incompatiblePrivileges: privilege.List{privilege.SELECT},
			shouldChange:           true,
			users: []security.SQLUsername{
				security.MakeSQLUsernameFromPreNormalizedString("test"),
			},
		},
		{ // 1
			privilegeDesc:        catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc: catpb.DefaultPrivilegeDescriptor{},
			privileges: privilege.List{
				privilege.CONNECT, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.ZONECONFIG,
			},
			incompatiblePrivileges: privilege.List{},
			shouldChange:           false,
			users: []security.SQLUsername{
				security.MakeSQLUsernameFromPreNormalizedString("test"),
			},
		},
		{ // 2
			privilegeDesc:          catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc:   catpb.DefaultPrivilegeDescriptor{},
			privileges:             privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE, privilege.DELETE},
			incompatiblePrivileges: privilege.List{privilege.SELECT, privilege.INSERT, privilege.UPDATE, privilege.DELETE},
			shouldChange:           true,
			users: []security.SQLUsername{
				security.MakeSQLUsernameFromPreNormalizedString("test"),
			},
		},
		{ // 3
			privilegeDesc:          catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc:   catpb.DefaultPrivilegeDescriptor{},
			privileges:             privilege.List{privilege.SELECT},
			incompatiblePrivileges: privilege.List{privilege.SELECT},
			shouldChange:           true,
			users: []security.SQLUsername{
				security.MakeSQLUsernameFromPreNormalizedString("test"),
				security.MakeSQLUsernameFromPreNormalizedString("foo"),
			},
		},
		{ // 4
			privilegeDesc:        catpb.PrivilegeDescriptor{},
			defaultPrivilegeDesc: catpb.DefaultPrivilegeDescriptor{},
			privileges: privilege.List{
				privilege.CONNECT, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.ZONECONFIG,
			},
			incompatiblePrivileges: privilege.List{},
			shouldChange:           false,
			users: []security.SQLUsername{
				security.MakeSQLUsernameFromPreNormalizedString("test"),
				security.MakeSQLUsernameFromPreNormalizedString("foo"),
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
			for _, privilege := range test.incompatiblePrivileges {
				// Check that the incompatible privileges are removed from the
				// PrivilegeDescriptor.
				if test.privilegeDesc.CheckPrivilege(testUser, privilege) {
					t.Errorf("found incompatible privilege %s", privilege.String())
				}

				forAllRoles := test.defaultPrivilegeDesc.
					FindOrCreateUser(catpb.DefaultPrivilegesRole{ForAllRoles: true})
				// Check that the incompatible privileges have been converted to the
				// equivalent default privileges.
				if !forAllRoles.DefaultPrivilegesPerObject[tree.Tables].CheckPrivilege(testUser, privilege) {
					t.Errorf(
						"expected incompatible privilege %s to be converted to a default privilege",
						privilege.String(),
					)
				}
			}
		}
	}
}
