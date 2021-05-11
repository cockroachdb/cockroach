// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package typedesc_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestTypeDescIsCompatibleWith(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		a descpb.TypeDescriptor
		b descpb.TypeDescriptor
		// If err == "", then no error is expected. Otherwise, an error that
		// matches is expected.
		err string
	}{
		// Different type kinds shouldn't be equal.
		{
			a: descpb.TypeDescriptor{
				Name: "a",
				Kind: descpb.TypeDescriptor_ENUM,
			},
			b: descpb.TypeDescriptor{
				Name: "b",
				Kind: descpb.TypeDescriptor_ALIAS,
			},
			err: `"b" of type "ALIAS" is not compatible with type "ENUM"`,
		},
		{
			a: descpb.TypeDescriptor{
				Name: "a",
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Name: "b",
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
				},
			},
			err: `"b" of type "MULTIREGION_ENUM" is not compatible with type "ENUM"`,
		},
		// We aren't considering compatibility between different alias kinds.
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ALIAS,
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ALIAS,
			},
			err: `compatibility comparison unsupported`,
		},
		// The empty enum should be compatible with any other enums.
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
				},
			},
			err: ``,
		},
		// The same enum should be compatible with itself.
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: ``,
		},
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: ``,
		},
		// An enum with only some members of another enum should be compatible.
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: ``,
		},
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: ``,
		},
		// An enum with missing members shouldn't be compatible.
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "howdy",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: `could not find enum value "howdy"`,
		},
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-3",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: `could not find enum value "us-east-1"`,
		},
		// An enum with a different physical representation shouldn't be compatible.
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{201},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "hello",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "hi",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: `has differing physical representation for value "hi"`,
		},
		{
			a: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{201},
					},
				},
			},
			b: descpb.TypeDescriptor{
				Kind: descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{128},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{200},
					},
				},
			},
			err: `has differing physical representation for value "us-east-2"`,
		},
	}

	for i, test := range tests {
		a := typedesc.NewBuilder(&test.a).BuildImmutableType()
		b := typedesc.NewBuilder(&test.b).BuildImmutableType()
		err := a.IsCompatibleWith(b)
		if test.err == "" {
			require.NoError(t, err)
		} else {
			if !testutils.IsError(err, test.err) {
				t.Errorf("#%d expected error %s, but found %s", i, test.err, err)
			}
		}
	}
}

func TestValidateTypeDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	descs := catalog.MakeMapDescGetter()
	descs.Descriptors[100] = dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
		Name: "db",
		ID:   100,
	}).BuildImmutable()
	descs.Descriptors[101] = schemadesc.NewBuilder(&descpb.SchemaDescriptor{
		ID:       101,
		ParentID: 100,
		Name:     "schema",
	}).BuildImmutable()
	descs.Descriptors[102] = typedesc.NewBuilder(&descpb.TypeDescriptor{
		ID:   102,
		Name: "type",
	}).BuildImmutable()
	descs.Descriptors[200] = dbdesc.NewBuilder(&descpb.DatabaseDescriptor{
		Name: "multi-region-db",
		ID:   200,
		RegionConfig: &descpb.DatabaseDescriptor_RegionConfig{
			PrimaryRegion: "us-east-1",
		},
	}).BuildImmutable()

	defaultPrivileges := descpb.NewDefaultPrivilegeDescriptor(security.RootUserName())
	invalidPrivileges := descpb.NewDefaultPrivilegeDescriptor(security.RootUserName())
	// Make the PrivilegeDescriptor invalid by granting SELECT to a type.
	invalidPrivileges.Grant(security.TestUserName(), privilege.List{privilege.SELECT})
	typeDescID := descpb.ID(keys.MaxReservedDescID + 1)
	testData := []struct {
		err  string
		desc descpb.TypeDescriptor
	}{
		{
			`empty type name`,
			descpb.TypeDescriptor{
				Privileges: defaultPrivileges,
			},
		},
		{
			`invalid ID 0`,
			descpb.TypeDescriptor{
				Name:       "t",
				Privileges: defaultPrivileges,
			},
		},
		{
			`invalid parentID 0`,
			descpb.TypeDescriptor{
				Name: "t", ID: typeDescID,
				Privileges: defaultPrivileges,
			},
		},

		{
			`invalid parent schema ID 0`,
			descpb.TypeDescriptor{
				Name:       "t",
				ID:         typeDescID,
				ParentID:   100,
				Privileges: defaultPrivileges,
			},
		},
		{
			`enum members are not sorted [{[2] a ALL NONE} {[1] b ALL NONE}]`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "a",
						PhysicalRepresentation: []byte{2},
					},
					{
						LogicalRepresentation:  "b",
						PhysicalRepresentation: []byte{1},
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`duplicate enum physical rep [1]`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "a",
						PhysicalRepresentation: []byte{1},
					},
					{
						LogicalRepresentation:  "b",
						PhysicalRepresentation: []byte{1},
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`duplicate enum physical rep [1]`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east-1",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{1},
					},
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{1},
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`duplicate enum member "a"`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "a",
						PhysicalRepresentation: []byte{1},
					},
					{
						LogicalRepresentation:  "a",
						PhysicalRepresentation: []byte{2},
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`duplicate enum member "us-east-1"`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{1},
					},
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{2},
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`read only capability member must have transition direction set`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "a",
						PhysicalRepresentation: []byte{1},
						Capability:             descpb.TypeDescriptor_EnumMember_READ_ONLY,
						Direction:              descpb.TypeDescriptor_EnumMember_NONE,
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`public enum member can not have transition direction set`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "a",
						PhysicalRepresentation: []byte{1},
						Capability:             descpb.TypeDescriptor_EnumMember_ALL,
						Direction:              descpb.TypeDescriptor_EnumMember_ADD,
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`public enum member can not have transition direction set`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east1",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east1",
						PhysicalRepresentation: []byte{1},
						Capability:             descpb.TypeDescriptor_EnumMember_ALL,
						Direction:              descpb.TypeDescriptor_EnumMember_REMOVE,
					},
				},
				Privileges: defaultPrivileges,
			},
		},
		{
			`ALIAS type desc has nil alias type`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ALIAS,
				Privileges:     defaultPrivileges,
			},
		},
		{
			`referenced database ID 500: descriptor not found`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       500,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_ALIAS,
				Alias:          types.Int,
				Privileges:     defaultPrivileges,
			},
		},
		{
			`referenced schema ID 500: descriptor not found`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: 500,
				Kind:           descpb.TypeDescriptor_ALIAS,
				Alias:          types.Int,
				Privileges:     defaultPrivileges,
			},
		},
		{
			`arrayTypeID 500 does not exist for "ENUM": referenced type ID 500: descriptor not found`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: 101,
				Kind:           descpb.TypeDescriptor_ENUM,
				ArrayTypeID:    500,
				Privileges:     defaultPrivileges,
			},
		},
		{
			`arrayTypeID 500 does not exist for "MULTIREGION_ENUM": referenced type ID 500: descriptor not found`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east-1",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{1},
					},
				},
				ArrayTypeID: 500,
				Privileges:  defaultPrivileges,
			},
		},
		{
			"referenced table ID 500: descriptor not found",
			descpb.TypeDescriptor{
				Name:                     "t",
				ID:                       typeDescID,
				ParentID:                 100,
				ParentSchemaID:           keys.PublicSchemaID,
				Kind:                     descpb.TypeDescriptor_ENUM,
				ArrayTypeID:              102,
				ReferencingDescriptorIDs: []descpb.ID{500},
				Privileges:               defaultPrivileges,
			},
		},
		{
			"referenced table ID 500: descriptor not found",
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east-1",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{1},
					},
				},
				ArrayTypeID:              102,
				ReferencingDescriptorIDs: []descpb.ID{500},
				Privileges:               defaultPrivileges,
			},
		},
		{
			"user testuser must not have SELECT privileges on type with ID=50",
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: 101,
				Kind:           descpb.TypeDescriptor_ENUM,
				ArrayTypeID:    102,
				Privileges:     invalidPrivileges,
			},
		},
		{
			`found region config on ENUM type desc`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       100,
				ParentSchemaID: 101,
				Kind:           descpb.TypeDescriptor_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east-1",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "foo",
						PhysicalRepresentation: []byte{2},
					},
				},
				ArrayTypeID: 102,
				Privileges:  defaultPrivileges,
			},
		},
		{
			`no region config on MULTIREGION_ENUM type desc`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{2},
					},
				},
				ArrayTypeID: 102,
				Privileges:  defaultPrivileges,
			},
		},
		{
			`unexpected primary region on db desc: "us-east-1" expected "us-east-2"`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east-2",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-2",
						PhysicalRepresentation: []byte{2},
					},
				},
				ArrayTypeID: 102,
				Privileges:  defaultPrivileges,
			},
		},
		{
			`primary region "us-east-2" not found in list of enum members`,
			descpb.TypeDescriptor{
				Name:           "t",
				ID:             typeDescID,
				ParentID:       200,
				ParentSchemaID: keys.PublicSchemaID,
				Kind:           descpb.TypeDescriptor_MULTIREGION_ENUM,
				RegionConfig: &descpb.TypeDescriptor_RegionConfig{
					PrimaryRegion: "us-east-2",
				},
				EnumMembers: []descpb.TypeDescriptor_EnumMember{
					{
						LogicalRepresentation:  "us-east-1",
						PhysicalRepresentation: []byte{2},
					},
				},
				ArrayTypeID: 102,
				Privileges:  defaultPrivileges,
			},
		},
	}

	for i, test := range testData {
		desc := typedesc.NewBuilder(&test.desc).BuildImmutable()
		expectedErr := fmt.Sprintf("%s %q (%d): %s", desc.DescriptorType(), desc.GetName(), desc.GetID(), test.err)
		if err := catalog.ValidateSelfAndCrossReferences(ctx, descs, desc); err == nil {
			t.Errorf("#%d expected err: %s but found nil: %v", i, expectedErr, test.desc)
		} else if expectedErr != err.Error() {
			t.Errorf("#%d expected err: %s but found: %s", i, expectedErr, err)
		}
	}
}
