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
			err: `"b" is not an enum`,
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
	}

	for _, test := range tests {
		a := typedesc.NewImmutable(test.a)
		b := typedesc.NewImmutable(test.b)
		err := a.IsCompatibleWith(b)
		if test.err == "" {
			require.NoError(t, err)
		} else {
			if !testutils.IsError(err, test.err) {
				t.Errorf("expected error %s, but found %s", test.err, err)
			}
		}
	}
}

func TestValidateTypeDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	descs := catalog.MapDescGetter{}
	descs[100] = dbdesc.NewImmutable(descpb.DatabaseDescriptor{
		Name: "db",
		ID:   100,
	})
	descs[101] = schemadesc.NewImmutable(descpb.SchemaDescriptor{
		ID:   101,
		Name: "schema",
	})
	descs[102] = typedesc.NewImmutable(descpb.TypeDescriptor{
		ID:   102,
		Name: "type",
	})

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
			`enum members are not sorted [{[2] a ALL} {[1] b ALL}]`,
			descpb.TypeDescriptor{
				Name:     "t",
				ID:       typeDescID,
				ParentID: 1,
				Kind:     descpb.TypeDescriptor_ENUM,
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
				Name:     "t",
				ID:       typeDescID,
				ParentID: 1,
				Kind:     descpb.TypeDescriptor_ENUM,
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
			`duplicate enum member "a"`,
			descpb.TypeDescriptor{
				Name:     "t",
				ID:       typeDescID,
				ParentID: 1,
				Kind:     descpb.TypeDescriptor_ENUM,
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
			`ALIAS type desc has nil alias type`,
			descpb.TypeDescriptor{
				Name:       "t",
				ID:         typeDescID,
				ParentID:   1,
				Kind:       descpb.TypeDescriptor_ALIAS,
				Privileges: defaultPrivileges,
			},
		},
		{
			`parentID 500 does not exist`,
			descpb.TypeDescriptor{
				Name:       "t",
				ID:         typeDescID,
				ParentID:   500,
				Kind:       descpb.TypeDescriptor_ALIAS,
				Alias:      types.Int,
				Privileges: defaultPrivileges,
			},
		},
		{
			`parentSchemaID 500 does not exist`,
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
			"arrayTypeID 500 does not exist",
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
			"referencing descriptor 500 does not exist",
			descpb.TypeDescriptor{
				Name:                     "t",
				ID:                       typeDescID,
				ParentID:                 100,
				ParentSchemaID:           101,
				Kind:                     descpb.TypeDescriptor_ENUM,
				ArrayTypeID:              102,
				ReferencingDescriptorIDs: []descpb.ID{500},
				Privileges:               defaultPrivileges,
			},
		},
		{
			"user testuser must not have SELECT privileges on system type with ID=50",
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
	}

	for _, test := range testData {
		desc := typedesc.NewImmutable(test.desc)
		if err := desc.Validate(ctx, descs); err == nil {
			t.Errorf("expected err: %s but found nil: %v", test.err, test.desc)
		} else if test.err != err.Error() && "internal error: "+test.err != err.Error() {
			t.Errorf("expected err: %s but found: %s", test.err, err)
		}
	}
}
