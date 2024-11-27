// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestMaybeIncrementVersion(t *testing.T) {
	// Created descriptors should not have their version incremented.
	t.Run("created does not get incremented", func(t *testing.T) {
		{
			mut := NewBuilder(&descpb.TableDescriptor{
				ID:      1,
				Version: 1,
			}).BuildCreatedMutableTable()
			mut.MaybeIncrementVersion()
			require.Equal(t, descpb.DescriptorVersion(1), mut.GetVersion())
		}
		{
			mut := NewBuilder(&descpb.TableDescriptor{
				ID:      1,
				Version: 42,
			}).BuildCreatedMutableTable()
			mut.MaybeIncrementVersion()
			require.Equal(t, descpb.DescriptorVersion(42), mut.GetVersion())
		}
	})
	t.Run("existed gets incremented once", func(t *testing.T) {
		mut := NewBuilder(&descpb.TableDescriptor{
			ID:      1,
			Version: 1,
		}).BuildExistingMutableTable()
		require.Equal(t, descpb.DescriptorVersion(1), mut.GetVersion())
		mut.MaybeIncrementVersion()
		require.Equal(t, descpb.DescriptorVersion(2), mut.GetVersion())
		mut.MaybeIncrementVersion()
		require.Equal(t, descpb.DescriptorVersion(2), mut.GetVersion())
	})
}

// TestingSetClusterVersion is a test helper to override the original table
// descriptor.
func (desc *Mutable) TestingSetClusterVersion(d descpb.TableDescriptor) {
	desc.original = makeImmutable(&d)
}

func TestStripDanglingBackReferencesAndRoles(t *testing.T) {
	type testCase struct {
		name                           string
		input, expectedOutput          descpb.TableDescriptor
		validDescIDs                   catalog.DescriptorIDSet
		validJobIDs                    map[jobspb.JobID]struct{}
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
			name: "descriptor IDs",
			input: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 12345}, {ID: 105}, {ID: 5678},
				},
				SequenceOpts: &descpb.TableDescriptor_SequenceOpts{
					SequenceOwner: descpb.TableDescriptor_SequenceOpts_SequenceOwner{
						OwnerTableID: 12345,
					},
				},
				InboundFKs: []descpb.ForeignKeyConstraint{
					{OriginTableID: 12345, ReferencedTableID: 104},
					{OriginTableID: 12345, ReferencedTableID: 12345},
					{OriginTableID: 105, ReferencedTableID: 104},
				},
				ReplacementOf: descpb.TableDescriptor_Replacement{ID: 12345},
				Privileges:    badPrivilege,
			},
			expectedOutput: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				DependedOnBy: []descpb.TableDescriptor_Reference{
					{ID: 105},
				},
				SequenceOpts: &descpb.TableDescriptor_SequenceOpts{},
				InboundFKs: []descpb.ForeignKeyConstraint{
					{OriginTableID: 105},
				},
				Privileges: goodPrivilege,
			},
			validDescIDs:                   catalog.MakeDescriptorIDSet(100, 101, 104, 105),
			validJobIDs:                    map[jobspb.JobID]struct{}{},
			strippedDanglingBackReferences: true,
			strippedNonExistentRoles:       true,
		},
		{
			name: "job IDs",
			input: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{JobID: 1, MutationID: 1},
					{JobID: 111222333444, MutationID: 1},
					{JobID: 2, MutationID: 2},
				},
				Mutations: []descpb.DescriptorMutation{
					{MutationID: 1},
					{MutationID: 2},
				},
				DropJobID:  1,
				Privileges: badPrivilege,
			},
			expectedOutput: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{JobID: 111222333444, MutationID: 1},
				},
				Mutations: []descpb.DescriptorMutation{
					{MutationID: 1},
					{MutationID: 2},
				},
				Privileges: goodPrivilege,
			},
			validDescIDs:                   catalog.MakeDescriptorIDSet(100, 101, 104, 105),
			validJobIDs:                    map[jobspb.JobID]struct{}{111222333444: {}},
			strippedDanglingBackReferences: true,
			strippedNonExistentRoles:       true,
		},
		{
			name: "LDR job IDs",
			input: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{JobID: 111222333444, MutationID: 1},
				},
				Mutations: []descpb.DescriptorMutation{
					{MutationID: 1},
					{MutationID: 2},
				},
				LDRJobIDs:  []catpb.JobID{1, 2, 3},
				Privileges: goodPrivilege,
			},
			expectedOutput: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{JobID: 111222333444, MutationID: 1},
				},
				Mutations: []descpb.DescriptorMutation{
					{MutationID: 1},
					{MutationID: 2},
				},
				LDRJobIDs:  []catpb.JobID{},
				Privileges: goodPrivilege,
			},
			validDescIDs:                   catalog.MakeDescriptorIDSet(100, 101, 104, 105),
			validJobIDs:                    map[jobspb.JobID]struct{}{111222333444: {}},
			strippedDanglingBackReferences: true,
			strippedNonExistentRoles:       false,
		},
		{
			name: "missing owner",
			input: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{JobID: 111222333444, MutationID: 1},
				},
				Mutations: []descpb.DescriptorMutation{
					{MutationID: 1},
					{MutationID: 2},
				},
				Privileges: badOwnerPrivilege,
			},
			expectedOutput: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				MutationJobs: []descpb.TableDescriptor_MutationJob{
					{JobID: 111222333444, MutationID: 1},
				},
				Mutations: []descpb.DescriptorMutation{
					{MutationID: 1},
					{MutationID: 2},
				},
				Privileges: goodOwnerPrivilege,
			},
			validDescIDs:             catalog.MakeDescriptorIDSet(100, 101, 104, 105),
			validJobIDs:              map[jobspb.JobID]struct{}{111222333444: {}},
			strippedNonExistentRoles: true,
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			b := NewBuilder(&test.input)
			require.NoError(t, b.RunPostDeserializationChanges())
			out := NewBuilder(&test.expectedOutput)
			require.NoError(t, out.RunPostDeserializationChanges())
			require.NoError(t, b.StripDanglingBackReferences(test.validDescIDs.Contains, func(id jobspb.JobID) bool {
				_, ok := test.validJobIDs[id]
				return ok
			}))
			require.NoError(t, b.StripNonExistentRoles(func(role username.SQLUsername) bool {
				return role.IsAdminRole() || role.IsPublicRole() || role.IsRootUser()
			}))
			desc := b.BuildCreatedMutableTable()
			require.Equal(t, test.strippedDanglingBackReferences, desc.GetPostDeserializationChanges().Contains(catalog.StrippedDanglingBackReferences))
			require.Equal(t, test.strippedNonExistentRoles, desc.GetPostDeserializationChanges().Contains(catalog.StrippedNonExistentRoles))
			require.Equal(t, out.BuildCreatedMutableTable().TableDesc(), desc.TableDesc())
		})
	}
}

func TestFixIncorrectFKOriginTableID(t *testing.T) {
	type testCase struct {
		name                  string
		input, expectedOutput descpb.TableDescriptor
	}
	testData := []testCase{
		{
			name: "incorrect FK Origin Table ID",
			input: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				InboundFKs: []descpb.ForeignKeyConstraint{
					{OriginTableID: 32, ReferencedTableID: 1},
					{OriginTableID: 64, ReferencedTableID: 2},
				},
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{OriginTableID: 1, ReferencedTableID: 32},
					{OriginTableID: 2, ReferencedTableID: 64},
				},
			},
			expectedOutput: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				InboundFKs: []descpb.ForeignKeyConstraint{
					{OriginTableID: 32, ReferencedTableID: 104},
					{OriginTableID: 64, ReferencedTableID: 104},
				},
				OutboundFKs: []descpb.ForeignKeyConstraint{
					{OriginTableID: 104, ReferencedTableID: 32},
					{OriginTableID: 104, ReferencedTableID: 64},
				},
			},
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			b := NewBuilder(&test.input)
			require.NoError(t, b.RunPostDeserializationChanges())
			out := NewBuilder(&test.expectedOutput)
			require.NoError(t, out.RunPostDeserializationChanges())
			desc := b.BuildCreatedMutableTable()
			require.True(t, desc.GetPostDeserializationChanges().Contains(catalog.FixedIncorrectForeignKeyOrigins))
			require.False(t, out.BuildCreatedMutableTable().GetPostDeserializationChanges().Contains(catalog.FixedIncorrectForeignKeyOrigins))
			require.Equal(t, out.BuildCreatedMutableTable().TableDesc(), desc.TableDesc())
		})
	}
}

func TestFixMissingSequenceIdentityRefs(t *testing.T) {
	type testCase struct {
		name                  string
		input, expectedOutput descpb.TableDescriptor
	}
	defaultExpr := "nextval('sq1')"
	testData := []testCase{
		{
			name: "missing sequence references for identity",
			input: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				Columns: []descpb.ColumnDescriptor{{
					Name:                    "blah",
					ID:                      1,
					Type:                    types.Int,
					GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
					DefaultExpr:             &defaultExpr,
					OwnsSequenceIds:         []descpb.ID{23},
					UsesSequenceIds:         nil,
				},
				},
			},
			expectedOutput: descpb.TableDescriptor{
				Name: "foo",
				ID:   104,
				Columns: []descpb.ColumnDescriptor{{
					Name:                    "blah",
					ID:                      1,
					Type:                    types.Int,
					GeneratedAsIdentityType: catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
					DefaultExpr:             &defaultExpr,
					OwnsSequenceIds:         []descpb.ID{23},
					UsesSequenceIds:         []descpb.ID{23},
				},
				},
			},
		},
	}

	for _, test := range testData {
		t.Run(test.name, func(t *testing.T) {
			b := NewBuilder(&test.input)
			require.NoError(t, b.RunPostDeserializationChanges())
			out := NewBuilder(&test.expectedOutput)
			require.NoError(t, out.RunPostDeserializationChanges())
			desc := b.BuildCreatedMutableTable()
			require.True(t, desc.GetPostDeserializationChanges().Contains(catalog.FixedUsesSequencesIDForIdentityColumns))
			require.False(t, out.BuildCreatedMutableTable().GetPostDeserializationChanges().Contains(catalog.FixedUsesSequencesIDForIdentityColumns))
			require.Equal(t, out.BuildCreatedMutableTable().TableDesc(), desc.TableDesc())
		})
	}
}
