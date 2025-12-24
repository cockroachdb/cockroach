// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	catpb "github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/stretchr/testify/require"
)

// TestDeprecatedIsInvertedMigration tests that the IsInverted field is migrated
// to the new Type field.
func TestDeprecatedIsInvertedMigration(t *testing.T) {
	state := DescriptorState{
		Targets: []Target{
			MakeTarget(ToPublic,
				&PrimaryIndex{Index: Index{
					TableID:    1,
					IndexID:    1,
					IsInverted: true,
				}},
				nil,
			),
			MakeTarget(ToPublic,
				&SecondaryIndex{Index: Index{
					TableID:    1,
					IndexID:    1,
					IsInverted: true,
				}},
				nil,
			),
			MakeTarget(ToPublic,
				&TemporaryIndex{Index: Index{
					TableID:    1,
					IndexID:    1,
					IsInverted: true,
				}},
				nil,
			),
		},
		CurrentStatuses: []Status{Status_PUBLIC, Status_PUBLIC, Status_PUBLIC},
		TargetRanks:     []uint32{1, 2, 3},
	}
	migrationOccurred := MigrateDescriptorState(
		clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()},
		1,
		&state,
	)
	require.True(t, migrationOccurred)
	require.Len(t, state.Targets, 4)

	primary := state.Targets[0].GetPrimaryIndex()
	require.True(t, primary.IsInverted)
	require.Equal(t, idxtype.INVERTED, primary.Type)

	secondary := state.Targets[1].GetSecondaryIndex()
	require.True(t, secondary.IsInverted)
	require.Equal(t, idxtype.INVERTED, secondary.Type)

	temp := state.Targets[2].GetTemporaryIndex()
	require.True(t, temp.IsInverted)
	require.Equal(t, idxtype.INVERTED, temp.Type)
}

// TestDeprecatedTriggerDeps tests that the relation IDs in TriggerDeps are
// migrated to RelationReferences.
func TestDeprecatedTriggerDeps(t *testing.T) {
	state := DescriptorState{
		Targets: []Target{
			MakeTarget(ToPublic,
				&TriggerDeps{
					UsesRelationIDs: []catid.DescID{112, 113},
				},
				nil,
			),
		},
		CurrentStatuses: []Status{Status_PUBLIC},
		TargetRanks:     []uint32{1},
	}
	migrationOccurred := MigrateDescriptorState(
		clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()},
		1,
		&state,
	)
	require.True(t, migrationOccurred)
	require.Len(t, state.Targets, 1)

	triggerDeps := state.Targets[0].GetTriggerDeps()
	require.Nil(t, triggerDeps.UsesRelationIDs)
	require.NotNil(t, triggerDeps.UsesRelations)
	require.Len(t, triggerDeps.UsesRelations, 2)
	require.Equal(t, catid.DescID(112), triggerDeps.UsesRelations[0].ID)
	require.Equal(t, catid.DescID(113), triggerDeps.UsesRelations[1].ID)
}

// TestDeprecatedColumnGeneratedAsIdentity will ensure that a ColumnGeneratedAsIdentity
// element is added.
func TestDeprecatedColumnGeneratedAsIdentity(t *testing.T) {
	state := DescriptorState{
		Targets: []Target{
			MakeTarget(ToPublic,
				&Column{
					TableID:                           100,
					GeneratedAsIdentityType:           catpb.GeneratedAsIdentityType_GENERATED_ALWAYS,
					GeneratedAsIdentitySequenceOption: "Hello",
				},
				nil,
			),
		},
		CurrentStatuses: []Status{Status_PUBLIC},
		TargetRanks:     []uint32{1},
	}
	migrationOccurred := MigrateDescriptorState(
		clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()},
		1,
		&state,
	)
	require.True(t, migrationOccurred)
	require.Len(t, state.CurrentStatuses, 2)
	require.Len(t, state.Targets, 2)
	require.NotNil(t, state.Targets[0].GetColumn())
	col := state.Targets[0].GetColumn()
	require.Equal(t, col.GeneratedAsIdentityType, catpb.GeneratedAsIdentityType_NOT_IDENTITY_COLUMN)
	require.Equal(t, col.GeneratedAsIdentitySequenceOption, "")
	cge := state.Targets[1].GetColumnGeneratedAsIdentity()
	require.NotNil(t, cge)
	require.Equal(t, cge.TableID, col.TableID)
	require.Equal(t, cge.ColumnID, col.ColumnID)
	require.Equal(t, cge.Type, catpb.GeneratedAsIdentityType_GENERATED_ALWAYS)
	require.Equal(t, cge.SequenceOption, "Hello")
	require.Equal(t, state.CurrentStatuses[1], Status_PUBLIC)
	require.Equal(t, state.TargetRanks[1], uint32(2))
}

func TestDeprecatedColumnHiddenMigration(t *testing.T) {
	state := DescriptorState{
		Targets: []Target{
			MakeTarget(ToPublic,
				&Column{
					TableID:  100,
					ColumnID: 105,
					IsHidden: true,
				},
				nil,
			),
		},
		CurrentStatuses: []Status{Status_PUBLIC},
		TargetRanks:     []uint32{1},
	}
	migrationOccurred := MigrateDescriptorState(
		clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()},
		1,
		&state,
	)
	require.True(t, migrationOccurred)
	require.Len(t, state.CurrentStatuses, 2)
	require.Len(t, state.Targets, 2)

	column := state.Targets[0].GetColumn()
	require.NotNil(t, column)
	require.False(t, column.IsHidden)

	columnHidden := state.Targets[1].GetColumnHidden()
	require.NotNil(t, columnHidden)

	require.Equal(t, column.TableID, columnHidden.TableID)
	require.Equal(t, column.ColumnID, columnHidden.ColumnID)
	require.Equal(t, state.CurrentStatuses[1], Status_PUBLIC)
	require.Equal(t, state.TargetRanks[1], uint32(2))
}
