// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/stretchr/testify/require"
)

func TestDeprecatedSecondaryIndexPartialMigration(t *testing.T) {
	state := DescriptorState{
		Targets: []Target{
			MakeTarget(ToPublic,
				&SecondaryIndexPartial{
					TableID: 100,
					IndexID: 100,
					Expression: Expression{
						Expr: catpb.Expression("C1 > 10"),
					},
				},
				nil,
			),
			MakeTarget(ToPublic,
				&SecondaryIndex{
					Index: Index{
						TableID: 100,
						IndexID: 100,
					},
				},
				nil),
		},
		CurrentStatuses: []Status{Status_PUBLIC, Status_PUBLIC},
		TargetRanks:     []uint32{1, 2},
	}
	migrationOccurred := MigrateDescriptorState(
		clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()},
		1,
		&state,
	)
	require.True(t, migrationOccurred)
	// Verify the SecondaryIndex was removed. We should still have 2 targets
	// though as we would also have injected a TableData element.
	require.Len(t, state.CurrentStatuses, 2)
	require.Len(t, state.Targets, 2)
	require.NotNil(t, state.Targets[0].GetSecondaryIndex())
	require.NotNil(t, state.Targets[0].GetSecondaryIndex().EmbeddedExpr)
	require.Equal(t, state.Targets[0].GetSecondaryIndex().EmbeddedExpr.Expr, catpb.Expression("C1 > 10"))
	require.NotNil(t, state.Targets[1].GetTableData()) // Will be injected because we are adding an index
}

// TestDeprecatedColumnComputeFieldMigration will ensure that ComputeExpr in
// ColumnType is nulled out and a new ColumnComputeExpression is added.
func TestDeprecatedColumnComputeFieldMigration(t *testing.T) {
	state := DescriptorState{
		Targets: []Target{
			MakeTarget(ToPublic,
				&ColumnType{
					TableID: 100,
					ComputeExpr: &Expression{
						Expr: catpb.Expression("Hello"),
					},
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
	require.NotNil(t, state.Targets[0].GetColumnType())
	ct := state.Targets[0].GetColumnType()
	require.Nil(t, ct.ComputeExpr)
	require.NotNil(t, state.Targets[1].GetColumnComputeExpression())
	cce := state.Targets[1].GetColumnComputeExpression()
	require.Equal(t, cce.TableID, ct.TableID)
	require.Equal(t, cce.ColumnID, ct.ColumnID)
	require.Equal(t, cce.Expr, catpb.Expression("Hello"))
	require.Equal(t, state.CurrentStatuses[1], Status_PUBLIC)
	require.Equal(t, state.TargetRanks[1], uint32(2))
}

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
