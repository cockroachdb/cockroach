// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbackup

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const testTableID catid.DescID = 104

type testTarget struct {
	elem    scpb.Element
	target  scpb.Status
	current scpb.Status
	linked  bool
}

func linked(elem scpb.Element, target, current scpb.Status) testTarget {
	return testTarget{elem: elem, target: target, current: current, linked: true}
}

func primary(id catid.IndexID) *scpb.PrimaryIndex {
	return &scpb.PrimaryIndex{Index: scpb.Index{TableID: testTableID, IndexID: id}}
}

func secondary(id, recreateSource catid.IndexID) *scpb.SecondaryIndex {
	return &scpb.SecondaryIndex{
		Index:                 scpb.Index{TableID: testTableID, IndexID: id},
		RecreateSourceIndexID: recreateSource,
	}
}

func temp(id catid.IndexID) *scpb.TemporaryIndex {
	return &scpb.TemporaryIndex{Index: scpb.Index{TableID: testTableID, IndexID: id}}
}

func indexName(id catid.IndexID) *scpb.IndexName {
	return &scpb.IndexName{TableID: testTableID, IndexID: id, Name: "idx"}
}

func makeState(revertible bool, targets ...testTarget) *scpb.DescriptorState {
	ds := &scpb.DescriptorState{Revertible: revertible}
	for i, tt := range targets {
		var md *scpb.TargetMetadata
		if tt.linked {
			md = &scpb.TargetMetadata{SourceElementID: 1, SubWorkID: 1}
		}
		ds.Targets = append(ds.Targets, scpb.MakeTarget(scpb.AsTargetStatus(tt.target), tt.elem, md))
		ds.CurrentStatuses = append(ds.CurrentStatuses, tt.current)
		ds.TargetRanks = append(ds.TargetRanks, uint32(i))
	}
	return ds
}

func restored(ids ...catid.IndexID) map[catid.IndexID]bool {
	m := make(map[catid.IndexID]bool)
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func TestDependsOnUnrestoredIndexData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		state    *scpb.DescriptorState
		restored map[catid.IndexID]bool
		expected bool
	}{
		{
			name:     "backfilled index would be promoted empty",
			state:    makeState(false, linked(primary(4), scpb.Status_PUBLIC, scpb.Status_VALIDATED)),
			restored: restored(2),
			expected: true,
		},
		{
			name:     "merged secondary index",
			state:    makeState(true, linked(secondary(3, 0), scpb.Status_PUBLIC, scpb.Status_MERGED)),
			restored: restored(1),
			expected: true,
		},
		{
			name:     "index not yet backfilled",
			state:    makeState(true, linked(primary(4), scpb.Status_PUBLIC, scpb.Status_BACKFILL_ONLY)),
			restored: restored(1),
			expected: false,
		},
		{
			name:     "temp index capturing writes",
			state:    makeState(true, linked(temp(5), scpb.Status_TRANSIENT_ABSENT, scpb.Status_WRITE_ONLY)),
			restored: restored(1),
			expected: false,
		},
		{
			name: "middle primary on its way out",
			state: makeState(false,
				linked(primary(2), scpb.Status_TRANSIENT_ABSENT, scpb.Status_TRANSIENT_DELETE_ONLY)),
			restored: restored(4),
			expected: false,
		},
		{
			name:     "demoted index a rollback would revive",
			state:    makeState(true, linked(primary(1), scpb.Status_ABSENT, scpb.Status_VALIDATED)),
			restored: restored(2),
			expected: true,
		},
		{
			name:     "demoted index past the point of no return",
			state:    makeState(false, linked(primary(1), scpb.Status_ABSENT, scpb.Status_DELETE_ONLY)),
			restored: restored(2),
			expected: false,
		},
		{
			name:     "public index being dropped and restored",
			state:    makeState(true, linked(secondary(2, 0), scpb.Status_ABSENT, scpb.Status_PUBLIC)),
			restored: restored(1, 2),
			expected: false,
		},
		{
			name: "unlinked target",
			state: makeState(true, testTarget{
				elem: primary(4), target: scpb.Status_PUBLIC, current: scpb.Status_VALIDATED,
			}),
			restored: restored(1),
			expected: false,
		},
		{
			name:     "restored public index",
			state:    makeState(true, linked(primary(2), scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC)),
			restored: restored(2),
			expected: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, dependsOnUnrestoredIndexData(tc.state, tc.restored))
		})
	}
}

func TestSettle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	targetsOf := func(ds *scpb.DescriptorState) []scpb.Status {
		var out []scpb.Status
		for _, tgt := range ds.Targets {
			out = append(out, tgt.TargetStatus)
		}
		return out
	}

	tests := []struct {
		name            string
		state           *scpb.DescriptorState
		expectedTargets []scpb.Status
	}{
		{
			name: "public kept, non-public dropped",
			state: makeState(true,
				linked(primary(2), scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(primary(1), scpb.Status_ABSENT, scpb.Status_VALIDATED),
				linked(primary(4), scpb.Status_PUBLIC, scpb.Status_MERGE_ONLY),
			),
			expectedTargets: []scpb.Status{scpb.Status_PUBLIC, scpb.Status_ABSENT, scpb.Status_ABSENT},
		},
		{
			name: "public transient check constraint dropped",
			state: makeState(false,
				linked(&scpb.CheckConstraint{TableID: testTableID, ConstraintID: 2, IndexIDForValidation: 4},
					scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(primary(4), scpb.Status_PUBLIC, scpb.Status_VALIDATED),
			),
			expectedTargets: []scpb.Status{scpb.Status_ABSENT, scpb.Status_ABSENT},
		},
		{
			name: "public transient parts of a public intermediate primary index kept",
			state: makeState(false,
				linked(primary(2), scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 2, ColumnID: 1, Kind: scpb.IndexColumn_KEY,
				}, scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.IndexData{TableID: testTableID, IndexID: 2},
					scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(primary(4), scpb.Status_PUBLIC, scpb.Status_VALIDATED),
			),
			expectedTargets: []scpb.Status{
				scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_ABSENT,
			},
		},
		{
			name: "transient public target kept",
			state: makeState(true,
				linked(&scpb.TableSchemaLocked{TableID: testTableID},
					scpb.Status_TRANSIENT_PUBLIC, scpb.Status_ABSENT),
			),
			expectedTargets: []scpb.Status{scpb.Status_TRANSIENT_PUBLIC},
		},
		{
			name: "unlinked target untouched",
			state: makeState(true,
				testTarget{elem: primary(9), target: scpb.Status_PUBLIC, current: scpb.Status_BACKFILL_ONLY},
			),
			expectedTargets: []scpb.Status{scpb.Status_PUBLIC},
		},
		{
			name: "public parts of a dropped column dropped with it",
			state: makeState(true,
				linked(&scpb.Column{TableID: testTableID, ColumnID: 2},
					scpb.Status_PUBLIC, scpb.Status_WRITE_ONLY),
				linked(&scpb.ColumnName{TableID: testTableID, ColumnID: 2, Name: "l"},
					scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(&scpb.ColumnDefaultExpression{TableID: testTableID, ColumnID: 2},
					scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{scpb.Status_ABSENT, scpb.Status_ABSENT, scpb.Status_ABSENT},
		},
		{
			name: "key column of a kept primary index kept with its NOT NULL",
			state: makeState(true,
				linked(&scpb.Column{TableID: testTableID, ColumnID: 5},
					scpb.Status_PUBLIC, scpb.Status_WRITE_ONLY),
				linked(&scpb.ColumnNotNull{TableID: testTableID, ColumnID: 5, IndexIDForValidation: 12},
					scpb.Status_PUBLIC, scpb.Status_WRITE_ONLY),
				linked(primary(10), scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 10, ColumnID: 5, Kind: scpb.IndexColumn_KEY,
				}, scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(secondary(6, 2), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 6, ColumnID: 5, Kind: scpb.IndexColumn_KEY_SUFFIX,
				}, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(secondary(2, 0), scpb.Status_ABSENT, scpb.Status_VALIDATED),
			),
			expectedTargets: []scpb.Status{
				scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_PUBLIC,
				scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_ABSENT,
			},
		},
		{
			name: "NOT NULL being dropped from a key column of a kept primary index kept",
			state: makeState(true,
				linked(&scpb.Column{TableID: testTableID, ColumnID: 3},
					scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.ColumnNotNull{TableID: testTableID, ColumnID: 3},
					scpb.Status_ABSENT, scpb.Status_VALIDATED),
				linked(primary(1), scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 1, ColumnID: 3, Kind: scpb.IndexColumn_KEY,
				}, scpb.Status_ABSENT, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{
				scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_PUBLIC, scpb.Status_PUBLIC,
			},
		},
		{
			name: "secondary index on a column not public at backup dropped with its parts",
			state: makeState(true,
				linked(&scpb.Column{TableID: testTableID, ColumnID: 4},
					scpb.Status_PUBLIC, scpb.Status_WRITE_ONLY),
				linked(primary(1), scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(secondary(3, 0), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 3, ColumnID: 4, Kind: scpb.IndexColumn_KEY,
				}, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(indexName(3), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{
				scpb.Status_ABSENT, scpb.Status_PUBLIC, scpb.Status_ABSENT, scpb.Status_ABSENT,
				scpb.Status_ABSENT,
			},
		},
		{
			name: "secondary index storing a column not public at backup dropped with its parts",
			state: makeState(false,
				linked(&scpb.Column{TableID: testTableID, ColumnID: 4},
					scpb.Status_PUBLIC, scpb.Status_WRITE_ONLY),
				linked(primary(1), scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(secondary(3, 0), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 3, ColumnID: 4, Kind: scpb.IndexColumn_STORED,
				}, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(indexName(3), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{
				scpb.Status_ABSENT, scpb.Status_PUBLIC, scpb.Status_ABSENT, scpb.Status_ABSENT,
				scpb.Status_ABSENT,
			},
		},
		{
			name: "enum value being dropped kept with its kept type",
			state: makeState(true,
				linked(&scpb.EnumType{TypeID: testTableID}, scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.EnumTypeValue{TypeID: testTableID, LogicalRepresentation: "red"},
					scpb.Status_ABSENT, scpb.Status_VALIDATED),
			),
			expectedTargets: []scpb.Status{scpb.Status_PUBLIC, scpb.Status_PUBLIC},
		},
		{
			name: "enum value being added dropped",
			state: makeState(true,
				linked(&scpb.EnumType{TypeID: testTableID}, scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(&scpb.EnumTypeValue{TypeID: testTableID, LogicalRepresentation: "red"},
					scpb.Status_PUBLIC, scpb.Status_WRITE_ONLY),
			),
			expectedTargets: []scpb.Status{scpb.Status_PUBLIC, scpb.Status_ABSENT},
		},
		{
			name: "enum value of a dropped type dropped",
			state: makeState(true,
				linked(&scpb.EnumType{TypeID: testTableID}, scpb.Status_ABSENT, scpb.Status_DROPPED),
				linked(&scpb.EnumTypeValue{TypeID: testTableID, LogicalRepresentation: "red"},
					scpb.Status_ABSENT, scpb.Status_VALIDATED),
			),
			expectedTargets: []scpb.Status{scpb.Status_ABSENT, scpb.Status_ABSENT},
		},
		{
			name: "stored column of a kept index dropped",
			state: makeState(true,
				linked(&scpb.Column{TableID: testTableID, ColumnID: 2},
					scpb.Status_ABSENT, scpb.Status_WRITE_ONLY),
				linked(primary(1), scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.IndexColumn{
					TableID: testTableID, IndexID: 1, ColumnID: 2, Kind: scpb.IndexColumn_STORED,
				}, scpb.Status_ABSENT, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{scpb.Status_ABSENT, scpb.Status_PUBLIC, scpb.Status_PUBLIC},
		},
		{
			name: "parts of a descriptor not yet public dropped with it",
			state: makeState(true,
				linked(&scpb.Schema{SchemaID: testTableID},
					scpb.Status_PUBLIC, scpb.Status_DESCRIPTOR_ADDED),
				linked(&scpb.Namespace{DatabaseID: 100, DescriptorID: testTableID, Name: "sc"},
					scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(&scpb.SchemaParent{SchemaID: testTableID, ParentDatabaseID: 100},
					scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{scpb.Status_ABSENT, scpb.Status_ABSENT, scpb.Status_ABSENT},
		},
		{
			name: "recreated index dropped when its source is kept",
			state: makeState(true,
				linked(secondary(2, 0), scpb.Status_ABSENT, scpb.Status_PUBLIC),
				linked(secondary(8, 2), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(indexName(8), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{scpb.Status_PUBLIC, scpb.Status_ABSENT, scpb.Status_ABSENT},
		},
		{
			name: "parts of a dropped index dropped with it, not constraints validated on it",
			state: makeState(true,
				linked(primary(4), scpb.Status_PUBLIC, scpb.Status_MERGE_ONLY),
				linked(&scpb.IndexData{TableID: testTableID, IndexID: 4},
					scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(indexName(4), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
				linked(temp(5), scpb.Status_TRANSIENT_ABSENT, scpb.Status_WRITE_ONLY),
				linked(&scpb.IndexData{TableID: testTableID, IndexID: 5},
					scpb.Status_TRANSIENT_ABSENT, scpb.Status_PUBLIC),
				linked(&scpb.ColumnNotNull{TableID: testTableID, ColumnID: 3, IndexIDForValidation: 4},
					scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{
				scpb.Status_ABSENT, scpb.Status_ABSENT, scpb.Status_ABSENT,
				scpb.Status_ABSENT, scpb.Status_ABSENT, scpb.Status_PUBLIC,
			},
		},
		{
			name: "recreated index kept when its source is dropped",
			state: makeState(true,
				linked(secondary(2, 0), scpb.Status_ABSENT, scpb.Status_VALIDATED),
				linked(secondary(8, 2), scpb.Status_PUBLIC, scpb.Status_PUBLIC),
			),
			expectedTargets: []scpb.Status{scpb.Status_ABSENT, scpb.Status_PUBLIC},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			settle(tc.state)
			require.Equal(t, tc.expectedTargets, targetsOf(tc.state))
		})
	}

	t.Run("settled state cannot be rolled back", func(t *testing.T) {
		ds := makeState(true,
			linked(primary(4), scpb.Status_PUBLIC, scpb.Status_VALIDATED),
			linked(primary(6), scpb.Status_ABSENT, scpb.Status_PUBLIC),
		)
		settle(ds)
		require.False(t, ds.Revertible)
		require.True(t, ds.InRollback)
		settled := targetsOf(ds)
		cs := scpb.CurrentState{
			TargetState: scpb.TargetState{Targets: ds.Targets},
			Current:     ds.CurrentStatuses,
			InRollback:  ds.InRollback,
		}
		cs.Rollback()
		require.Equal(t, settled, targetsOf(ds))
	})
}
