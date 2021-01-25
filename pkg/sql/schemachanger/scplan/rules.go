// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func columnInSecondaryIndex(this *scpb.Column, that *scpb.SecondaryIndex) bool {
	return this.TableID == that.TableID &&
		indexContainsColumn(&that.Index, this.Column.ID)
}

func columnInPrimaryIndex(this *scpb.Column, that *scpb.PrimaryIndex) bool {
	return this.TableID == that.TableID &&
		indexContainsColumn(&that.Index, this.Column.ID) ||
		columnsContainsID(that.StoreColumnIDs, this.Column.ID)
}

func primaryIndexContainsColumn(this *scpb.PrimaryIndex, that *scpb.Column) bool {
	return columnInPrimaryIndex(that, this)
}

func primaryIndexesReferenceEachOther(this, that *scpb.PrimaryIndex) bool {
	return this.TableID == that.TableID &&
		this.OtherPrimaryIndex == that.Index.ID
}

func sameDirection(a, b scpb.Target_Direction) bool {
	return a == b
}

func oppositeDirection(a, b scpb.Target_Direction) bool {
	return a != b
}

// Suppress the linter.
var _ = oppositeDirection

func bothDirectionsEqual(dir scpb.Target_Direction) func(a, b scpb.Target_Direction) bool {
	return directionsMatch(dir, dir)
}

func directionsMatch(thisDir, thatDir scpb.Target_Direction) func(a, b scpb.Target_Direction) bool {
	return func(a, b scpb.Target_Direction) bool {
		return a == thisDir && b == thatDir
	}
}

var rules = map[scpb.Element]targetRules{
	(*scpb.Column)(nil): {
		deps: targetDepRules{
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					dirPredicate: sameDirection,
					thatState:    scpb.State_DELETE_AND_WRITE_ONLY,
					predicate:    columnInSecondaryIndex,
				},
				{
					dirPredicate: sameDirection,
					thatState:    scpb.State_DELETE_AND_WRITE_ONLY,
					predicate:    columnInPrimaryIndex,
				},
			},
			scpb.State_PUBLIC: {
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_ADD),
					thatState:    scpb.State_PUBLIC,
					predicate:    columnInSecondaryIndex,
				},
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_ADD),
					thatState:    scpb.State_PUBLIC,
					predicate:    columnInPrimaryIndex,
				},
			},
		},
		forward: targetOpRules{
			scpb.State_ABSENT: {
				{
					predicate: func(this *scpb.Column, flags Params) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return scop.MakeAddedColumnDeleteOnly{
							TableID:    this.TableID,
							FamilyID:   this.FamilyID,
							FamilyName: this.FamilyName,
							Column:     this.Column,
						}
					},
				},
			},
			scpb.State_DELETE_ONLY: {
				{
					predicate: func(this *scpb.Column, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return scop.MakeAddedColumnDeleteAndWriteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					nextState: scpb.State_PUBLIC,
					op: func(this *scpb.Column) scop.Op {
						return scop.MakeColumnPublic{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
		},
		backwards: targetOpRules{
			scpb.State_PUBLIC: {
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return scop.MakeDroppedColumnDeleteAndWriteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *scpb.Column, flags Params) bool {
						return !flags.CreatedDescriptorIDs.Contains(this.TableID) &&
							(flags.ExecutionPhase == PostStatementPhase ||
								flags.ExecutionPhase == PreCommitPhase)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return scop.MakeDroppedColumnDeleteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.State_DELETE_ONLY: {
				{
					nextState: scpb.State_ABSENT,
					op: func(this *scpb.Column) scop.Op {
						return scop.MakeColumnAbsent{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
		},
	},
	(*scpb.PrimaryIndex)(nil): {
		deps: targetDepRules{
			scpb.State_PUBLIC: {
				{
					dirPredicate: directionsMatch(scpb.Target_ADD, scpb.Target_DROP),
					thatState:    scpb.State_DELETE_AND_WRITE_ONLY,
					predicate:    primaryIndexesReferenceEachOther,
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					dirPredicate: directionsMatch(scpb.Target_DROP, scpb.Target_ADD),
					thatState:    scpb.State_PUBLIC,
					predicate:    primaryIndexesReferenceEachOther,
				},
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_DROP),
					thatState:    scpb.State_DELETE_AND_WRITE_ONLY,
					predicate:    primaryIndexContainsColumn,
				},
			},
		},
		forward: targetOpRules{
			scpb.State_ABSENT: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						idx := this.Index
						idx.StoreColumnNames = this.StoreColumnNames
						idx.StoreColumnIDs = this.StoreColumnIDs
						idx.EncodingType = descpb.PrimaryIndexEncoding
						return scop.MakeAddedIndexDeleteOnly{
							TableID: this.TableID,
							Index:   idx,
						}
					},
				},
			},
			scpb.State_DELETE_ONLY: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.MakeAddedIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					// If this index is unique (which primary indexes should be) and
					// there's not already a covering primary index, then we'll need to
					// validate that this index indeed is unique.
					//
					// TODO(ajwerner): Rationalize this and hook up the optimization.
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return this.Index.Unique
					},
					nextState: scpb.State_BACKFILLED,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.BackfillIndex{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
				{
					nextState: scpb.State_VALIDATED,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.BackfillIndex{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			scpb.State_BACKFILLED: {
				{
					nextState: scpb.State_VALIDATED,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.ValidateUniqueIndex{
							TableID:        this.TableID,
							PrimaryIndexID: this.OtherPrimaryIndex,
							IndexID:        this.Index.ID,
						}
					},
				},
			},
			scpb.State_VALIDATED: {
				{
					nextState: scpb.State_PUBLIC,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.MakeAddedPrimaryIndexPublic{
							TableID: this.TableID,
							Index:   this.Index,
						}
					},
				},
			},
		},
		backwards: targetOpRules{
			scpb.State_PUBLIC: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						// Most of this logic is taken from MakeMutationComplete().
						idx := this.Index
						idx.StoreColumnIDs = this.StoreColumnIDs
						idx.StoreColumnNames = this.StoreColumnNames
						idx.EncodingType = descpb.PrimaryIndexEncoding
						return scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							Index:   idx,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.MakeDroppedIndexDeleteOnly{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			scpb.State_DELETE_ONLY: {
				{
					nextState: scpb.State_ABSENT,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.MakeIndexAbsent{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
		},
	},
}
