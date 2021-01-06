package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

var rules = map[scpb.Target]targetRules{
	(*scpb.AddColumn)(nil): {
		deps: targetDepRules{
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					matcher: func(
						this *scpb.AddColumn,
						that *scpb.AddIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: scpb.State_DELETE_AND_WRITE_ONLY,
				},
				{
					matcher: func(
						this *scpb.AddColumn,
						that *scpb.AddPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID, that.StoreColumnIDs...)
					},
					s: scpb.State_DELETE_AND_WRITE_ONLY,
				},
			},
			scpb.State_PUBLIC: {
				{
					matcher: func(
						this *scpb.AddColumn,
						that *scpb.AddIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: scpb.State_PUBLIC,
				},
				{
					matcher: func(
						this *scpb.AddColumn,
						that *scpb.AddPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID, that.StoreColumnIDs...)
					},
					s: scpb.State_PUBLIC,
				},
			},
		},
		forward: targetOpRules{
			scpb.State_ABSENT: {
				{
					predicate: func(this *scpb.AddColumn, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.AddColumn) scop.Op {
						return scop.MakeAddedColumnDescriptorDeleteOnly{
							TableID: this.TableID,
							Column:  this.Column,
						}
					},
				},
			},
			scpb.State_DELETE_ONLY: {
				{
					predicate: func(this *scpb.AddColumn, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.AddColumn) scop.Op {
						return scop.MakeAddedColumnDescriptorDeleteAndWriteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					nextState: scpb.State_PUBLIC,
					op: func(this *scpb.AddColumn) scop.Op {
						return scop.MakeColumnDescriptorPublic{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
		},
	},
	(*scpb.AddCheckConstraint)(nil): {
		forward: map[scpb.State][]decOpEdge{},
	},
	(*scpb.AddPrimaryIndex)(nil): {
		deps: targetDepRules{
			scpb.State_PUBLIC: {
				{
					matcher: func(
						this *scpb.AddPrimaryIndex,
						that *scpb.DropPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							this.ReplacementFor == that.Index.ID
					},
					s: scpb.State_DELETE_AND_WRITE_ONLY,
				},
			},
		},
		forward: targetOpRules{
			scpb.State_ABSENT: {
				{
					predicate: func(this *scpb.AddPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.AddPrimaryIndex) scop.Op {
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
					predicate: func(this *scpb.AddPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.AddPrimaryIndex) scop.Op {
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
					predicate: func(this *scpb.AddPrimaryIndex, flags CompileFlags) bool {
						return this.Index.Unique
					},
					nextState: scpb.State_BACKFILLED,
					op: func(this *scpb.AddPrimaryIndex) scop.Op {
						return scop.IndexBackfill{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
				{
					nextState: scpb.State_VALIDATED,
					op: func(this *scpb.AddPrimaryIndex) scop.Op {
						return scop.IndexBackfill{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			scpb.State_BACKFILLED: {
				{
					nextState: scpb.State_VALIDATED,
					op: func(this *scpb.AddPrimaryIndex) scop.Op {
						return scop.UniqueIndexValidation{
							TableID:        this.TableID,
							PrimaryIndexID: this.PrimaryIndex,
							IndexID:        this.Index.ID,
						}
					},
				},
			},
			scpb.State_VALIDATED: {
				{
					nextState: scpb.State_PUBLIC,
					op: func(this *scpb.AddPrimaryIndex) scop.Op {
						return scop.MakeAddedPrimaryIndexPublic{
							TableID: this.TableID,
							Index:   this.Index,
						}
					},
				},
			},
		},
	},
	(*scpb.DropColumn)(nil): {
		deps: targetDepRules{
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					matcher: func(
						this *scpb.DropColumn,
						that *scpb.DropIndex,
					) bool {
						return this.TableID == that.TableID &&
							columnsContainsID(that.ColumnIDs, this.Column.ID)
					},
					s: scpb.State_DELETE_AND_WRITE_ONLY,
				},
				{
					matcher: func(
						this *scpb.DropColumn,
						that *scpb.DropPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID, that.StoreColumnIDs...)
					},
					s: scpb.State_DELETE_AND_WRITE_ONLY,
				},
			},
		},
		forward: targetOpRules{
			scpb.State_PUBLIC: {
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.DropColumn) scop.Op {
						return scop.MakeDroppedColumnDeleteAndWriteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *scpb.DropColumn, flags CompileFlags) bool {
						return !flags.CreatedDescriptorIDs.Contains(this.TableID) &&
							(flags.ExecutionPhase == PostStatementPhase ||
								flags.ExecutionPhase == PreCommitPhase)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.DropColumn) scop.Op {
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
					op: func(this *scpb.DropColumn) scop.Op {
						return scop.MakeColumnAbsent{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
		},
	},
	(*scpb.DropIndex)(nil): {
		deps: targetDepRules{
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					matcher: func(
						this *scpb.DropIndex,
						that *scpb.AddIndex) bool {
						return this.TableID == that.TableID &&
							this.ReplacedBy == that.Index.ID
					},
					s: scpb.State_PUBLIC,
				},
				{
					matcher: func(
						this *scpb.DropIndex,
						that *scpb.DropColumn,
					) bool {
						return this.TableID == that.TableID &&
							columnsContainsID(this.ColumnIDs, that.Column.ID)
					},
					s: scpb.State_DELETE_AND_WRITE_ONLY,
				},
			},
		},
		forward: map[scpb.State][]decOpEdge{
			scpb.State_PUBLIC: {
				{
					predicate: func(this *scpb.DropIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.DropIndex) scop.Op {
						return scop.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							IndexID: this.IndexID,
						}
					},
				},
			},
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *scpb.DropIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.DropIndex) scop.Op {
						return scop.MakeDroppedIndexDeleteOnly{
							TableID: this.TableID,
							IndexID: this.IndexID,
						}
					},
				},
			},
			scpb.State_DELETE_ONLY: {
				{
					nextState: scpb.State_ABSENT,
					op: func(this *scpb.DropIndex) scop.Op {
						return scop.MakeIndexAbsent{
							TableID: this.TableID,
							IndexID: this.IndexID,
						}
					},
				},
			},
		},
	},
	(*scpb.DropPrimaryIndex)(nil): {
		deps: targetDepRules{
			scpb.State_DELETE_AND_WRITE_ONLY: {
				{
					scpb.State_PUBLIC,
					func(
						this *scpb.DropPrimaryIndex,
						that *scpb.AddPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							this.ReplacedBy == that.Index.ID
					},
				},
				{
					scpb.State_DELETE_AND_WRITE_ONLY,
					func(
						this *scpb.DropPrimaryIndex,
						that *scpb.DropColumn,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&this.Index, that.Column.ID, this.StoreColumnIDs...)
					},
				},
			},
		},
		forward: targetOpRules{
			scpb.State_PUBLIC: {
				{
					predicate: func(this *scpb.DropPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.DropPrimaryIndex) scop.Op {
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
					predicate: func(this *scpb.DropPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextState: scpb.State_DELETE_ONLY,
					op: func(this *scpb.DropPrimaryIndex) scop.Op {
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
					op: func(this *scpb.DropPrimaryIndex) scop.Op {
						return scop.MakeIndexAbsent{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
		},
		backwards: nil,
	},
	(*scpb.AddColumnFamily)(nil): {
		forward: targetOpRules{
			scpb.State_ABSENT: {
				{
					predicate: func(this *scpb.AddColumnFamily, flags CompileFlags) bool {
						return !flags.CreatedDescriptorIDs.Contains(this.TableID) &&
							flags.ExecutionPhase == PostStatementPhase
					},
				},
				{
					nextState: scpb.State_PUBLIC,
					op: func(this *scpb.AddColumnFamily) scop.Op {
						return scop.AddColumnFamily{
							TableID: this.TableID,
							Family:  this.Family,
						}
					},
				},
			},
		},
	},
}
