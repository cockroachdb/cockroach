package compiler

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
)

var rules = map[targets.Target]targetRules{
	(*targets.AddColumn)(nil): {
		deps: targetDepRules{
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					matcher: func(
						this *targets.AddColumn,
						that *targets.AddIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: targets.State_DELETE_AND_WRITE_ONLY,
				},
				{
					matcher: func(
						this *targets.AddColumn,
						that *targets.AddPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: targets.State_DELETE_AND_WRITE_ONLY,
				},
			},
			targets.State_PUBLIC: {
				{
					matcher: func(
						this *targets.AddColumn,
						that *targets.AddIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: targets.State_PUBLIC,
				},
				{
					matcher: func(
						this *targets.AddColumn,
						that *targets.AddPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: targets.State_PUBLIC,
				},
			},
		},
		forward: targetOpRules{
			targets.State_ABSENT: {
				{
					predicate: func(this *targets.AddColumn, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_ONLY,
					op: func(this *targets.AddColumn) ops.Op {
						return ops.AddColumnDescriptor{
							TableID: this.TableID,
							Column:  this.Column,
						}
					},
				},
			},
			targets.State_DELETE_ONLY: {
				{
					predicate: func(this *targets.AddColumn, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_AND_WRITE_ONLY,
					op: func(this *targets.AddColumn) ops.Op {
						return ops.ColumnDescriptorStateChange{
							TableID:   this.TableID,
							ColumnID:  this.Column.ID,
							State:     targets.State_DELETE_ONLY,
							NextState: targets.State_DELETE_AND_WRITE_ONLY,
						}
					},
				},
			},
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					nextState: targets.State_PUBLIC,
					op: func(this *targets.AddColumn) ops.Op {
						return ops.ColumnDescriptorStateChange{
							TableID:   this.TableID,
							ColumnID:  this.Column.ID,
							State:     targets.State_DELETE_AND_WRITE_ONLY,
							NextState: targets.State_PUBLIC,
						}
					},
				},
			},
		},
	},
	(*targets.AddCheckConstraint)(nil): {
		forward: map[targets.State][]decOpEdge{},
	},
	(*targets.AddPrimaryIndex)(nil): {
		deps: targetDepRules{
			targets.State_PUBLIC: {
				{
					matcher: func(
						this *targets.AddPrimaryIndex,
						that *targets.DropPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							this.ReplacementFor == that.Index.ID
					},
					s: targets.State_DELETE_AND_WRITE_ONLY,
				},
			},
		},
		forward: targetOpRules{
			targets.State_ABSENT: {
				{
					predicate: func(this *targets.AddPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_ONLY,
					op: func(this *targets.AddPrimaryIndex) ops.Op {
						return ops.MakeAddedPrimaryIndexDeleteOnly{
							TableID:          this.TableID,
							Index:            this.Index,
							StoreColumnIDs:   this.StoreColumnIDs,
							StoreColumnNames: this.StoreColumnNames,
						}
					},
				},
			},
			targets.State_DELETE_ONLY: {
				{
					predicate: func(this *targets.AddPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_AND_WRITE_ONLY,
					op: func(this *targets.AddPrimaryIndex) ops.Op {
						return ops.MakeAddedIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					// If this index is unique (which primary indexes should be) and
					// there's not already a covering primary index, then we'll need to
					// validate that this index indeed is unique.
					//
					// TODO(ajwerner): Rationalize this and hook up the optimization.
					predicate: func(this *targets.AddPrimaryIndex, flags CompileFlags) bool {
						return this.Index.Unique
					},
					nextState: targets.State_BACKFILLED,
					op: func(this *targets.AddPrimaryIndex) ops.Op {
						return ops.IndexBackfill{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
				{
					nextState: targets.State_VALIDATED,
					op: func(this *targets.AddPrimaryIndex) ops.Op {
						return ops.IndexBackfill{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			targets.State_BACKFILLED: {
				{
					nextState: targets.State_VALIDATED,
					op: func(this *targets.AddPrimaryIndex) ops.Op {
						return ops.UniqueIndexValidation{
							TableID:        this.TableID,
							PrimaryIndexID: this.PrimaryIndex,
							IndexID:        this.Index.ID,
						}
					},
				},
			},
			targets.State_VALIDATED: {
				{
					nextState: targets.State_PUBLIC,
					op: func(this *targets.AddPrimaryIndex) ops.Op {
						return ops.MakeAddedPrimaryIndexPublic{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
		},
	},
	(*targets.DropColumn)(nil): {
		deps: targetDepRules{
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					matcher: func(
						this *targets.DropColumn,
						that *targets.DropIndex,
					) bool {
						return this.TableID == that.TableID &&
							columnsContainsID(that.ColumnIDs, this.Column.ID)
					},
					s: targets.State_DELETE_AND_WRITE_ONLY,
				},
				{
					matcher: func(
						this *targets.DropColumn,
						that *targets.DropPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&that.Index, this.Column.ID)
					},
					s: targets.State_DELETE_AND_WRITE_ONLY,
				},
			},
		},
		forward: targetOpRules{
			targets.State_PUBLIC: {
				{
					nextState: targets.State_DELETE_AND_WRITE_ONLY,
					op: func(this *targets.DropColumn) ops.Op {
						return ops.ColumnDescriptorStateChange{
							TableID:   this.TableID,
							ColumnID:  this.Column.ID,
							State:     targets.State_PUBLIC,
							NextState: targets.State_DELETE_AND_WRITE_ONLY,
						}
					},
				},
			},
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *targets.DropColumn, flags CompileFlags) bool {
						return !flags.CreatedDescriptorIDs.contains(this.TableID) &&
							(flags.ExecutionPhase == PostStatementPhase ||
								flags.ExecutionPhase == PreCommitPhase)
					},
				},
				{
					nextState: targets.State_DELETE_ONLY,
					op: func(this *targets.DropColumn) ops.Op {
						return ops.ColumnDescriptorStateChange{
							TableID:   this.TableID,
							ColumnID:  this.Column.ID,
							State:     targets.State_DELETE_AND_WRITE_ONLY,
							NextState: targets.State_DELETE_ONLY,
						}
					},
				},
			},
			targets.State_DELETE_ONLY: {
				{
					nextState: targets.State_ABSENT,
					op: func(this *targets.DropColumn) ops.Op {
						return ops.ColumnDescriptorStateChange{
							TableID:   this.TableID,
							ColumnID:  this.Column.ID,
							State:     targets.State_DELETE_ONLY,
							NextState: targets.State_ABSENT,
						}
					},
				},
			},
		},
	},
	(*targets.DropIndex)(nil): {
		deps: targetDepRules{
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					matcher: func(
						this *targets.DropIndex,
						that *targets.AddIndex) bool {
						return this.TableID == that.TableID &&
							this.ReplacedBy == that.Index.ID
					},
					s: targets.State_PUBLIC,
				},
				{
					matcher: func(
						this *targets.DropIndex,
						that *targets.DropColumn,
					) bool {
						return this.TableID == that.TableID &&
							columnsContainsID(this.ColumnIDs, that.Column.ID)
					},
					s: targets.State_DELETE_AND_WRITE_ONLY,
				},
			},
		},
		forward: map[targets.State][]decOpEdge{
			targets.State_PUBLIC: {
				{
					predicate: func(this *targets.DropIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_AND_WRITE_ONLY,
					op: func(this *targets.DropIndex) ops.Op {
						return ops.IndexDescriptorStateChange{
							TableID:   this.TableID,
							IndexID:   this.IndexID,
							State:     targets.State_PUBLIC,
							NextState: targets.State_DELETE_AND_WRITE_ONLY,
						}
					},
				},
			},
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *targets.DropIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_ONLY,
					op: func(this *targets.DropIndex) ops.Op {
						return ops.IndexDescriptorStateChange{
							TableID:   this.TableID,
							IndexID:   this.IndexID,
							State:     targets.State_DELETE_AND_WRITE_ONLY,
							NextState: targets.State_DELETE_ONLY,
						}
					},
				},
			},
			targets.State_DELETE_ONLY: {
				{
					nextState: targets.State_ABSENT,
					op: func(this *targets.DropIndex) ops.Op {
						return ops.IndexDescriptorStateChange{
							TableID:   this.TableID,
							IndexID:   this.IndexID,
							State:     targets.State_DELETE_ONLY,
							NextState: targets.State_ABSENT,
						}
					},
				},
			},
		},
	},
	(*targets.DropPrimaryIndex)(nil): {
		deps: targetDepRules{
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					targets.State_PUBLIC,
					func(
						this *targets.DropPrimaryIndex,
						that *targets.AddPrimaryIndex,
					) bool {
						return this.TableID == that.TableID &&
							this.ReplacedBy == that.Index.ID
					},
				},
				{
					targets.State_DELETE_AND_WRITE_ONLY,
					func(
						this *targets.DropPrimaryIndex,
						that *targets.DropColumn,
					) bool {
						return this.TableID == that.TableID &&
							indexContainsColumn(&this.Index, that.Column.ID)
					},
				},
			},
		},
		forward: targetOpRules{
			targets.State_PUBLIC: {
				{
					predicate: func(this *targets.DropPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PostStatementPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_AND_WRITE_ONLY,
					op: func(this *targets.DropPrimaryIndex) ops.Op {
						return ops.MakeDroppedPrimaryIndexDeleteAndWriteOnly{
							TableID:          this.TableID,
							IndexID:          this.Index.ID,
							StoreColumnIDs:   this.StoreColumnIDs,
							StoreColumnNames: this.StoreColumnNames,
						}
					},
				},
			},
			targets.State_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *targets.DropPrimaryIndex, flags CompileFlags) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.contains(this.TableID)
					},
				},
				{
					nextState: targets.State_DELETE_ONLY,
					op: func(this *targets.DropPrimaryIndex) ops.Op {
						return ops.MakeDroppedIndexDeleteOnly{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
			targets.State_DELETE_ONLY: {
				{
					nextState: targets.State_ABSENT,
					op: func(this *targets.DropPrimaryIndex) ops.Op {
						return ops.MakeDroppedIndexAbsent{
							TableID: this.TableID,
							IndexID: this.Index.ID,
						}
					},
				},
			},
		},
		backwards: nil,
	},
	(*targets.AddColumnFamily)(nil): {
		forward: targetOpRules{
			targets.State_ABSENT: {
				{
					predicate: func(this *targets.AddColumnFamily, flags CompileFlags) bool {
						return !flags.CreatedDescriptorIDs.contains(this.TableID) &&
							flags.ExecutionPhase == PostStatementPhase
					},
				},
				{
					nextState: targets.State_PUBLIC,
					op: func(this *targets.AddColumnFamily) ops.Op {
						return ops.AddColumnFamily{
							TableID: this.TableID,
							Family:  this.Family,
						}
					},
				},
			},
		},
	},
}
