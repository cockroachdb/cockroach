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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func columnInSecondaryIndex(this *scpb.Column, that *scpb.SecondaryIndex) bool {
	if this.TableID == that.TableID {
		for _, columnID := range that.KeyColumnIDs {
			if columnID == this.Column.ID {
				return true
			}
		}
		for _, columnID := range that.StoringColumnIDs {
			if columnID == this.Column.ID {
				return true
			}
		}
		for _, columnID := range that.KeySuffixColumnIDs {
			if columnID == this.Column.ID {
				return true
			}
		}
	}
	return false
}

func columnInPrimaryIndex(this *scpb.Column, that *scpb.PrimaryIndex) bool {
	if this.TableID == that.TableID {
		for _, columnID := range that.KeyColumnIDs {
			if columnID == this.Column.ID {
				return true
			}
		}
		for _, columnID := range that.StoringColumnIDs {
			if columnID == this.Column.ID {
				return true
			}
		}
		for _, columnID := range that.KeySuffixColumnIDs {
			if columnID == this.Column.ID {
				return true
			}
		}
	}
	return false
}

func primaryIndexContainsColumn(this *scpb.PrimaryIndex, that *scpb.Column) bool {
	return columnInPrimaryIndex(that, this)
}

func primaryIndexesReferenceEachOther(this, that *scpb.PrimaryIndex) bool {
	return this.TableID == that.TableID &&
		this.IndexId != that.IndexId
}

func typeReferenceIsFromThisView(this *scpb.View, that *scpb.TypeReference) bool {
	return this.TableID == that.DescID
}

func thatViewDependsOnThisView(this *scpb.View, that *scpb.View) bool {
	for _, dep := range this.DependedOnBy {
		if dep == that.TableID {
			return true
		}
	}
	return false
}

func tableReferencesView(this *scpb.Table, that *scpb.View) bool {
	for _, dep := range that.DependsOn {
		if dep == this.TableID {
			return true
		}
	}
	return false
}

func tableReferencesType(this *scpb.Table, that *scpb.TypeReference) bool {
	return this.TableID == that.DescID
}

func outFkOriginatesFromTable(this *scpb.Table, that *scpb.OutboundForeignKey) bool {
	return this.TableID == that.OriginID
}

func inFkReferencesTable(this *scpb.Table, that *scpb.OutboundForeignKey) bool {
	return this.TableID == that.ReferenceID
}

func indexReferencesTable(this *scpb.Table, that *scpb.SecondaryIndex) bool {
	return that.TableID == this.TableID
}

func seqOwnedByReferencesTable(this *scpb.Table, that *scpb.SequenceOwnedBy) bool {
	return this.TableID == that.OwnerTableID
}

func seqOwnedByReferencesSeq(this *scpb.SequenceOwnedBy, that *scpb.Sequence) bool {
	return this.SequenceID == that.SequenceID
}

func tableReferencesDefaultExpression(this *scpb.Table, that *scpb.DefaultExpression) bool {
	return this.TableID == that.TableID
}

func tableReferencedByDependedOnBy(this *scpb.Table, that *scpb.RelationDependedOnBy) bool {
	return this.TableID == that.DependedOnBy
}

func defaultExprReferencesColumn(this *scpb.Sequence, that *scpb.DefaultExpression) bool {
	for _, seq := range that.UsesSequenceIDs {
		if seq == this.SequenceID {
			return true
		}
	}
	return false
}

func schemaDependsOnTable(this *scpb.Schema, that *scpb.Table) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TableID {
			return true
		}
	}
	return false
}

func schemaDependsOnView(this *scpb.Schema, that *scpb.View) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TableID {
			return true
		}
	}
	return false
}

func schemaDependsOnSequence(this *scpb.Schema, that *scpb.Sequence) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.SequenceID {
			return true
		}
	}
	return false
}

func schemaDependsOnType(this *scpb.Schema, that *scpb.Type) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TypeID {
			return true
		}
	}
	return false
}

func databaseDependsOnTable(this *scpb.Database, that *scpb.Table) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TableID {
			return true
		}
	}
	return false
}

func databaseDependsOnView(this *scpb.Database, that *scpb.View) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TableID {
			return true
		}
	}
	return false
}

func databaseDependsOnSequence(this *scpb.Database, that *scpb.Sequence) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.SequenceID {
			return true
		}
	}
	return false
}

func databaseDependsOnType(this *scpb.Database, that *scpb.Type) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TypeID {
			return true
		}
	}
	return false
}

func databaseDependsOnSchema(this *scpb.Database, that *scpb.Type) bool {
	for _, descID := range this.DependentObjects {
		if descID == that.TypeID {
			return true
		}
	}
	return false
}
func typeHasReference(this *scpb.Type, that *scpb.TypeReference) bool {
	return this.TypeID == that.TypeID
}

func indexReferencesColumn(this *scpb.SecondaryIndex, that *scpb.Column) bool {
	for _, columnID := range this.KeyColumnIDs {
		if that.TableID == this.TableID &&
			that.Column.ID == columnID {
			return true
		}
	}
	return false
}

func partitioningReferencesIndex(this *scpb.Partitioning, that *scpb.SecondaryIndex) bool {
	return this.TableID == that.TableID &&
		this.IndexId == that.IndexId
}

func indexReferencesPartitioning(this *scpb.SecondaryIndex, that *scpb.Partitioning) bool {
	return this.TableID == that.TableID &&
		this.IndexId == that.IndexId
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
	(*scpb.Partitioning)(nil): {
		deps: targetDepRules{
			scpb.Status_PUBLIC: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_DELETE_ONLY,
					predicate:    partitioningReferencesIndex,
				},
			},
		},
		backwards: nil,
		forward: targetOpRules{
			scpb.Status_ABSENT: {
				{
					predicate: func(this *scpb.Partitioning, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_PUBLIC,
					op: func(this *scpb.Partitioning) scop.Op {
						return &scop.AddIndexPartitionInfo{
							TableID:         this.TableID,
							IndexID:         this.IndexId,
							PartitionFields: this.Fields,
							ListPartitions:  this.ListPartitions,
							RangePartitions: this.RangePartitions,
						}
					},
				},
			},
		},
	},
	(*scpb.SecondaryIndex)(nil): {
		deps: targetDepRules{
			scpb.Status_DELETE_ONLY: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_PUBLIC,
					predicate:    indexReferencesColumn,
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_PUBLIC,
					predicate:    indexReferencesPartitioning,
				},
			},
		},
		backwards: nil,
		forward: targetOpRules{
			scpb.Status_ABSENT: {
				{
					predicate: func(this *scpb.SecondaryIndex, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.SecondaryIndex) scop.Op {
						return &scop.MakeAddedIndexDeleteOnly{
							TableID:             this.TableID,
							IndexID:             this.IndexId,
							IndexName:           this.IndexName,
							Unique:              this.Unique,
							KeyColumnIDs:        this.KeyColumnIDs,
							KeyColumnDirections: convertSecondaryIndexColumnDir(this),
							KeySuffixColumnIDs:  this.KeySuffixColumnIDs,
							StoreColumnIDs:      this.StoringColumnIDs,
							CompositeColumnIDs:  this.CompositeColumnIDs,
							ShardedDescriptor:   this.ShardedDescriptor,
							Inverted:            this.Inverted,
							Concurrently:        this.Concurrently,
							SecondaryIndex:      true,
						}
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					predicate: func(this *scpb.SecondaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.SecondaryIndex) scop.Op {
						return &scop.MakeAddedIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					// If this index is unique (which primary indexes should be) and
					// there's not already a covering primary index, then we'll need to
					// validate that this index indeed is unique.
					predicate: func(this *scpb.SecondaryIndex, flags Params) bool {
						return this.Unique
					},
					nextStatus: scpb.Status_BACKFILLED,
					op: func(this *scpb.SecondaryIndex) scop.Op {
						return scop.BackfillIndex{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
				{
					nextStatus: scpb.Status_VALIDATED,
					op: func(this *scpb.SecondaryIndex) scop.Op {
						return scop.BackfillIndex{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_BACKFILLED: {
				{
					nextStatus: scpb.Status_VALIDATED,
					op: func(this *scpb.SecondaryIndex) scop.Op {
						return scop.ValidateUniqueIndex{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_VALIDATED: {
				{
					nextStatus: scpb.Status_PUBLIC,
					op: func(this *scpb.SecondaryIndex) scop.Op {
						return &scop.MakeAddedSecondaryIndexPublic{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
		},
	},
	(*scpb.Database)(nil): {
		deps: targetDepRules{
			scpb.Status_DELETE_ONLY: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    databaseDependsOnTable,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    databaseDependsOnView,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    databaseDependsOnSequence,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    databaseDependsOnType,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    databaseDependsOnSchema,
				},
			},
		},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.Database, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.DatabaseID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.Database) []scop.Op {
						ops := []scop.Op{
							&scop.MarkDescriptorAsDropped{
								TableID: this.DatabaseID,
							},
						}
						return ops
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					predicate: func(this *scpb.Database, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.DatabaseID)
					},
				},
				{
					nextStatus:    scpb.Status_ABSENT,
					nonRevertible: true,
					op: func(this *scpb.Database) []scop.Op {
						ops := []scop.Op{
							&scop.DrainDescriptorName{TableID: this.DatabaseID},
						}
						return ops
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.Schema)(nil): {
		deps: targetDepRules{
			scpb.Status_DELETE_ONLY: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    schemaDependsOnTable,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    schemaDependsOnView,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    schemaDependsOnSequence,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    schemaDependsOnType,
				},
			},
		},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.Schema, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.SchemaID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.Schema) []scop.Op {
						ops := []scop.Op{
							&scop.MarkDescriptorAsDropped{
								TableID: this.SchemaID,
							},
						}
						return ops
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					predicate: func(this *scpb.Schema, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.SchemaID)
					},
				},
				{
					nextStatus:    scpb.Status_ABSENT,
					nonRevertible: true,
					op: func(this *scpb.Schema) []scop.Op {
						ops := []scop.Op{
							&scop.DrainDescriptorName{TableID: this.SchemaID},
						}
						return ops
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.SequenceOwnedBy)(nil): {
		deps: targetDepRules{
			scpb.Status_ABSENT: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_DELETE_ONLY,
					predicate:    seqOwnedByReferencesSeq,
				},
			},
		},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.SequenceOwnedBy, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.SequenceID)
					},
				},
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.SequenceOwnedBy) scop.Op {
						return &scop.RemoveSequenceOwnedBy{
							TableID: this.SequenceID,
						}
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.RelationDependedOnBy)(nil): {
		deps: targetDepRules{},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.RelationDependedOnBy, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.RelationDependedOnBy) scop.Op {
						return &scop.RemoveRelationDependedOnBy{
							TableID:      this.TableID,
							DependedOnBy: this.DependedOnBy,
						}
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.TypeReference)(nil): {
		deps: targetDepRules{},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.TypeReference, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TypeID) &&
							!flags.CreatedDescriptorIDs.Contains(this.DescID)
					},
				},
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.TypeReference) scop.Op {
						return &scop.RemoveTypeBackRef{
							TypeID: this.TypeID,
							DescID: this.DescID,
						}
					},
				},
			},
		},
		forward: targetOpRules{
			scpb.Status_ABSENT: {
				{
					predicate: func(this *scpb.TypeReference, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TypeID) &&
							!flags.CreatedDescriptorIDs.Contains(this.DescID)
					},
				},
				{
					nextStatus: scpb.Status_PUBLIC,
					op: func(this *scpb.TypeReference) scop.Op {
						return &scop.AddTypeBackRef{
							TypeID: this.TypeID,
							DescID: this.DescID,
						}
					},
				},
			},
		},
	},
	(*scpb.DefaultExpression)(nil): {
		deps: targetDepRules{},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.DefaultExpression, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.DefaultExpression) []scop.Op {
						return []scop.Op{
							&scop.RemoveColumnDefaultExpression{
								TableID:  this.TableID,
								ColumnID: this.ColumnID,
							},
							&scop.UpdateRelationDeps{
								TableID: this.TableID,
							},
						}
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.Type)(nil): {
		deps: targetDepRules{
			scpb.Status_PUBLIC: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_DELETE_ONLY,
					predicate:    typeHasReference,
				},
			},
		},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.Type, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TypeID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.Type) []scop.Op {
						ops := []scop.Op{
							&scop.MarkDescriptorAsDropped{
								TableID: this.TypeID,
							},
						}
						return ops
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					predicate: func(this *scpb.Type, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TypeID)
					},
				},
				{
					nextStatus:    scpb.Status_ABSENT,
					nonRevertible: true,
					op: func(this *scpb.Type) []scop.Op {
						ops := []scop.Op{
							&scop.DrainDescriptorName{TableID: this.TypeID},
						}
						return ops
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.Sequence)(nil): {
		deps: targetDepRules{
			scpb.Status_PUBLIC: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    defaultExprReferencesColumn,
				},
			},
		},

		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.Sequence, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.SequenceID)
					},
				},
				{
					nextStatus:    scpb.Status_ABSENT,
					nonRevertible: true,
					op: func(this *scpb.Sequence) []scop.Op {
						ops := []scop.Op{
							&scop.MarkDescriptorAsDropped{
								TableID: this.SequenceID,
							},
							&scop.DrainDescriptorName{
								TableID: this.SequenceID,
							},
							&scop.CreateGcJobForDescriptor{
								DescID: this.SequenceID,
							},
						}
						return ops
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.View)(nil): {
		deps: targetDepRules{
			scpb.Status_ABSENT: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    thatViewDependsOnThisView,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    typeReferenceIsFromThisView,
				},
			},
		},

		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.View, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus:    scpb.Status_ABSENT,
					nonRevertible: true,
					op: func(this *scpb.View) []scop.Op {
						ops := []scop.Op{
							&scop.MarkDescriptorAsDropped{
								TableID: this.TableID,
							},
							&scop.DrainDescriptorName{TableID: this.TableID},
							&scop.CreateGcJobForDescriptor{
								DescID: this.TableID,
							},
						}
						return ops
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.OutboundForeignKey)(nil): {
		deps: nil,
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.OutboundForeignKey, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.OriginID)
					},
				},
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.OutboundForeignKey) scop.Op {
						return &scop.DropForeignKeyRef{
							TableID:  this.OriginID,
							Name:     this.Name,
							Outbound: true,
						}
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.InboundForeignKey)(nil): {
		deps: nil,
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.InboundForeignKey, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.OriginID)
					},
				},
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.InboundForeignKey) scop.Op {
						return &scop.DropForeignKeyRef{
							TableID:  this.OriginID,
							Name:     this.Name,
							Outbound: false,
						}
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.Table)(nil): {
		deps: targetDepRules{
			scpb.Status_PUBLIC: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    indexReferencesTable,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    outFkOriginatesFromTable,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    inFkReferencesTable,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    tableReferencesView,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    tableReferencesType,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    seqOwnedByReferencesTable,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    tableReferencesDefaultExpression,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_ABSENT,
					predicate:    tableReferencedByDependedOnBy,
				},
			},
		},

		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.Table, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus:    scpb.Status_ABSENT,
					nonRevertible: true,
					op: func(this *scpb.Table) []scop.Op {
						ops := []scop.Op{
							&scop.MarkDescriptorAsDropped{
								TableID: this.TableID,
							},
							&scop.DrainDescriptorName{TableID: this.TableID},
							&scop.CreateGcJobForDescriptor{
								DescID: this.TableID,
							},
						}
						return ops
					},
				},
			},
		},
		forward: nil,
	},
	(*scpb.Column)(nil): {
		deps: targetDepRules{
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_DELETE_AND_WRITE_ONLY,
					predicate:    columnInSecondaryIndex,
				},
				{
					dirPredicate: sameDirection,
					thatStatus:   scpb.Status_DELETE_AND_WRITE_ONLY,
					predicate:    columnInPrimaryIndex,
				},
			},
			scpb.Status_PUBLIC: {
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_ADD),
					thatStatus:   scpb.Status_PUBLIC,
					predicate:    columnInSecondaryIndex,
				},
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_ADD),
					thatStatus:   scpb.Status_PUBLIC,
					predicate:    columnInPrimaryIndex,
				},
			},
		},
		forward: targetOpRules{
			scpb.Status_ABSENT: {
				{
					predicate: func(this *scpb.Column, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return &scop.MakeAddedColumnDeleteOnly{
							TableID:    this.TableID,
							FamilyID:   this.FamilyID,
							FamilyName: this.FamilyName,
							Column:     this.Column,
						}
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					predicate: func(this *scpb.Column, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus:    scpb.Status_DELETE_AND_WRITE_ONLY,
					nonRevertible: true,
					op: func(this *scpb.Column) scop.Op {
						return &scop.MakeAddedColumnDeleteAndWriteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					nextStatus: scpb.Status_PUBLIC,
					op: func(this *scpb.Column) scop.Op {
						return &scop.MakeColumnPublic{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
		},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					nextStatus: scpb.Status_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return &scop.MakeDroppedColumnDeleteAndWriteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *scpb.Column, flags Params) bool {
						return !flags.CreatedDescriptorIDs.Contains(this.TableID) &&
							(flags.ExecutionPhase == StatementPhase ||
								flags.ExecutionPhase == PreCommitPhase)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.Column) scop.Op {
						return &scop.MakeDroppedColumnDeleteOnly{
							TableID:  this.TableID,
							ColumnID: this.Column.ID,
						}
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.Column) scop.Op {
						return &scop.MakeColumnAbsent{
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
			scpb.Status_DELETE_ONLY: {
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_ADD),
					thatStatus:   scpb.Status_DELETE_ONLY,
					predicate:    primaryIndexContainsColumn,
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					dirPredicate: directionsMatch(scpb.Target_DROP, scpb.Target_ADD),
					thatStatus:   scpb.Status_DELETE_ONLY,
					predicate:    primaryIndexesReferenceEachOther,
				},
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_DROP),
					thatStatus:   scpb.Status_DELETE_AND_WRITE_ONLY,
					predicate:    primaryIndexContainsColumn,
				},
			},
		},
		forward: targetOpRules{
			scpb.Status_ABSENT: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return &scop.MakeAddedIndexDeleteOnly{
							TableID:             this.TableID,
							IndexID:             this.IndexId,
							IndexName:           this.IndexName,
							Unique:              this.Unique,
							KeyColumnIDs:        this.KeyColumnIDs,
							KeyColumnDirections: convertPrimaryIndexColumnDir(this),
							KeySuffixColumnIDs:  this.KeySuffixColumnIDs,
							StoreColumnIDs:      this.StoringColumnIDs,
							CompositeColumnIDs:  this.CompositeColumnIDs,
							ShardedDescriptor:   this.ShardedDescriptor,
							Inverted:            this.Inverted,
							Concurrently:        this.Concurrently,
							SecondaryIndex:      false,
						}
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return &scop.MakeAddedIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					// If this index is unique (which primary indexes should be) and
					// there's not already a covering primary index, then we'll need to
					// validate that this index indeed is unique.
					//
					// TODO(ajwerner): Rationalize this and hook up the optimization.
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return this.Unique
					},
					nextStatus: scpb.Status_BACKFILLED,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.BackfillIndex{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
				{
					nextStatus: scpb.Status_VALIDATED,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return scop.BackfillIndex{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_BACKFILLED: {
				{
					nextStatus: scpb.Status_VALIDATED,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return &scop.ValidateUniqueIndex{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_VALIDATED: {
				{
					nextStatus: scpb.Status_PUBLIC,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return &scop.MakeAddedPrimaryIndexPublic{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
		},
		backwards: targetOpRules{
			scpb.Status_PUBLIC: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == StatementPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_AND_WRITE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						// Most of this logic is taken from MakeMutationComplete().
						return &scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					predicate: func(this *scpb.PrimaryIndex, flags Params) bool {
						return flags.ExecutionPhase == PreCommitPhase &&
							!flags.CreatedDescriptorIDs.Contains(this.TableID)
					},
				},
				{
					nextStatus: scpb.Status_DELETE_ONLY,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return &scop.MakeDroppedIndexDeleteOnly{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
			scpb.Status_DELETE_ONLY: {
				{
					nextStatus: scpb.Status_ABSENT,
					op: func(this *scpb.PrimaryIndex) scop.Op {
						return &scop.MakeIndexAbsent{
							TableID: this.TableID,
							IndexID: this.IndexId,
						}
					},
				},
			},
		},
	},
}
