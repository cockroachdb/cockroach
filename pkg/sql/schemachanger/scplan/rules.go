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

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"

func columnInSecondaryIndex(this *scpb.Column, that *scpb.SecondaryIndex) bool {
	return this.TableID == that.TableID &&
		indexContainsColumn(&that.Index, this.Column.ID)
}

func columnInPrimaryIndex(this *scpb.Column, that *scpb.PrimaryIndex) bool {
	return this.TableID == that.TableID &&
		indexContainsColumn(&that.Index, this.Column.ID) ||
		columnsContainsID(that.Index.StoreColumnIDs, this.Column.ID)
}

func primaryIndexContainsColumn(this *scpb.PrimaryIndex, that *scpb.Column) bool {
	return columnInPrimaryIndex(that, this)
}

func primaryIndexesReferenceEachOther(this, that *scpb.PrimaryIndex) bool {
	return this.TableID == that.TableID &&
		this.OtherPrimaryIndexID == that.Index.ID
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
	},
	(*scpb.RelationDependedOnBy)(nil): {
		deps: targetDepRules{},
	},
	(*scpb.TypeReference)(nil): {
		deps: targetDepRules{},
	},
	(*scpb.DefaultExpression)(nil): {
		deps:      targetDepRules{},
		backwards: nil,
		forward:   nil,
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
	},
	(*scpb.OutboundForeignKey)(nil): {
		deps: nil,
	},
	(*scpb.InboundForeignKey)(nil): {
		deps: nil,
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
	},
	(*scpb.PrimaryIndex)(nil): {
		deps: targetDepRules{
			scpb.Status_PUBLIC: {
				{
					dirPredicate: directionsMatch(scpb.Target_ADD, scpb.Target_DROP),
					thatStatus:   scpb.Status_DELETE_AND_WRITE_ONLY,
					predicate:    primaryIndexesReferenceEachOther,
				},
			},
			scpb.Status_DELETE_AND_WRITE_ONLY: {
				{
					dirPredicate: directionsMatch(scpb.Target_DROP, scpb.Target_ADD),
					thatStatus:   scpb.Status_PUBLIC,
					predicate:    primaryIndexesReferenceEachOther,
				},
				{
					dirPredicate: bothDirectionsEqual(scpb.Target_DROP),
					thatStatus:   scpb.Status_DELETE_AND_WRITE_ONLY,
					predicate:    primaryIndexContainsColumn,
				},
			},
		},
	},
}
