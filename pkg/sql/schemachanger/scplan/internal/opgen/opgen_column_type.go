// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.ColumnType)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnType) *scop.UpsertColumnType {
					return &scop.UpsertColumnType{
						ColumnType: *protoutil.Clone(this).(*scpb.ColumnType),
					}
				}),
				emit(func(this *scpb.ColumnType) *scop.UpdateTableBackReferencesInTypes {
					if ids := referencedTypeIDs(this); len(ids) > 0 {
						return &scop.UpdateTableBackReferencesInTypes{
							TypeIDs:               ids,
							BackReferencedTableID: this.TableID,
						}
					}
					return nil
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.ColumnType, md *opGenContext) *scop.RemoveDroppedColumnType {
					if ids := referencedTypeIDs(this); len(ids) > 0 {
						// Make this a no-op if the column type already exists. The
						// RemoveDroppedColumnType op is meant to be emitted only for columns
						// that were dropped.
						if checkIfColumnTypeExists(this.TableID, this.ColumnID, md) {
							return nil
						}
						return &scop.RemoveDroppedColumnType{
							TableID:  this.TableID,
							ColumnID: this.ColumnID,
						}
					}
					return nil
				}),
				emit(func(this *scpb.ColumnType) *scop.UpdateTableBackReferencesInTypes {
					if ids := referencedTypeIDs(this); len(ids) > 0 {
						return &scop.UpdateTableBackReferencesInTypes{
							TypeIDs:               ids,
							BackReferencedTableID: this.TableID,
						}
					}
					return nil
				}),
			),
		),
	)
}

func referencedTypeIDs(this *scpb.ColumnType) []catid.DescID {
	var ids catalog.DescriptorIDSet
	if this.ComputeExpr != nil {
		for _, id := range this.ComputeExpr.UsesTypeIDs {
			ids.Add(id)
		}
	}
	for _, id := range this.ClosedTypeIDs {
		ids.Add(id)
	}
	return ids.Ordered()
}

// checkIfColumnTypeExists will determine if we are changing the given columns
// type. It does this by looking at all the targets and checking if there are
// two ColumnType's.
func checkIfColumnTypeExists(tableID descpb.ID, columnID descpb.ColumnID, md *opGenContext) bool {
	foundAbsentToPublicTransition := false
	foundPublicToAbsentTransition := false
	for idx, t := range md.Targets {
		if screl.GetDescID(t.Element()) != tableID {
			continue
		}
		switch t.Element().(type) {
		case *scpb.ColumnType:
			if t.Element().(*scpb.ColumnType).ColumnID != columnID {
				continue
			}
			if md.Initial[idx] == scpb.Status_ABSENT &&
				md.TargetState.Targets[idx].TargetStatus == scpb.Status_PUBLIC {
				foundAbsentToPublicTransition = true
			}
			if md.Initial[idx] == scpb.Status_PUBLIC &&
				md.TargetState.Targets[idx].TargetStatus == scpb.Status_ABSENT {
				foundPublicToAbsentTransition = true
			}
		}
	}
	return foundPublicToAbsentTransition && foundAbsentToPublicTransition
}
