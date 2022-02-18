// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.Column)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DELETE_ONLY,
				minPhase(scop.PreCommitPhase),
				emit(func(this *scpb.Column) scop.Op {
					return &scop.MakeAddedColumnDeleteOnly{
						Column: *protoutil.Clone(this).(*scpb.Column),
					}
				}),
				emit(func(this *scpb.Column) scop.Op {
					if ids := referencedTypeIDs(this); !ids.Empty() {
						return &scop.UpdateBackReferencesInTypes{
							TypeIDs:              ids,
							BackReferencedDescID: this.TableID,
						}
					}
					return nil
				}),
				emit(func(this *scpb.Column, ts scpb.TargetState) scop.Op {
					return newLogEventOp(this, ts)
				}),
			),
			to(scpb.Status_DELETE_AND_WRITE_ONLY,
				minPhase(scop.PostCommitPhase),
				emit(func(this *scpb.Column) scop.Op {
					return &scop.MakeAddedColumnDeleteAndWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Column) scop.Op {
					return &scop.MakeColumnPublic{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_DELETE_AND_WRITE_ONLY,
				emit(func(this *scpb.Column) scop.Op {
					return &scop.MakeDroppedColumnDeleteAndWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.Column, ts scpb.TargetState) scop.Op {
					return newLogEventOp(this, ts)
				}),
			),
			to(scpb.Status_DELETE_ONLY,
				minPhase(scop.PostCommitPhase),
				revertible(false),
				emit(func(this *scpb.Column) scop.Op {
					return &scop.MakeDroppedColumnDeleteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Column) scop.Op {
					return &scop.MakeColumnAbsent{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.Column) scop.Op {
					if ids := referencedTypeIDs(this); !ids.Empty() {
						return &scop.UpdateBackReferencesInTypes{
							TypeIDs:              ids,
							BackReferencedDescID: this.TableID,
						}
					}
					return nil
				}),
			),
		),
	)
}

func referencedTypeIDs(this *scpb.Column) (ids catalog.DescriptorIDSet) {
	if this.ComputeExpr != nil {
		for _, id := range this.ComputeExpr.UsesTypeIDs {
			ids.Add(id)
		}
	}
	for _, id := range this.ClosedTypeIDs {
		ids.Add(id)
	}
	return ids
}
