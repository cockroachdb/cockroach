// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.ColumnType)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				minPhase(scop.PreCommitPhase),
				emit(func(this *scpb.ColumnType) scop.Op {
					return &scop.SetAddedColumnType{
						ColumnType: *protoutil.Clone(this).(*scpb.ColumnType),
					}
				}),
				emit(func(this *scpb.ColumnType) scop.Op {
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
				minPhase(scop.PreCommitPhase),
				revertible(false),
				emit(func(this *scpb.ColumnType) scop.Op {
					return &scop.RemoveDroppedColumnType{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.ColumnType) scop.Op {
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
