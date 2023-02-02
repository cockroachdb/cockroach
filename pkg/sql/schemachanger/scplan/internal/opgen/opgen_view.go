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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.View)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DROPPED,
				emit(func(this *scpb.View) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.View) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.ViewID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.View) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.ViewID,
					}
				}),
				emit(func(this *scpb.View) *scop.RemoveBackReferenceInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferenceInTypes{
						BackReferencedDescriptorID: this.ViewID,
						TypeIDs:                    this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.View) *scop.RemoveBackReferencesInRelations {
					if len(this.UsesRelationIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferencesInRelations{
						BackReferencedID: this.ViewID,
						RelationIDs:      this.UsesRelationIDs,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.View, md *opGenContext) *scop.CreateGCJobForTable {
					if this.IsMaterialized && !md.ActiveVersion.IsActive(clusterversion.V23_1) {
						return &scop.CreateGCJobForTable{
							TableID:             this.ViewID,
							DatabaseID:          databaseIDFromDroppedNamespaceTarget(md, this.ViewID),
							StatementForDropJob: statementForDropJob(this, md),
						}
					}
					return nil
				}),
				emit(func(this *scpb.View) *scop.DeleteDescriptor {
					if this.IsMaterialized {
						return nil
					}
					return &scop.DeleteDescriptor{
						DescriptorID: this.ViewID,
					}
				}),
			),
		),
	)
}
