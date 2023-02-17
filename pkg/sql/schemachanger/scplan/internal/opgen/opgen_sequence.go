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

	opRegistry.register((*scpb.Sequence)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DROPPED,
				emit(func(this *scpb.Sequence) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Sequence) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.SequenceID,
					}
				}),
			),
		),
		toAbsent(scpb.Status_PUBLIC,
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Sequence) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.SequenceID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Sequence, md *opGenContext) *scop.CreateGCJobForTable {
					if !md.ActiveVersion.IsActive(clusterversion.V23_1) {
						return &scop.CreateGCJobForTable{
							TableID:             this.SequenceID,
							DatabaseID:          databaseIDFromDroppedNamespaceTarget(md, this.SequenceID),
							StatementForDropJob: statementForDropJob(this, md),
						}
					}
					return nil
				}),
			),
		),
	)
}
