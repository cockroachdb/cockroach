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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.Type)(nil),
		add(
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Type) scop.Op {
					return notImplemented(this)
				}),
			),
			equiv(scpb.Status_TXN_DROPPED, scpb.Status_ABSENT),
			equiv(scpb.Status_DROPPED, scpb.Status_ABSENT),
		),
		drop(
			to(scpb.Status_TXN_DROPPED,
				emit(func(this *scpb.Type) scop.Op {
					return &scop.MarkDescriptorAsDroppedSynthetically{
						DescID: this.TypeID,
					}
				}),
			),
			to(scpb.Status_DROPPED,
				// TODO(ajwerner): The move to DELETE_ONLY should be marked
				// non-revertible.
				minPhase(scop.PreCommitPhase),
				revertible(false),
				emit(func(this *scpb.Type) scop.Op {
					return &scop.MarkDescriptorAsDropped{
						DescID: this.TypeID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				minPhase(scop.PostCommitPhase),
				revertible(false),
				emit(func(this *scpb.Type, md *scpb.ElementMetadata) scop.Op {
					return &scop.LogEvent{Metadata: *md,
						DescID:    this.TypeID,
						Element:   &scpb.ElementProto{Type: this},
						Direction: scpb.Target_DROP,
					}
				}),
				emit(func(this *scpb.Type) scop.Op {
					return &scop.DeleteDescriptor{
						DescriptorID: this.TypeID,
					}
				}),
			),
		),
	)
}
