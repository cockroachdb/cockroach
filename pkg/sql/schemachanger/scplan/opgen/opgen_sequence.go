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

	// TODO(ajwerner): This needs more steps.
	opRegistry.register(
		(*scpb.Sequence)(nil),
		scpb.Target_DROP,
		scpb.Status_PUBLIC,
		to(scpb.Status_TXN_DROPPED,
			minPhase(scop.StatementPhase),
			emit(func(this *scpb.Sequence) scop.Op {
				return &scop.MarkDescriptorAsDroppedSynthetically{
					DescID: this.SequenceID,
				}
			})),
		to(scpb.Status_DROPPED,
			minPhase(scop.PreCommitPhase),
			revertible(false),
			emit(func(this *scpb.Sequence) scop.Op {
				return &scop.MarkDescriptorAsDropped{
					DescID: this.SequenceID,
				}
			}),
		),
		to(scpb.Status_ABSENT,
			minPhase(scop.PreCommitPhase),
			revertible(false),
			emit(func(this *scpb.Sequence) scop.Op {
				return &scop.DrainDescriptorName{
					TableID: this.SequenceID,
				}
			}),
			emit(func(this *scpb.Sequence, md *scpb.ElementMetadata) scop.Op {
				return &scop.LogEvent{Metadata: *md,
					DescID:    this.SequenceID,
					Element:   &scpb.ElementProto{Sequence: this},
					Direction: scpb.Target_DROP,
				}
			}),
			emit(func(this *scpb.Sequence) scop.Op {
				return &scop.CreateGcJobForDescriptor{
					DescID: this.SequenceID,
				}
			}),
		),
	)
}
