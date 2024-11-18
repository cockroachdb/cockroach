// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {

	opRegistry.register((*scpb.Sequence)(nil),
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_DESCRIPTOR_ADDED,
				emit(func(this *scpb.Sequence) *scop.CreateSequenceDescriptor {
					return &scop.CreateSequenceDescriptor{
						SequenceID: this.SequenceID,
						Temporary:  this.IsTemporary,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Sequence) *scop.InitSequence {
					return &scop.InitSequence{
						SequenceID:     this.SequenceID,
						RestartWith:    this.RestartWith,
						UseRestartWith: this.UseRestartWith,
					}
				}),
				emit(func(this *scpb.Sequence) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.SequenceID,
					}
				}),
			),
		),
		toAbsent(scpb.Status_PUBLIC,
			equiv(scpb.Status_DESCRIPTOR_ADDED),
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
					return nil
				}),
			),
		),
	)
}
