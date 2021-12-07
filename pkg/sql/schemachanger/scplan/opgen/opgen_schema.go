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
	opRegistry.register((*scpb.Schema)(nil),
		add(
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Schema) scop.Op {
					return notImplemented(this)
				}),
			),
			equiv(scpb.Status_TXN_DROPPED, scpb.Status_ABSENT),
			equiv(scpb.Status_DROPPED, scpb.Status_ABSENT),
		),
		drop(
			to(scpb.Status_TXN_DROPPED,
				minPhase(scop.StatementPhase),
				emit(func(this *scpb.Schema) scop.Op {
					return &scop.MarkDescriptorAsDroppedSynthetically{
						DescID: this.SchemaID,
					}
				})),
			to(scpb.Status_DROPPED,
				minPhase(scop.PreCommitPhase),
				revertible(false),
				emit(func(this *scpb.Schema) scop.Op {
					return &scop.MarkDescriptorAsDropped{
						DescID: this.SchemaID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				// TODO(ajwerner): The minPhase here feels like it should be PostCommit.
				// Also, this definitely is not revertible. Leaving to make this commit
				// a port.
				minPhase(scop.PreCommitPhase),
				revertible(false),
				emit(func(this *scpb.Schema) scop.Op {
					return &scop.DrainDescriptorName{
						TableID: this.SchemaID,
					}
				}),
				emit(func(this *scpb.Schema, md *scpb.ElementMetadata) scop.Op {
					return &scop.LogEvent{Metadata: *md,
						DescID:    this.SchemaID,
						Element:   &scpb.ElementProto{Schema: this},
						Direction: scpb.Target_DROP,
					}
				}),
				emit(func(this *scpb.Schema) scop.Op {
					return &scop.DeleteDescriptor{
						DescriptorID: this.SchemaID,
					}
				}),
			),
		),
	)
}
