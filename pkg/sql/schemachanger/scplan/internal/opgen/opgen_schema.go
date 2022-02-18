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
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_TXN_DROPPED),
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Schema) scop.Op {
					return notImplemented(this)
				}),
			),
		),
		toAbsent(scpb.Status_PUBLIC,
			to(scpb.Status_TXN_DROPPED,
				emit(func(this *scpb.Schema) scop.Op {
					return &scop.MarkDescriptorAsDroppedSynthetically{
						DescID: this.SchemaID,
					}
				}),
			),
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
				minPhase(scop.PostCommitPhase),
				emit(func(this *scpb.Schema, ts scpb.TargetState) scop.Op {
					return newLogEventOp(this, ts)
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
