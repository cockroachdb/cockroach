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
	opRegistry.register((*scpb.Table)(nil),
		add(
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Table) scop.Op {
					return notImplemented(this)
				}),
			),
			equiv(scpb.Status_TXN_DROPPED, scpb.Status_ABSENT),
			equiv(scpb.Status_DROPPED, scpb.Status_ABSENT),
		),
		drop(
			to(scpb.Status_TXN_DROPPED,
				emit(func(this *scpb.Table) scop.Op {
					return &scop.MarkDescriptorAsDroppedSynthetically{
						DescID: this.TableID,
					}
				}),
			),
			to(scpb.Status_DROPPED,
				minPhase(scop.PreCommitPhase),
				revertible(false),
				emit(func(this *scpb.Table) scop.Op {
					return &scop.MarkDescriptorAsDropped{
						DescID: this.TableID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				minPhase(scop.PostCommitPhase),
				// TODO(fqazi): We need to revisit if at this phase anything is revertible.
				revertible(false),
				emit(func(this *scpb.Table, md *scpb.ElementMetadata) scop.Op {
					return &scop.LogEvent{
						Metadata:     *md,
						DescID:       this.TableID,
						Element:      &scpb.ElementProto{Table: this},
						TargetStatus: scpb.Status_ABSENT,
					}
				}),
				emit(func(this *scpb.Table) scop.Op {
					return &scop.CreateGcJobForTable{
						TableID: this.TableID,
					}
				}),
			),
		),
	)
}
