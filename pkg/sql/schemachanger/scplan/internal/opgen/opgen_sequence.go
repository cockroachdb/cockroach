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

	opRegistry.register((*scpb.Sequence)(nil),
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Sequence) scop.Op {
					return notImplemented(this)
				}),
			),
		),
		toAbsent(scpb.Status_PUBLIC,
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Sequence) scop.Op {
					return &scop.MarkDescriptorAsDropped{
						DescID: this.SequenceID,
					}
				}),
				emit(func(this *scpb.Sequence) scop.Op {
					return &scop.RemoveAllTableComments{
						TableID: this.SequenceID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Sequence, md *targetsWithElementMap) scop.Op {
					return newLogEventOp(this, md)
				}),
				emit(func(this *scpb.Sequence, md *targetsWithElementMap) scop.Op {
					return &scop.CreateGcJobForTable{
						TableID:             this.SequenceID,
						StatementForDropJob: statementForDropJob(this, md),
					}
				}),
			),
		),
	)
}
