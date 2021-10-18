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
	opRegistry.register(
		(*scpb.Type)(nil),
		scpb.Target_DROP,
		scpb.Status_PUBLIC,
		to(scpb.Status_DELETE_ONLY,
			minPhase(scop.PreCommitPhase),
			// TODO(ajwerner): This should be marked as non-revertible.
			emit(func(this *scpb.Type) scop.Op {
				return &scop.MarkDescriptorAsDropped{
					TableID: this.TypeID,
				}
			})),
		to(scpb.Status_ABSENT,
			// TODO(ajwerner): The move to DELETE_ONLY should be marked
			// non-revertible.
			minPhase(scop.PostCommitPhase),
			revertible(false),
			emit(func(this *scpb.Type) scop.Op {
				return &scop.DrainDescriptorName{
					TableID: this.TypeID,
				}
			})),
	)
}
