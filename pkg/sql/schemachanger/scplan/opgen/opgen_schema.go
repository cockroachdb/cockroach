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
		(*scpb.Schema)(nil),
		scpb.Target_DROP,
		scpb.Status_PUBLIC,
		to(scpb.Status_DELETE_ONLY,
			// TODO(ajwerner): Sort out these steps. In general, DELETE_ONLY is
			// not revertible.
			minPhase(scop.PreCommitPhase),
			emit(func(this *scpb.Schema) scop.Op {
				return &scop.MarkDescriptorAsDropped{
					TableID: this.SchemaID,
				}
			})),
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
			})),
	)
}
