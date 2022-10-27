// Copyright 2022 The Cockroach Authors.
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
	opRegistry.register((*scpb.TableData)(nil),
		toPublic(scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_PUBLIC)),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.TableData, md *opGenContext) *scop.CreateGCJobForTable {
					return &scop.CreateGCJobForTable{
						TableID:             this.TableID,
						DatabaseID:          this.DatabaseID,
						StatementForDropJob: statementForDropJob(this, md),
					}
				}),
			),
			to(scpb.Status_ABSENT),
		),
	)
}
