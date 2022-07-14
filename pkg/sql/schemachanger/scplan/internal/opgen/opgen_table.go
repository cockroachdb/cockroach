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
	opRegistry.register((*scpb.Table)(nil),
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_OFFLINE,
				emit(func(this *scpb.Table) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Table) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescID: this.TableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_OFFLINE,
				emit(func(this *scpb.Table, md *targetsWithElementMap) *scop.MarkDescriptorAsOffline {
					return &scop.MarkDescriptorAsOffline{
						DescID: this.TableID,
						Reason: statementForDropJob(this, md).Statement,
					}
				}),
			),
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Table) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescID: this.TableID,
					}
				}),
				emit(func(this *scpb.Table) *scop.RemoveAllTableComments {
					return &scop.RemoveAllTableComments{
						TableID: this.TableID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Table, md *targetsWithElementMap) *scop.LogEvent {
					return newLogEventOp(this, md)
				}),
				emit(func(this *scpb.Table, md *targetsWithElementMap) *scop.CreateGcJobForTable {
					return &scop.CreateGcJobForTable{
						TableID:             this.TableID,
						StatementForDropJob: statementForDropJob(this, md),
					}
				}),
			),
		),
	)
}
