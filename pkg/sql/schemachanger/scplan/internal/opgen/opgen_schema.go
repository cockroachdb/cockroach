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
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_OFFLINE,
				emit(func(this *scpb.Schema) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Schema) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescID: this.SchemaID,
					}
				}),
			),
		),
		toAbsent(scpb.Status_PUBLIC,
			to(scpb.Status_OFFLINE,
				emit(func(this *scpb.Schema, md *targetsWithElementMap) *scop.MarkDescriptorAsOffline {
					return &scop.MarkDescriptorAsOffline{
						DescID: this.SchemaID,
						Reason: statementForDropJob(this, md).Statement,
					}
				}),
			),
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Schema) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescID: this.SchemaID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Schema, md *targetsWithElementMap) *scop.LogEvent {
					return newLogEventOp(this, md)
				}),
				emit(func(this *scpb.Schema) *scop.DeleteDescriptor {
					return &scop.DeleteDescriptor{
						DescriptorID: this.SchemaID,
					}
				}),
			),
		),
	)
}
