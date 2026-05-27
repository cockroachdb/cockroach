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
	opRegistry.register((*scpb.Table)(nil),
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_DESCRIPTOR_ADDED,
				emit(func(this *scpb.Table) *scop.CreateTableDescriptor {
					return &scop.CreateTableDescriptor{
						TableID: this.TableID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Table) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.TableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			equiv(scpb.Status_DESCRIPTOR_ADDED),
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Table) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.TableID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Table, md *opGenContext) *scop.CreateGCJobForTable {
					return nil
				}),
			),
		),
	)
}
