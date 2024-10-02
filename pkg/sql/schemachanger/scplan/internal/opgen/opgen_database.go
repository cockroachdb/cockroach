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
	opRegistry.register((*scpb.Database)(nil),
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_DESCRIPTOR_ADDED,
				emit(func(this *scpb.Database) *scop.CreateDatabaseDescriptor {
					return &scop.CreateDatabaseDescriptor{
						DatabaseID: this.DatabaseID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				revertible(false),
				emit(func(this *scpb.Database) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.DatabaseID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			equiv(scpb.Status_DESCRIPTOR_ADDED),
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Database) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.DatabaseID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Database, md *opGenContext) *scop.CreateGCJobForDatabase {
					return nil
				}),
				emit(func(this *scpb.Database) *scop.DeleteDescriptor {
					return &scop.DeleteDescriptor{
						DescriptorID: this.DatabaseID,
					}
				}),
			),
		),
	)
}
