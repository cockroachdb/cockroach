// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.CompositeType)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DROPPED,
				emit(func(this *scpb.CompositeType) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.CompositeType) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.TypeID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.CompositeType) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.TypeID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.CompositeType) *scop.DeleteDescriptor {
					return &scop.DeleteDescriptor{
						DescriptorID: this.TypeID,
					}
				}),
			),
		),
	)
}
