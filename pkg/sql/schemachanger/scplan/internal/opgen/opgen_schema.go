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
	opRegistry.register((*scpb.Schema)(nil),
		toPublic(
			scpb.Status_ABSENT,
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_DESCRIPTOR_ADDED,
				emit(func(this *scpb.Schema) *scop.CreateSchemaDescriptor {
					if this.IsTemporary {
						return nil
					}
					return &scop.CreateSchemaDescriptor{
						SchemaID: this.SchemaID,
					}
				}),
				emit(func(this *scpb.Schema) *scop.InsertTemporarySchema {
					if !this.IsTemporary {
						return nil
					}
					return &scop.InsertTemporarySchema{
						DescriptorID: this.SchemaID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				revertible(false),
				emit(func(this *scpb.Schema) *scop.MarkDescriptorAsPublic {
					if this.IsTemporary {
						return nil
					}
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.SchemaID,
					}
				}),
			),
		),
		toAbsent(scpb.Status_PUBLIC,
			equiv(scpb.Status_DESCRIPTOR_ADDED),
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Schema) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.SchemaID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Schema) *scop.DeleteDescriptor {
					return &scop.DeleteDescriptor{
						DescriptorID: this.SchemaID,
					}
				}),
			),
		),
	)
}
