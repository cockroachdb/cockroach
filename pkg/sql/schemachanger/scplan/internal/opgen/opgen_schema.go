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
			to(scpb.Status_DESCRIPTOR_ADDED,
				emit(func(this *scpb.Schema) *scop.CreateSchemaDescriptor {
					return &scop.CreateSchemaDescriptor{
						SchemaID: this.SchemaID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				revertible(false),
				emit(func(this *scpb.Schema) *scop.MarkDescriptorAsPublic {
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
