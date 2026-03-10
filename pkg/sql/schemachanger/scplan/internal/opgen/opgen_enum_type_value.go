// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.EnumTypeValue)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.EnumTypeValue) *scop.AddEnumTypeValue {
					return &scop.AddEnumTypeValue{
						TypeID:                 this.TypeID,
						PhysicalRepresentation: this.PhysicalRepresentation,
						LogicalRepresentation:  this.LogicalRepresentation,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				revertible(false),
				emit(func(this *scpb.EnumTypeValue) *scop.PromoteEnumTypeValue {
					return &scop.PromoteEnumTypeValue{
						TypeID:                 this.TypeID,
						PhysicalRepresentation: this.PhysicalRepresentation,
						LogicalRepresentation:  this.LogicalRepresentation,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.EnumTypeValue) *scop.DemoteEnumTypeValue {
					return &scop.DemoteEnumTypeValue{
						TypeID:                 this.TypeID,
						PhysicalRepresentation: this.PhysicalRepresentation,
						LogicalRepresentation:  this.LogicalRepresentation,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.EnumTypeValue) *scop.RemoveEnumTypeValue {
					return &scop.RemoveEnumTypeValue{
						TypeID:                 this.TypeID,
						PhysicalRepresentation: this.PhysicalRepresentation,
						LogicalRepresentation:  this.LogicalRepresentation,
					}
				}),
			),
		),
	)
}
