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
	opRegistry.register((*scpb.DomainNotNull)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.DomainNotNull) *scop.AddDomainNotNull {
					return &scop.AddDomainNotNull{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			// The constraint is added as NOT VALID, so no work is required to
			// transition to VALIDATED.
			to(scpb.Status_VALIDATED),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DomainNotNull) *scop.MakeValidatedDomainNotNullPublic {
					return &scop.MakeValidatedDomainNotNullPublic{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.DomainNotNull) *scop.MakePublicDomainNotNullValidated {
					return &scop.MakePublicDomainNotNullValidated{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.DomainNotNull) *scop.RemoveDomainNotNull {
					return &scop.RemoveDomainNotNull{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
