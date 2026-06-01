// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.DomainCheckConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.DomainCheckConstraint) *scop.AddDomainCheckConstraint {
					return &scop.AddDomainCheckConstraint{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
						Expr:         this.Expression.Expr,
						Validity:     descpb.ConstraintValidity_Validating,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.DomainCheckConstraint) *scop.ValidateDomainConstraint {
					return &scop.ValidateDomainConstraint{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DomainCheckConstraint) *scop.MakeValidatedDomainCheckConstraintPublic {
					return &scop.MakeValidatedDomainCheckConstraintPublic{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.DomainCheckConstraint) *scop.MakePublicDomainCheckConstraintValidated {
					return &scop.MakePublicDomainCheckConstraintValidated{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.DomainCheckConstraint) *scop.RemoveDomainCheckConstraint {
					return &scop.RemoveDomainCheckConstraint{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
