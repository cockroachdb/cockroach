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
	opRegistry.register((*scpb.DomainCheckConstraintUnvalidated)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DomainCheckConstraintUnvalidated) *scop.AddDomainCheckConstraint {
					return &scop.AddDomainCheckConstraint{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
						Expr:         this.Expression.Expr,
						Validity:     descpb.ConstraintValidity_Unvalidated,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.DomainCheckConstraintUnvalidated) *scop.RemoveDomainCheckConstraint {
					return &scop.RemoveDomainCheckConstraint{
						TypeID:       this.TypeID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
