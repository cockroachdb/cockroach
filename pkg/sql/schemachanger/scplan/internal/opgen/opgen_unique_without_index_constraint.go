// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.UniqueWithoutIndexConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.AddUniqueWithoutIndexConstraint {
					var partialExpr catpb.Expression
					if this.Predicate != nil {
						partialExpr = this.Predicate.Expr
					}
					return &scop.AddUniqueWithoutIndexConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
						ColumnIDs:    this.ColumnIDs,
						PartialExpr:  partialExpr,
						Validity:     descpb.ConstraintValidity_Validating,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.UniqueWithoutIndexConstraint, md *opGenContext) *scop.ValidateConstraint {
					if checkIfDescriptorIsWithoutData(this.TableID, md) {
						return nil
					}
					return &scop.ValidateConstraint{
						TableID:              this.TableID,
						ConstraintID:         this.ConstraintID,
						IndexIDForValidation: this.IndexIDForValidation,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.MakeValidatedUniqueWithoutIndexConstraintPublic {
					return &scop.MakeValidatedUniqueWithoutIndexConstraintPublic{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.MakePublicUniqueWithoutIndexConstraintValidated {
					return &scop.MakePublicUniqueWithoutIndexConstraintValidated{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.RemoveUniqueWithoutIndexConstraint {
					return &scop.RemoveUniqueWithoutIndexConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraint) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
		),
	)
}
