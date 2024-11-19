// Copyright 2021 The Cockroach Authors.
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
	opRegistry.register((*scpb.CheckConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.CheckConstraint) *scop.AddCheckConstraint {
					return &scop.AddCheckConstraint{
						TableID:               this.TableID,
						ConstraintID:          this.ConstraintID,
						ColumnIDs:             this.ColumnIDs,
						CheckExpr:             this.Expr,
						FromHashShardedColumn: this.FromHashShardedColumn,
						Validity:              descpb.ConstraintValidity_Validating,
					}
				}),
				emit(func(this *scpb.CheckConstraint) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:           this.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) *scop.AddTableConstraintBackReferencesInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.AddTableConstraintBackReferencesInFunctions{
						FunctionIDs:                this.UsesFunctionIDs,
						BackReferencedTableID:      this.TableID,
						BackReferencedConstraintID: this.ConstraintID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.CheckConstraint, md *opGenContext) *scop.ValidateConstraint {
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
				emit(func(this *scpb.CheckConstraint) *scop.MakeValidatedCheckConstraintPublic {
					return &scop.MakeValidatedCheckConstraintPublic{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.CheckConstraint) *scop.MakePublicCheckConstraintValidated {
					return &scop.MakePublicCheckConstraintValidated{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.CheckConstraint) *scop.RemoveCheckConstraint {
					return &scop.RemoveCheckConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:           this.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) *scop.RemoveTableConstraintBackReferencesFromFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.RemoveTableConstraintBackReferencesFromFunctions{
						FunctionIDs:                this.UsesFunctionIDs,
						BackReferencedTableID:      this.TableID,
						BackReferencedConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
