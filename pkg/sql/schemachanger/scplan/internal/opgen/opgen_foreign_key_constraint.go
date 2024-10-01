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
	opRegistry.register((*scpb.ForeignKeyConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.ForeignKeyConstraint) *scop.AddForeignKeyConstraint {
					return &scop.AddForeignKeyConstraint{
						TableID:                 this.TableID,
						ConstraintID:            this.ConstraintID,
						ColumnIDs:               this.ColumnIDs,
						ReferencedTableID:       this.ReferencedTableID,
						ReferencedColumnIDs:     this.ReferencedColumnIDs,
						OnUpdateAction:          this.OnUpdateAction,
						OnDeleteAction:          this.OnDeleteAction,
						CompositeKeyMatchMethod: this.CompositeKeyMatchMethod,
						Validity:                descpb.ConstraintValidity_Validating,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.ForeignKeyConstraint, md *opGenContext) *scop.ValidateConstraint {
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
				emit(func(this *scpb.ForeignKeyConstraint) *scop.MakeValidatedForeignKeyConstraintPublic {
					return &scop.MakeValidatedForeignKeyConstraintPublic{
						TableID:           this.TableID,
						ConstraintID:      this.ConstraintID,
						ReferencedTableID: this.ReferencedTableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.ForeignKeyConstraint) *scop.MakePublicForeignKeyConstraintValidated {
					return &scop.MakePublicForeignKeyConstraintValidated{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				// Back-reference removal must precede forward-reference removal,
				// because the constraint ID in the origin table and the reference
				// table do not match. We therefore have to identify the constraint
				// by name in the origin table descriptor to be able to remove the
				// back-reference.
				emit(func(this *scpb.ForeignKeyConstraint) *scop.RemoveForeignKeyBackReference {
					return &scop.RemoveForeignKeyBackReference{
						ReferencedTableID:  this.ReferencedTableID,
						OriginTableID:      this.TableID,
						OriginConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.ForeignKeyConstraint) *scop.RemoveForeignKeyConstraint {
					return &scop.RemoveForeignKeyConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
