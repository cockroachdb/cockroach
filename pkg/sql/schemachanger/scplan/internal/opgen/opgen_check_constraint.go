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
	opRegistry.register((*scpb.CheckConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.CheckConstraint) *scop.MakeAbsentCheckConstraintWriteOnly {
					return &scop.MakeAbsentCheckConstraintWriteOnly{
						TableID:               this.TableID,
						ConstraintID:          this.ConstraintID,
						ColumnIDs:             this.ColumnIDs,
						CheckExpr:             this.Expr,
						FromHashShardedColumn: this.FromHashShardedColumn,
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
				emit(func(this *scpb.CheckConstraint) *scop.UpdateBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateBackReferencesInSequences{
						SequenceIDs:           this.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.CheckConstraint) *scop.ValidateConstraint {
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
				emit(func(this *scpb.CheckConstraint) *scop.UpdateBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateBackReferencesInSequences{
						SequenceIDs:           this.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
		),
	)
}
