// Copyright 2023 The Cockroach Authors.
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
	opRegistry.register((*scpb.UniqueWithoutIndexConstraintUnvalidated)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.UniqueWithoutIndexConstraintUnvalidated) *scop.AddUniqueWithoutIndexConstraint {
					var partialExpr catpb.Expression
					if this.Predicate != nil {
						partialExpr = this.Predicate.Expr
					}
					return &scop.AddUniqueWithoutIndexConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
						ColumnIDs:    this.ColumnIDs,
						PartialExpr:  partialExpr,
						Validity:     descpb.ConstraintValidity_Unvalidated,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintUnvalidated) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintUnvalidated) *scop.UpdateTableBackReferencesInSequences {
					if this.Predicate == nil || this.Predicate.UsesSequenceIDs == nil || len(this.Predicate.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:           this.Predicate.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.UniqueWithoutIndexConstraintUnvalidated) *scop.RemoveUniqueWithoutIndexConstraint {
					return &scop.RemoveUniqueWithoutIndexConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintUnvalidated) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintUnvalidated) *scop.UpdateTableBackReferencesInSequences {
					if this.Predicate == nil || this.Predicate.UsesSequenceIDs == nil || len(this.Predicate.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:           this.Predicate.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
		),
	)
}
