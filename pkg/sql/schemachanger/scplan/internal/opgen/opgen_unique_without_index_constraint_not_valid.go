// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.UniqueWithoutIndexConstraintNotValid)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.UniqueWithoutIndexConstraintNotValid) *scop.MakeAbsentUniqueWithoutIndexConstraintNotValidPublic {
					var partialExpr catpb.Expression
					if this.Predicate != nil {
						partialExpr = this.Predicate.Expr
					}
					return &scop.MakeAbsentUniqueWithoutIndexConstraintNotValidPublic{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
						ColumnIDs:    this.ColumnIDs,
						PartialExpr:  partialExpr,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintNotValid) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintNotValid) *scop.UpdateTableBackReferencesInSequences {
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
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.UniqueWithoutIndexConstraintNotValid) *scop.RemoveUniqueWithoutIndexConstraint {
					return &scop.RemoveUniqueWithoutIndexConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintNotValid) *scop.UpdateTableBackReferencesInTypes {
					if this.Predicate == nil || this.Predicate.UsesTypeIDs == nil || len(this.Predicate.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.Predicate.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.UniqueWithoutIndexConstraintNotValid) *scop.UpdateTableBackReferencesInSequences {
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
