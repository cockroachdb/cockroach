// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.SecondaryIndexPartial)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.SecondaryIndexPartial) *scop.SetAddedIndexPartialPredicate {
					return &scop.SetAddedIndexPartialPredicate{
						TableID: this.TableID,
						IndexID: this.IndexID,
						Expr:    this.Expr,
					}
				}),
				emit(func(this *scpb.SecondaryIndexPartial) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.SecondaryIndexPartial) *scop.AddTableIndexBackReferencesInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.AddTableIndexBackReferencesInFunctions{
						FunctionIDs:           this.UsesFunctionIDs,
						BackReferencedTableID: this.TableID,
						BackReferencedIndexID: this.IndexID,
					}
				}),
			),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.SecondaryIndexPartial) *scop.RemoveDroppedIndexPartialPredicate {
					return &scop.RemoveDroppedIndexPartialPredicate{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
				emit(func(this *scpb.SecondaryIndexPartial) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.SecondaryIndexPartial) *scop.RemoveTableIndexBackReferencesInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.RemoveTableIndexBackReferencesInFunctions{
						FunctionIDs:           this.UsesFunctionIDs,
						BackReferencedTableID: this.TableID,
						BackReferencedIndexID: this.IndexID,
					}
				}),
			),
		),
	)
}
