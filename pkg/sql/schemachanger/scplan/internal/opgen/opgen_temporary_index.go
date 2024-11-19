// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.TemporaryIndex)(nil),
		toTransientAbsent(
			scpb.Status_ABSENT,
			to(scpb.Status_DELETE_ONLY,
				emit(func(this *scpb.TemporaryIndex) *scop.MakeAbsentTempIndexDeleteOnly {
					return &scop.MakeAbsentTempIndexDeleteOnly{
						Index:            *protoutil.Clone(&this.Index).(*scpb.Index),
						IsSecondaryIndex: this.IsUsingSecondaryEncoding,
					}
				}),
				emit(func(this *scpb.TemporaryIndex) *scop.SetAddedIndexPartialPredicate {
					if this.Expr == nil {
						return nil
					}
					return &scop.SetAddedIndexPartialPredicate{
						TableID: this.TableID,
						IndexID: this.IndexID,
						Expr:    this.Expr.Expr,
					}
				}),

				emit(func(this *scpb.TemporaryIndex, md *opGenContext) *scop.MaybeAddSplitForIndex {
					// Avoid adding splits for tables without any data (i.e. newly created ones).
					if checkIfDescriptorIsWithoutData(this.TableID, md) {
						return nil
					}
					return &scop.MaybeAddSplitForIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.TemporaryIndex) *scop.MakeDeleteOnlyIndexWriteOnly {
					return &scop.MakeDeleteOnlyIndexWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_WRITE_ONLY,
			to(scpb.Status_DELETE_ONLY,
				// DELETE_ONLY is an irretrievable information-loss state since we could
				// miss out concurrent writes, but it's not marked as a non-revertible
				// transition because we have a dep rule ("index is MERGED before its
				// temp index starts to disappear") that enforces a temporary index to
				// not transition into DELETE_ONLY until its "master" index has
				// transitioned into MERGED, a state that can receive writes.
				emit(func(this *scpb.TemporaryIndex) *scop.MakeWriteOnlyIndexDeleteOnly {
					return &scop.MakeWriteOnlyIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TemporaryIndex, md *opGenContext) *scop.CreateGCJobForIndex {
					return nil
				}),
				emit(func(this *scpb.TemporaryIndex) *scop.MakeIndexAbsent {
					return &scop.MakeIndexAbsent{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
		),
	)
}
