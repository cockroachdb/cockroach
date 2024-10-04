// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
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
				emit(func(this *scpb.TemporaryIndex) *scop.MaybeAddSplitForIndex {
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
				revertible(false),
				emit(func(this *scpb.TemporaryIndex) *scop.MakeWriteOnlyIndexDeleteOnly {
					return &scop.MakeWriteOnlyIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TemporaryIndex, md *opGenContext) *scop.CreateGCJobForIndex {
					if !md.ActiveVersion.IsActive(clusterversion.V23_1) {
						return &scop.CreateGCJobForIndex{
							TableID:             this.TableID,
							IndexID:             this.IndexID,
							StatementForDropJob: statementForDropJob(this, md),
						}
					}
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
