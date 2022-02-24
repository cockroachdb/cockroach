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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.SecondaryIndex)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DELETE_ONLY,
				minPhase(scop.PreCommitPhase),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeAddedIndexDeleteOnly{
						Index:            *protoutil.Clone(&this.Index).(*scpb.Index),
						IsSecondaryIndex: true,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				minPhase(scop.PostCommitPhase),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeAddedIndexDeleteAndWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_BACKFILLED,
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.BackfillIndex{
						TableID:       this.TableID,
						SourceIndexID: this.SourceIndexID,
						IndexID:       this.IndexID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.ValidateUniqueIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeAddedSecondaryIndexPublic{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				minPhase(scop.PreCommitPhase),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					// Most of this logic is taken from MakeMutationComplete().
					return &scop.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				minPhase(scop.PostCommitPhase),
				revertible(false),
			),
			equiv(scpb.Status_BACKFILLED),
			to(scpb.Status_DELETE_ONLY,
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeDroppedIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.CreateGcJobForIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeIndexAbsent{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
		),
	)
}
