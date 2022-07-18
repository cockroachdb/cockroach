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
	opRegistry.register((*scpb.PrimaryIndex)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_BACKFILL_ONLY,
				emit(func(this *scpb.PrimaryIndex) *scop.MakeAddedIndexBackfilling {
					return &scop.MakeAddedIndexBackfilling{
						Index: *protoutil.Clone(&this.Index).(*scpb.Index),
					}
				}),
			),
			to(scpb.Status_BACKFILLED,
				emit(func(this *scpb.PrimaryIndex) *scop.BackfillIndex {
					return &scop.BackfillIndex{
						TableID:       this.TableID,
						SourceIndexID: this.SourceIndexID,
						IndexID:       this.IndexID,
					}
				}),
			),
			to(scpb.Status_DELETE_ONLY,
				emit(func(this *scpb.PrimaryIndex) *scop.MakeBackfillingIndexDeleteOnly {
					return &scop.MakeBackfillingIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_MERGE_ONLY,
				emit(func(this *scpb.PrimaryIndex) *scop.MakeBackfilledIndexMerging {
					return &scop.MakeBackfilledIndexMerging{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_MERGED,
				emit(func(this *scpb.PrimaryIndex) *scop.MergeIndex {
					return &scop.MergeIndex{
						TableID:           this.TableID,
						TemporaryIndexID:  this.TemporaryIndexID,
						BackfilledIndexID: this.IndexID,
					}
				}),
			),
			// The transition from MERGED to WRITE_ONLY must precede index validation.
			// In MERGE_ONLY and MERGED, the index receives writes, but writes do not
			// enforce uniqueness (they don't use CPut, see ForcePut). In WRITE_ONLY,
			// the index receives normal writes. Only once writes are enforcing the
			// uniqueness constraint for new can we validate that the index does
			// indeed represent a valid uniqueness constraint over all rows.
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.PrimaryIndex) *scop.MakeMergedIndexWriteOnly {
					return &scop.MakeMergedIndexWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.PrimaryIndex) *scop.ValidateUniqueIndex {
					return &scop.ValidateUniqueIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.PrimaryIndex, md *targetsWithElementMap) *scop.MakeAddedPrimaryIndexPublic {
					return &scop.MakeAddedPrimaryIndexPublic{
						EventBase: newLogEventBase(this, md),
						TableID:   this.TableID,
						IndexID:   this.IndexID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.PrimaryIndex) *scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly {
					// Most of this logic is taken from MakeMutationComplete().
					return &scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				revertible(false),
			),
			equiv(scpb.Status_MERGE_ONLY),
			equiv(scpb.Status_MERGED),
			to(scpb.Status_DELETE_ONLY,
				emit(func(this *scpb.PrimaryIndex) *scop.MakeDroppedIndexDeleteOnly {
					return &scop.MakeDroppedIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			equiv(scpb.Status_BACKFILLED),
			equiv(scpb.Status_BACKFILL_ONLY),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.PrimaryIndex, md *targetsWithElementMap) *scop.CreateGcJobForIndex {
					return &scop.CreateGcJobForIndex{
						TableID:             this.TableID,
						IndexID:             this.IndexID,
						StatementForDropJob: statementForDropJob(this, md),
					}
				}),
				emit(func(this *scpb.PrimaryIndex, md *targetsWithElementMap) *scop.MakeIndexAbsent {
					return &scop.MakeIndexAbsent{
						EventBase: newLogEventBase(this, md),
						TableID:   this.TableID,
						IndexID:   this.IndexID,
					}
				}),
			),
		),
	)
}
