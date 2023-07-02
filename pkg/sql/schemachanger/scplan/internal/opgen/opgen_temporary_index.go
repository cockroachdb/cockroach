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
