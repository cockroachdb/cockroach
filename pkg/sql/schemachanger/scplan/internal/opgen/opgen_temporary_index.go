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
	// TODO(postamar): rework these awful state transition paths
	//   DROPPED and PUBLIC are used as placeholders here because we can't reuse
	//   DELETE_ONLY and ABSENT in the same path.
	opRegistry.register((*scpb.TemporaryIndex)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DELETE_ONLY,
				minPhase(scop.PreCommitPhase),
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.MakeAddedIndex{
						Index:              *protoutil.Clone(&this.Index).(*scpb.Index),
						IsSecondaryIndex:   this.IsUsingSecondaryEncoding,
						IsDeletePreserving: true,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				minPhase(scop.PostCommitPhase),
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.MakeAddedIndexDeleteAndWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_DROPPED,
				// TODO(postamar): this really should be DELETE_ONLY
				revertible(false),
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.MakeDroppedIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				// TODO(postamar): this really should be ABSENT or TRANSIENT.
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.CreateGcJobForIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.MakeIndexAbsent{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_DELETE_ONLY,
				revertible(false),
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.MakeDroppedIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			// TODO(postamar): remove this as soon as we're no longer using it in
			//   the ABSENT -> PUBLIC path.
			equiv(scpb.Status_DROPPED),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.CreateGcJobForIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
				emit(func(this *scpb.TemporaryIndex) scop.Op {
					return &scop.MakeIndexAbsent{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
		),
	)
}
