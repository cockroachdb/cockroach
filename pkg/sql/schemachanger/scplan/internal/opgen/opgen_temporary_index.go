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
	opRegistry.register((*scpb.TemporaryIndex)(nil),
		toTransientAbsent(
			scpb.Status_ABSENT,
			to(scpb.Status_DELETE_ONLY,
				emit(func(this *scpb.TemporaryIndex) *scop.MakeAddedTempIndexDeleteOnly {
					return &scop.MakeAddedTempIndexDeleteOnly{
						Index:            *protoutil.Clone(&this.Index).(*scpb.Index),
						IsSecondaryIndex: this.IsUsingSecondaryEncoding,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.TemporaryIndex) *scop.MakeAddedIndexDeleteAndWriteOnly {
					return &scop.MakeAddedIndexDeleteAndWriteOnly{
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
				emit(func(this *scpb.TemporaryIndex) *scop.MakeDroppedIndexDeleteOnly {
					return &scop.MakeDroppedIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TemporaryIndex) *scop.CreateGcJobForIndex {
					return &scop.CreateGcJobForIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
				emit(func(this *scpb.TemporaryIndex) *scop.MakeIndexAbsent {
					return &scop.MakeIndexAbsent{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				})),
		),
	)
}
