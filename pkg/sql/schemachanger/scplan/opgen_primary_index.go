// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
)

func init() {
	opGenRegistry.Register(
		(*scpb.PrimaryIndex)(nil),
		scpb.Target_ADD,
		scpb.Status_ABSENT,
		To(scpb.Status_DELETE_ONLY,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.MakeAddedIndexDeleteOnly{
					TableID: this.TableID,
					Index:   this.Index,
				}
			})),
		To(scpb.Status_DELETE_AND_WRITE_ONLY,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.MakeAddedIndexDeleteAndWriteOnly{
					TableID: this.TableID,
					IndexID: this.Index.ID,
				}
			})),
		To(scpb.Status_BACKFILLED,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.BackfillIndex{
					TableID: this.TableID,
					IndexID: this.Index.ID,
				}
			})),
		// If this index is unique (which primary indexes should be) and
		// there's not already a covering primary index, then we'll need to
		// validate that this index indeed is unique.
		//
		// TODO(ajwerner): Rationalize this and hook up the optimization.
		To(scpb.Status_VALIDATED,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.ValidateUniqueIndex{
					TableID:        this.TableID,
					PrimaryIndexID: this.OtherPrimaryIndexID,
					IndexID:        this.Index.ID,
				}
			})),
		To(scpb.Status_PUBLIC,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.MakeAddedPrimaryIndexPublic{
					TableID: this.TableID,
					Index:   this.Index,
				}
			})),
	)

	opGenRegistry.Register(
		(*scpb.PrimaryIndex)(nil),
		scpb.Target_DROP,
		scpb.Status_PUBLIC,
		To(scpb.Status_DELETE_AND_WRITE_ONLY,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				// Most of this logic is taken from MakeMutationComplete().
				return &scop.MakeDroppedPrimaryIndexDeleteAndWriteOnly{
					TableID: this.TableID,
					Index:   this.Index,
				}
			})),
		To(scpb.Status_DELETE_ONLY,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.MakeDroppedIndexDeleteOnly{
					TableID: this.TableID,
					IndexID: this.Index.ID,
				}
			})),
		To(scpb.Status_ABSENT,
			Emit(func(this *scpb.PrimaryIndex) scop.Op {
				return &scop.MakeIndexAbsent{
					TableID: this.TableID,
					IndexID: this.Index.ID,
				}
			})),
	)
}
