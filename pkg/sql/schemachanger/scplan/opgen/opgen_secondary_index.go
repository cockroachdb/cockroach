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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func convertSecondaryIndexColumnDir(
	secondaryIndex *scpb.SecondaryIndex,
) []descpb.IndexDescriptor_Direction {
	// Convert column directions
	convertedColumnDirs := make([]descpb.IndexDescriptor_Direction, 0, len(secondaryIndex.KeyColumnDirections))
	for _, columnDir := range secondaryIndex.KeyColumnDirections {
		switch columnDir {
		case scpb.SecondaryIndex_DESC:
			convertedColumnDirs = append(convertedColumnDirs, descpb.IndexDescriptor_DESC)
		case scpb.SecondaryIndex_ASC:
			convertedColumnDirs = append(convertedColumnDirs, descpb.IndexDescriptor_ASC)
		}
	}
	return convertedColumnDirs
}

func init() {
	opRegistry.register((*scpb.SecondaryIndex)(nil),
		add(
			to(scpb.Status_DELETE_ONLY,
				minPhase(scop.PreCommitPhase),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeAddedIndexDeleteOnly{
						TableID:             this.TableID,
						IndexID:             this.IndexID,
						Unique:              this.Unique,
						KeyColumnIDs:        this.KeyColumnIDs,
						KeyColumnDirections: convertSecondaryIndexColumnDir(this),
						KeySuffixColumnIDs:  this.KeySuffixColumnIDs,
						StoreColumnIDs:      this.StoringColumnIDs,
						CompositeColumnIDs:  this.CompositeColumnIDs,
						ShardedDescriptor:   this.ShardedDescriptor,
						Inverted:            this.Inverted,
						Concurrently:        this.Concurrently,
						SecondaryIndex:      true,
					}
				}),
			),
			to(scpb.Status_DELETE_AND_WRITE_ONLY,
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
			// If this index is unique (which primary indexes should be) and
			// there's not already a covering primary index, then we'll need to
			// validate that this index indeed is unique.
			//
			// TODO(ajwerner): Rationalize this and hook up the optimization.
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
		drop(
			to(scpb.Status_DELETE_AND_WRITE_ONLY,
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					// Most of this logic is taken from MakeMutationComplete().
					return &scop.MakeDroppedNonPrimaryIndexDeleteAndWriteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			to(scpb.Status_DELETE_ONLY,
				minPhase(scop.PostCommitPhase),
				revertible(false),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeDroppedIndexDeleteOnly{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
			),
			equiv(scpb.Status_VALIDATED, scpb.Status_DELETE_AND_WRITE_ONLY),
			equiv(scpb.Status_BACKFILLED, scpb.Status_DELETE_AND_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.MakeIndexAbsent{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				}),
				emit(func(this *scpb.SecondaryIndex) scop.Op {
					return &scop.CreateGcJobForIndex{
						TableID: this.TableID,
						IndexID: this.IndexID,
					}
				})),
		),
	)
}
