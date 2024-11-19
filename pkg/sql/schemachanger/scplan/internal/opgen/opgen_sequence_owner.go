// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.SequenceOwner)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.SequenceOwner) *scop.AddSequenceOwner {
					return &scop.AddSequenceOwner{
						OwnedSequenceID: this.SequenceID,
						TableID:         this.TableID,
						ColumnID:        this.ColumnID,
					}
				}),
				emit(func(this *scpb.SequenceOwner) *scop.AddOwnerBackReferenceInSequence {
					return &scop.AddOwnerBackReferenceInSequence{
						SequenceID: this.SequenceID,
						TableID:    this.TableID,
						ColumnID:   this.ColumnID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.SequenceOwner) *scop.RemoveSequenceOwner {
					return &scop.RemoveSequenceOwner{
						OwnedSequenceID: this.SequenceID,
						TableID:         this.TableID,
						ColumnID:        this.ColumnID,
					}
				}),
				emit(func(this *scpb.SequenceOwner) *scop.RemoveOwnerBackReferenceInSequence {
					return &scop.RemoveOwnerBackReferenceInSequence{
						SequenceID: this.SequenceID,
					}
				}),
			),
		),
	)
}
