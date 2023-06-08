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
