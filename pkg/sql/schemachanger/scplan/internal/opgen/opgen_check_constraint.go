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
	opRegistry.register((*scpb.CheckConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.CheckConstraint) scop.Op {
					return notImplemented(this)
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				minPhase(scop.PreCommitPhase),
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.CheckConstraint) scop.Op {
					return &scop.RemoveCheckConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) scop.Op {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.CheckConstraint) scop.Op {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateBackReferencesInSequences{
						SequenceIDs:           this.UsesSequenceIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
			),
		),
	)
}
