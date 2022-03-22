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
	opRegistry.register((*scpb.ForeignKeyConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ForeignKeyConstraint) scop.Op {
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
				// Back-reference removal must precede forward-reference removal,
				// because the constraint ID in the origin table and the reference
				// table do not match. We therefore have to identify the constraint
				// by name in the origin table descriptor to be able to remove the
				// back-reference.
				emit(func(this *scpb.ForeignKeyConstraint) scop.Op {
					return &scop.RemoveForeignKeyBackReference{
						ReferencedTableID:  this.ReferencedTableID,
						OriginTableID:      this.TableID,
						OriginConstraintID: this.ConstraintID,
					}
				}),
				emit(func(this *scpb.ForeignKeyConstraint) scop.Op {
					return &scop.RemoveForeignKeyConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
	)
}
