// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.TableLocalityRegionalByRowUsingConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TableLocalityRegionalByRowUsingConstraint) *scop.SetTableRBRUsingConstraint {
					return &scop.SetTableRBRUsingConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableLocalityRegionalByRowUsingConstraint) *scop.SetTableRBRUsingConstraint {
					return &scop.SetTableRBRUsingConstraint{
						TableID:      this.TableID,
						ConstraintID: 0,
					}
				}),
			),
		),
	)
}
