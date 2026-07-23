// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.ConstraintWithoutIndexName)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ConstraintWithoutIndexName) *scop.SetConstraintName {
					return &scop.SetConstraintName{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
						Name:         this.Name,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ConstraintWithoutIndexName) *scop.SetConstraintName {
					return &scop.SetConstraintName{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
						Name:         tabledesc.ConstraintNamePlaceholder(this.ConstraintID),
					}
				}),
			),
		),
	)
}
