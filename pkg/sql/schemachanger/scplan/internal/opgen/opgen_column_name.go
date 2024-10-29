// Copyright 2021 The Cockroach Authors.
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
	opRegistry.register(
		(*scpb.ColumnName)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnName) *scop.SetColumnName {
					return &scop.SetColumnName{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
						Name:     this.Name,
					}
				}),
			),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnName) *scop.SetColumnName {
					op := &scop.SetColumnName{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
						Name:     tabledesc.ColumnNamePlaceholder(this.ColumnID),
					}
					// If a name was provided for the transition to absent, override the placeholder.
					if this.AbsentName != "" {
						op.Name = this.AbsentName
					}
					return op
				}),
			),
		),
	)
}
