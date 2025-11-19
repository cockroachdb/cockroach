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
	opRegistry.register((*scpb.ColumnHidden)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnHidden) *scop.MakeColumnHidden {
					return &scop.MakeColumnHidden{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			// These status are present in the Column element and need to be
			// handled for migration.
			equiv(scpb.Status_WRITE_ONLY),
			equiv(scpb.Status_DELETE_ONLY),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnHidden) *scop.MakeColumnVisible {
					return &scop.MakeColumnVisible{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			// These status are present in the Column element and need to be
			// handled for migration.
			equiv(scpb.Status_WRITE_ONLY),
			equiv(scpb.Status_DELETE_ONLY),
		),
	)
}
