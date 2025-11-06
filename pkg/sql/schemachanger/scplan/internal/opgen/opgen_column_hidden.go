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
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.ColumnHidden) *scop.MakeAbsentColumnHiddenWriteOnly {
					return &scop.MakeAbsentColumnHiddenWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_PUBLIC),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.ColumnHidden) *scop.MakePublicColumnHiddenWriteOnly {
					return &scop.MakePublicColumnHiddenWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_ABSENT),
		),
	)
}
