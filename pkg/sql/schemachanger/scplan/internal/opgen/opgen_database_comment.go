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
	opRegistry.register((*scpb.DatabaseComment)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DatabaseComment) *scop.UpsertDatabaseComment {
					return &scop.UpsertDatabaseComment{
						DatabaseID: this.DatabaseID,
						Comment:    this.Comment,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.DatabaseComment) *scop.RemoveDatabaseComment {
					return &scop.RemoveDatabaseComment{
						DatabaseID: this.DatabaseID,
					}
				}),
			),
		),
	)
}
