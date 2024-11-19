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
	opRegistry.register((*scpb.ColumnComment)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnComment) *scop.UpsertColumnComment {
					return &scop.UpsertColumnComment{
						TableID:        this.TableID,
						ColumnID:       this.ColumnID,
						Comment:        this.Comment,
						PGAttributeNum: this.PgAttributeNum,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnComment) *scop.RemoveColumnComment {
					return &scop.RemoveColumnComment{
						TableID:        this.TableID,
						ColumnID:       this.ColumnID,
						PgAttributeNum: this.PgAttributeNum,
					}
				}),
			),
		),
	)
}
