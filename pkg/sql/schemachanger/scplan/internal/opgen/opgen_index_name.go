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
		(*scpb.IndexName)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.IndexName) *scop.SetIndexName {
					return &scop.SetIndexName{
						TableID: this.TableID,
						IndexID: this.IndexID,
						Name:    this.Name,
					}
				}),
			),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.IndexName) *scop.SetIndexName {
					return &scop.SetIndexName{
						TableID: this.TableID,
						IndexID: this.IndexID,
						Name:    tabledesc.IndexNamePlaceholder(this.IndexID),
					}
				}),
			),
		),
	)
}
