// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	// IndexPartitionEntry represents individual partitions that can be added
	// or dropped independently. Each partition in the hierarchy is represented
	// by its own element.
	opRegistry.register((*scpb.IndexPartitionEntry)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.IndexPartitionEntry) *scop.AddIndexPartitionEntry {
					return &scop.AddIndexPartitionEntry{
						PartitionEntry: *protoutil.Clone(this).(*scpb.IndexPartitionEntry),
					}
				}),
			),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.IndexPartitionEntry) *scop.RemoveIndexPartitionEntry {
					return &scop.RemoveIndexPartitionEntry{
						PartitionEntry: *protoutil.Clone(this).(*scpb.IndexPartitionEntry),
					}
				}),
			),
		),
	)
}
