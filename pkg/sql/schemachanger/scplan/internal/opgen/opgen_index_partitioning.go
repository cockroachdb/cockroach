// Copyright 2022 The Cockroach Authors.
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

	// TODO(ajwerner): At time of writing, we have not implemented any DDLs which
	// modify the partitioning of a table or index. The only case where
	// partitioning matters is when partitioning is applied to new indexes
	// created for the purpose of adding or dropping columns. Given that, there's
	// no need to implement rolling back.
	//
	// Note that for correctness, we'll apply the partitioning in the same stage
	// as the index entering DELETE_ONLY/BACKFILLING so that zone configurations
	// can be applied to indexes as they are being backfilled.
	opRegistry.register((*scpb.IndexPartitioning)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.IndexPartitioning) *scop.AddIndexPartitionInfo {
					return &scop.AddIndexPartitionInfo{
						Partitioning: *protoutil.Clone(this).(*scpb.IndexPartitioning),
					}
				}),
			),
		),
		toTransientAbsentLikePublic(),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT),
		),
	)
}
