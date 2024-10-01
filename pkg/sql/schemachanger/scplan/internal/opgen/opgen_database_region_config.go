// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"

func init() {
	opRegistry.register((*scpb.DatabaseRegionConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT),
		),
	)
}
