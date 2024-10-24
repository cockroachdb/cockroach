// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.NamedRangeZoneConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.NamedRangeZoneConfig) *scop.AddNamedRangeZoneConfig {
					return &scop.AddNamedRangeZoneConfig{
						RangeID:    this.RangeID,
						ZoneConfig: this.ZoneConfig,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.NamedRangeZoneConfig) *scop.NotImplementedForPublicObjects {
					return notImplementedForPublicObjects(this)
				}),
			),
		),
	)
}
