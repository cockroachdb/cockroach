// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

func init() {
	opRegistry.register((*scpb.NamedRangeZoneConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.NamedRangeZoneConfig) *scop.AddNamedRangeZoneConfig {
					name, ok := zonepb.NamedZonesByID[uint32(this.RangeID)]
					if !ok {
						panic(errors.AssertionFailedf("unknown named zone with id: %d", this.RangeID))
					}
					return &scop.AddNamedRangeZoneConfig{
						RangeName:  name,
						ZoneConfig: *this.ZoneConfig,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.NamedRangeZoneConfig) *scop.DiscardNamedRangeZoneConfig {
					name, ok := zonepb.NamedZonesByID[uint32(this.RangeID)]
					if !ok {
						panic(errors.AssertionFailedf("unknown named zone with id: %d", this.RangeID))
					}
					return &scop.DiscardNamedRangeZoneConfig{
						RangeName: name,
					}
				}),
			),
		),
	)
}
