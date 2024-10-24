// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.DatabaseZoneConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DatabaseZoneConfig) *scop.AddDatabaseZoneConfig {
					return &scop.AddDatabaseZoneConfig{
						DatabaseID: this.DatabaseID,
						ZoneConfig: this.ZoneConfig,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.DatabaseZoneConfig) *scop.DiscardZoneConfig {
					return &scop.DiscardZoneConfig{
						DescID:     this.DatabaseID,
						ZoneConfig: this.ZoneConfig,
					}
				}),
			),
		),
	)
}
