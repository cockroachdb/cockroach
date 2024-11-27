// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
)

func init() {
	opRegistry.register((*scpb.TableZoneConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TableZoneConfig) *scop.AddTableZoneConfig {
					return &scop.AddTableZoneConfig{
						TableID:    this.TableID,
						ZoneConfig: *this.ZoneConfig,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableZoneConfig, md *opGenContext) *scop.DiscardTableZoneConfig {
					// If this belongs to a drop instead of a CONFIGURE ZONE DISCARD, let
					// the GC job take care of dropping the zone config.
					filterTable := func(e scpb.Element) bool {
						return screl.GetDescID(e) == this.TableID
					}
					if checkIfDescriptorHasGCDependents(filterTable, md) {
						return nil
					}
					return &scop.DiscardTableZoneConfig{
						TableID:    this.TableID,
						ZoneConfig: *this.ZoneConfig,
					}
				}),
			),
		),
	)
}
