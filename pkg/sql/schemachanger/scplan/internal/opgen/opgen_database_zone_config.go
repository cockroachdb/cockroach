// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

func init() {
	opRegistry.register((*scpb.DatabaseZoneConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DatabaseZoneConfig) *scop.AddDatabaseZoneConfig {
					return &scop.AddDatabaseZoneConfig{
						DatabaseID: this.DatabaseID,
						ZoneConfig: *this.ZoneConfig,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.DatabaseZoneConfig, md *opGenContext) *scop.DiscardZoneConfig {
					// If this belongs to a drop instead of a CONFIGURE ZONE DISCARD, let
					// the GC job take care of dropping the zone config.
					filterDatabase := func(e scpb.Element) bool {
						id, err := screl.Schema.GetAttribute(screl.ReferencedDescID, e)
						if err != nil {
							panic(errors.NewAssertionErrorWithWrappedErrf(
								err, "failed to retrieve descriptor ID for %T", e,
							))
						}
						return id == this.DatabaseID
					}
					if checkIfDescriptorHasGCDependents(filterDatabase, md) {
						return nil
					}
					return &scop.DiscardZoneConfig{
						DescID: this.DatabaseID,
					}
				}),
			),
		),
	)
}
