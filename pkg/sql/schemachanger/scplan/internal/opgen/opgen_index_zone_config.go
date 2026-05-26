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
	opRegistry.register((*scpb.IndexZoneConfig)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.IndexZoneConfig) *scop.AddIndexZoneConfig {
					return &scop.AddIndexZoneConfig{
						TableID:              this.TableID,
						Subzone:              this.Subzone,
						SubzoneSpans:         this.SubzoneSpans,
						SubzoneIndexToDelete: this.OldIdxRef,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.IndexZoneConfig, md *opGenContext) *scop.DiscardSubzoneConfig {
					// If this belongs to a drop instead of a CONFIGURE ZONE DISCARD, let
					// the GC job take care of dropping the zone config.
					if checkIfIndexHasGCDependents(this.TableID, md) {
						return nil
					}
					return &scop.DiscardSubzoneConfig{
						TableID:      this.TableID,
						Subzone:      this.Subzone,
						SubzoneSpans: this.SubzoneSpans,
					}
				}),
			),
		),
	)
}
