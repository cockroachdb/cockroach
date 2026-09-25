// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

func init() {
	opRegistry.register((*scpb.TableLocalitySecondaryRegion)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TableLocalitySecondaryRegion) *scop.SetTableLocalitySecondaryRegion {
					return &scop.SetTableLocalitySecondaryRegion{
						TableID:          this.TableID,
						RegionEnumTypeID: this.RegionEnumTypeID,
						RegionName:       this.RegionName,
					}
				}),
				// Must add a backreference to the multi-region enum for this type of locality.
				// Table has an "implicit" dependency on the multi-region enum because it explicitly refers
				// to a region. So even though the table doesn't have a region column, its region config uses
				// a value from the multi-region enum.
				emit(func(this *scpb.TableLocalitySecondaryRegion) *scop.UpdateTableBackReferencesInTypes {
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               []catid.DescID{this.RegionEnumTypeID},
						BackReferencedTableID: this.TableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableLocalitySecondaryRegion) *scop.UnsetTableLocality {
					return &scop.UnsetTableLocality{
						TableID: this.TableID,
					}
				}),
				// Becoming REGIONAL BY ROW keeps the table dependent on the
				// multi-region enum through its new crdb_region column, whose
				// type reference installs the back-reference in an earlier
				// stage. Removing it here would undo that and leave the enum
				// droppable out from under the table. The decision is made from
				// the plan's target state rather than the descriptor, which is
				// only partially updated at this point.
				emit(func(this *scpb.TableLocalitySecondaryRegion, md *opGenContext) *scop.RemoveBackReferenceInTypes {
					if isBecomingRegionalByRow(this.TableID, md) {
						return nil
					}
					return &scop.RemoveBackReferenceInTypes{
						TypeIDs:                    []catid.DescID{this.RegionEnumTypeID},
						BackReferencedDescriptorID: this.TableID,
					}
				}),
			),
		),
	)
}
