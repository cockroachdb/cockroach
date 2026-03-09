// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.TableLocalityRegionalByRowUsingConstraint)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TableLocalityRegionalByRowUsingConstraint) *scop.SetTableRBRUsingConstraint {
					return &scop.SetTableRBRUsingConstraint{
						TableID:      this.TableID,
						ConstraintID: this.ConstraintID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableLocalityRegionalByRowUsingConstraint, md *opGenContext) *scop.SetTableRBRUsingConstraint {
					if rbrUsingConstraintAppliedLater(this, md) {
						return nil
					}
					return &scop.SetTableRBRUsingConstraint{
						TableID:      this.TableID,
						ConstraintID: 0,
					}
				}),
			),
		),
	)
}

// rbrUsingConstraintAppliedLater returns true if there is another
// TableLocalityRegionalByRowUsingConstraint element for the same table that is
// going to PUBLIC. This indicates the constraint is being replaced, so we
// should not clear it in the toAbsent transition — the toPublic op will set the
// correct value.
func rbrUsingConstraintAppliedLater(
	this *scpb.TableLocalityRegionalByRowUsingConstraint, md *opGenContext,
) bool {
	for _, t := range md.Targets {
		other, ok := t.Element().(*scpb.TableLocalityRegionalByRowUsingConstraint)
		if !ok || other == this {
			continue
		}
		if other.TableID == this.TableID && t.TargetStatus == scpb.Status_PUBLIC {
			return true
		}
	}
	return false
}
