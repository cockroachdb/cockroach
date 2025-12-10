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
	opRegistry.register((*scpb.RowLevelTTL)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.RowLevelTTL, md *opGenContext) *scop.UpsertRowLevelTTL {
					return &scop.UpsertRowLevelTTL{
						TableID:     this.TableID,
						RowLevelTTL: this.RowLevelTTL,
						TTLExpr:     this.TTLExpr,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// TODO(postamar): remove revertibility constraint when possible
				revertible(false),
				emit(func(this *scpb.RowLevelTTL, md *opGenContext) *scop.DeleteSchedule {
					if ttlAppliedLater(this, md) {
						return nil
					}
					return &scop.DeleteSchedule{
						ScheduleID: this.RowLevelTTL.ScheduleID,
					}
				}),
			),
		),
	)
}

// ttlAppliedLater returns true if there is another RowLevelTTL element
// with the same ScheduleID and TableID, a higher SeqNum, and
// a target status of PUBLIC. This indicates that the TTL schedule
// will be applied later, so we should not delete the schedule yet.
func ttlAppliedLater(this *scpb.RowLevelTTL, md *opGenContext) bool {
	if this.RowLevelTTL.ScheduleID == 0 {
		return false
	}
	for _, t := range md.Targets {
		other, ok := t.Element().(*scpb.RowLevelTTL)
		if !ok || other == this {
			continue
		}
		if other.RowLevelTTL.ScheduleID == this.RowLevelTTL.ScheduleID &&
			other.TableID == this.TableID &&
			other.SeqNum > this.SeqNum &&
			t.TargetStatus == scpb.Status_PUBLIC {
			return true
		}
	}
	return false
}
