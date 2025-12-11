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
				emit(func(this *scpb.RowLevelTTL, md *opGenContext) *scop.CreateRowLevelTTLSchedule {
					if this.RowLevelTTL.ScheduleID != 0 {
						// Schedule already exists, no need to create one.
						return nil
					}
					return &scop.CreateRowLevelTTLSchedule{
						TableID: this.TableID,
					}
				}),
				emit(func(this *scpb.RowLevelTTL, md *opGenContext) *scop.UpdateTTLScheduleCron {
					if this.RowLevelTTL.ScheduleID == 0 {
						// No existing schedule to update; a new one will be created.
						return nil
					}
					// Find the old TTL element being dropped with a different cron.
					oldCron := findPreviousTTLCron(this, md)
					if oldCron == this.RowLevelTTL.DeletionCron {
						// No need to update the cron if it is unchanged.
						return nil
					}
					return &scop.UpdateTTLScheduleCron{
						TableID:     this.TableID,
						ScheduleID:  this.RowLevelTTL.ScheduleID,
						NewCronExpr: this.RowLevelTTL.DeletionCronOrDefault(),
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

// findPreviousTTLCron finds the DeletionCron from the TTL element for the same
// table with a lower SeqNum and the specified target status. In other words, it
// finds the TTL cron from the dropped RowLevelTTL element. Returns empty string
// if no such element exists.
func findPreviousTTLCron(this *scpb.RowLevelTTL, md *opGenContext) string {
	for _, t := range md.Targets {
		other, ok := t.Element().(*scpb.RowLevelTTL)
		if !ok || other == this {
			continue
		}
		if other.TableID == this.TableID &&
			other.RowLevelTTL.ScheduleID == this.RowLevelTTL.ScheduleID &&
			other.SeqNum < this.SeqNum &&
			t.TargetStatus == scpb.Status_ABSENT {
			return other.RowLevelTTL.DeletionCronOrDefault()
		}
	}
	return ""
}
