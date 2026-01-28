// Copyright 2024 The Cockroach Authors.
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
	opRegistry.register((*scpb.TriggerEnabled)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TriggerEnabled) *scop.SetTriggerEnabled {
					return &scop.SetTriggerEnabled{
						TableID:   this.TableID,
						TriggerID: this.TriggerID,
						Enabled:   true,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TriggerEnabled, md *opGenContext) *scop.SetTriggerEnabled {
					// If the trigger itself is being dropped, don't emit an operation
					// to set its enabled state - the trigger will be removed anyway.
					if isTriggerBeingDropped(this.TableID, this.TriggerID, md) {
						return nil
					}
					return &scop.SetTriggerEnabled{
						TableID:   this.TableID,
						TriggerID: this.TriggerID,
						Enabled:   false,
					}
				}),
			),
		),
	)
}

// isTriggerBeingDropped returns true if the Trigger element for the specified
// trigger is going to ABSENT.
func isTriggerBeingDropped(
	tableID catid.DescID, triggerID catid.TriggerID, md *opGenContext,
) bool {
	for _, t := range md.Targets {
		trigger, ok := t.Element().(*scpb.Trigger)
		if !ok {
			continue
		}
		if trigger.TableID == tableID &&
			trigger.TriggerID == triggerID &&
			t.TargetStatus == scpb.Status_ABSENT {
			return true
		}
	}
	return false
}
