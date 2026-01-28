// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.TriggerEnabled)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TriggerEnabled) *scop.SetTriggerEnabled {
					return &scop.SetTriggerEnabled{Enabled: *protoutil.Clone(this).(*scpb.TriggerEnabled)}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TriggerEnabled, md *opGenContext) *scop.SetTriggerEnabled {
					if triggerEnabledAppliedLater(this, md) {
						// Another TriggerEnabled element for the same trigger is going
						// PUBLIC, so it will set the enabled state. No need to emit
						// an operation here.
						return nil
					}
					// This shouldn't happen in normal operation - a trigger being
					// dropped will use the full drop path which handles enabled state.
					// But if we get here, return nil to avoid generating invalid ops.
					return nil
				}),
			),
		),
	)
}

// triggerEnabledAppliedLater returns true if there is another TriggerEnabled
// element for the same trigger that is going to PUBLIC. This handles both:
// - Forward operations: a higher SeqNum element is going PUBLIC (new enabled state)
// - Rollbacks: a lower SeqNum element is being restored to PUBLIC (old enabled state)
func triggerEnabledAppliedLater(this *scpb.TriggerEnabled, md *opGenContext) bool {
	for _, t := range md.Targets {
		other, ok := t.Element().(*scpb.TriggerEnabled)
		if !ok || other == this {
			continue
		}
		if other.TableID == this.TableID &&
			other.TriggerID == this.TriggerID &&
			t.TargetStatus == scpb.Status_PUBLIC &&
			((md.InRollback && other.SeqNum < this.SeqNum) ||
				(!md.InRollback && other.SeqNum > this.SeqNum)) {
			return true
		}
	}
	return false
}
