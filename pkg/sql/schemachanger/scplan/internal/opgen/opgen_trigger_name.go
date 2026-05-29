// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.TriggerName)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TriggerName) *scop.SetTriggerName {
					return &scop.SetTriggerName{Name: *protoutil.Clone(this).(*scpb.TriggerName)}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				// Free the trigger name by replacing it with a per-trigger
				// placeholder so the descriptor remains valid while the parent
				// Trigger element either stays public (rename) or is being
				// removed (drop). Mirrors the column/index/constraint rename
				// pattern.
				emit(func(this *scpb.TriggerName) *scop.SetTriggerName {
					return &scop.SetTriggerName{
						Name: scpb.TriggerName{
							TableID:   this.TableID,
							TriggerID: this.TriggerID,
							Name:      tabledesc.TriggerNamePlaceholder(this.TriggerID),
						},
					}
				}),
			),
		),
	)
}
