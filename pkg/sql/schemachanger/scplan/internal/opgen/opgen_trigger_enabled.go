// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
				emit(func(this *scpb.TriggerEnabled) *scop.SetTriggerEnabled {
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
