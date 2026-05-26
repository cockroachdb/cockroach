// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.Trigger)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Trigger) *scop.AddTrigger {
					return &scop.AddTrigger{Trigger: *protoutil.Clone(this).(*scpb.Trigger)}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				revertibleFunc(func(element scpb.Element, context *opGenContext) bool {
					// Starting 26.3 dropping a trigger is non-revertible, since the user
					// would be missing events if we rolled back.
					return !context.ActiveVersion.IsActiveVersion(clusterversion.V26_3.Version())
				}),
				emit(func(this *scpb.Trigger) *scop.RemoveTrigger {
					return &scop.RemoveTrigger{Trigger: *protoutil.Clone(this).(*scpb.Trigger)}
				}),
			),
		),
	)
}
