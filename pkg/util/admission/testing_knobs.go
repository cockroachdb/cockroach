// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// TestingKnobs provide fine-grained control over the various admission control
// components for testing.
//
// TODO(irfansharif): Consolidate the various other testing-knob like things (in
// admission.Options, for example) into this one struct.
type TestingKnobs struct {
	// AdmittedReplicatedWorkInterceptor is invoked whenever replicated work is
	// admitted.
	AdmittedReplicatedWorkInterceptor func(
		tenantID roachpb.TenantID,
		pri admissionpb.WorkPriority,
		rwi ReplicatedWorkInfo,
		originalTokens int64,
		createTime int64,
	)

	// DisableWorkQueueFastPath disables the fast-path in work queues.
	DisableWorkQueueFastPath bool

	// DisableWorkQueueGranting disables the work queue from granting admission
	// to waiting work.
	DisableWorkQueueGranting func() bool

	// AlwaysTryGrantWhenAdmitted causes the granter to unconditionally try
	// admitting another request when admitting one.
	AlwaysTryGrantWhenAdmitted bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
