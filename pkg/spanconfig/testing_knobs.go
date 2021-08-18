// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import "github.com/cockroachdb/cockroach/pkg/base"

// TestingKnobs provide fine-grained control over the various span config
// components for testing.
type TestingKnobs struct {
	// ManagerDisableJobCreation disables creating the auto span config
	// reconciliation job.
	ManagerDisableJobCreation bool
	// ManagerCreatedJobInterceptor expects a *jobs.Job to be passed into it. It
	// takes an interface here to resolve a circular dependency.
	ManagerCreatedJobInterceptor func(interface{})
	// ManagerAfterCheckedReconciliationJobExistsInterceptor is run after the
	// manager has checked if the auto span config reconciliation job exists or
	// not.
	ManagerAfterCheckedReconciliationJobExistsInterceptor func(exists bool)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
