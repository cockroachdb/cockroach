// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilities

import "github.com/cockroachdb/cockroach/pkg/base"

// TestingKnobs contain testing helpers which are used by various components
// that enable tenant capabilities.
type TestingKnobs struct {
	// WatcherTestingKnobs can be used to test the tenant capabilities Watcher.
	WatcherTestingKnobs base.ModuleTestingKnobs
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
