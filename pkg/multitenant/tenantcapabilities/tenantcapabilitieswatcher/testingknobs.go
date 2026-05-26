// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitieswatcher

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
)

// TestingKnobs contain testing helpers which are used by the Watcher.
type TestingKnobs struct {
	// WatcherRangeFeedKnobs control the lifecycle events for the underlying
	// rangefeed of the tenant capabilities Watcher.
	WatcherRangeFeedKnobs base.ModuleTestingKnobs

	// WatcherUpdatesInterceptor, if set, is called each time the Watcher
	// receives a set of updates.
	WatcherUpdatesInterceptor func(update tenantcapabilities.Update)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
