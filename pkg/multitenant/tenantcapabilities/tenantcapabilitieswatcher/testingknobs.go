// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitieswatcher

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
)

// TestingKnobs contain testing helpers which are used by the Watcher.
type TestingKnobs struct {
	// WatcherRangeFeedKnobs control the lifecycle events for the underlying
	// rangefeed of the tenant capabilities Watcher.
	WatcherRangeFeedKnobs base.ModuleTestingKnobs

	// WatcherUpdatesInterceptor, if set, is called each time the Watcher
	// receives a set of updates.
	WatcherUpdatesInterceptor func(
		updateType rangefeedcache.UpdateType, updates []tenantcapabilities.Update,
	)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
