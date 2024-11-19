// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settingswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// OverridesMonitor is an interface through which the settings watcher can
// receive setting overrides. Used for non-system tenants.
type OverridesMonitor interface {
	// WaitForStart waits until the overrides are ready for consumption.
	WaitForStart(ctx context.Context) error

	// Overrides retrieves the current set of setting overrides, as a
	// map from setting key to EncodedValue. Any settings that are
	// present must be set to the overridden value. It also returns a
	// channel that will be closed when the overrides are updated.
	Overrides() (map[settings.InternalKey]settings.EncodedValue, <-chan struct{})
}
