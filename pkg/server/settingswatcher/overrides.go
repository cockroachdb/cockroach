// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settingswatcher

import "github.com/cockroachdb/cockroach/pkg/settings"

// OverridesMonitor is an interface through which the settings watcher can
// receive setting overrides. Used for non-system tenants.
//
// The expected usage is to listen for a message on NotifyCh(), and use
// Current() to retrieve the updated list of overrides when a message is
// received.
type OverridesMonitor interface {
	// RegisterOverridesChannel returns a channel that receives a message
	// any time the current set of overrides changes.
	// The channel receives an initial event immediately.
	RegisterOverridesChannel() <-chan struct{}

	// Overrides retrieves the current set of setting overrides, as a map from
	// setting key to EncodedValue. Any settings that are present must be set to
	// the overridden value.
	Overrides() map[string]settings.EncodedValue
}
