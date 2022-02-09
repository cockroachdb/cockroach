// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
