// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settings

import (
	"context"
	"strings"
)

// Setting is the interface exposing the metadata for a cluster setting.
//
// The values for the settings are stored separately in Values. This allows
// having a global (singleton) settings registry while allowing potentially
// multiple "instances" (values) for each setting (e.g. for multiple test
// servers in the same process).
type Setting interface {
	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string
	// String returns the string representation of the setting's current value.
	// It's used when materializing results for `SHOW CLUSTER SETTINGS` or `SHOW
	// CLUSTER SETTING <setting-name>`.
	String(sv *Values) string
	// Description contains a helpful text explaining what the specific cluster
	// setting is for.
	Description() string
	// Visibility controls whether or not the setting is made publicly visible.
	// Reserved settings are still accessible to users, but they don't get
	// listed out when retrieving all settings.
	Visibility() Visibility

	// SystemOnly indicates if a setting is only applicable to the system tenant.
	SystemOnly() bool
}

// NonMaskedSetting is the exported interface of non-masked settings.
type NonMaskedSetting interface {
	Setting

	// Encoded returns the encoded representation of the current value of the
	// setting.
	Encoded(sv *Values) string
	// EncodedDefault returns the encoded representation of the default value of
	// the setting.
	EncodedDefault() string
	// SetOnChange installs a callback to be called when a setting's value
	// changes. `fn` should avoid doing long-running or blocking work as it is
	// called on the goroutine which handles all settings updates.
	SetOnChange(sv *Values, fn func(ctx context.Context))
	// ErrorHint returns a hint message to be displayed to the user when there's
	// an error.
	ErrorHint() (bool, string)
}

// Visibility describes how a user should feel confident that
// they can customize the setting.  See the constant definitions below
// for details.
type Visibility int

const (
	// Reserved - which is the default - indicates that a setting is
	// not documented and the CockroachDB team has not developed
	// internal experience about the impact of customizing it to other
	// values.
	// In short: "Use at your own risk."
	Reserved Visibility = iota
	// Public indicates that a setting is documented, the range of
	// possible values yields predictable results, and the CockroachDB
	// team is there to assist if issues occur as a result of the
	// customization.
	// In short: "Go ahead but be careful."
	Public
)

// AdminOnly returns whether the setting can only be viewed and modified by
// superusers. Otherwise, users with the MODIFYCLUSTERSETTING role privilege can
// do so.
func AdminOnly(name string) bool {
	return !strings.HasPrefix(name, "sql.defaults.")
}
