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

import "context"

// Setting is the interface exposing the metadata for a cluster setting.
//
// The values for the settings are stored separately in Values. This allows
// having a global (singleton) settings registry while allowing potentially
// multiple "instances" (values) for each setting (e.g. for multiple test
// servers in the same process).
type Setting interface {
	// Class returns the scope of the setting in multi-tenant scenarios.
	Class() Class

	// Key returns the name of the specific cluster setting.
	Key() string

	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string

	// String returns the string representation of the setting's current value.
	// It's used when materializing results for `SHOW CLUSTER SETTINGS` or `SHOW
	// CLUSTER SETTING <setting-name>`.
	//
	// If this object implements a non-reportable setting that was retrieved for
	// reporting (see LookupForReporting), String hides the actual value.
	String(sv *Values) string

	// Description contains a helpful text explaining what the specific cluster
	// setting is for.
	Description() string

	// Visibility returns whether the setting is made publicly visible. Reserved
	// settings are still accessible to users, but they don't get listed out when
	// retrieving all settings.
	Visibility() Visibility
}

// NonMaskedSetting is the exported interface of non-masked settings. A
// non-masked setting provides access to the current value (even if the setting
// is not reportable).
//
// A non-masked setting must not be used in the context of reporting values (see
// LookupForReporting).
type NonMaskedSetting interface {
	Setting

	// Encoded returns the encoded representation of the current value of the
	// setting.
	Encoded(sv *Values) string

	// EncodedDefault returns the encoded representation of the default value of
	// the setting.
	EncodedDefault() string

	// DecodeToString returns the string representation of the provided
	// encoded value.
	// This is analogous to loading the setting from storage and
	// then calling String() on it, however the setting is not modified.
	// The string representation of the default value can be obtained
	// by calling EncodedDefault() and then DecodeToString().
	DecodeToString(encoded string) (repr string, err error)

	// SetOnChange installs a callback to be called when a setting's value
	// changes. `fn` should avoid doing long-running or blocking work as it is
	// called on the goroutine which handles all settings updates.
	SetOnChange(sv *Values, fn func(ctx context.Context))

	// ErrorHint returns a hint message to be displayed to the user when there's
	// an error.
	ErrorHint() (bool, string)
}

// Class describes the scope of a setting in multi-tenant scenarios. While all
// settings can be used on the system tenant, the classes restrict use on
// non-system tenants.
//
// Settings can only be registered via the Class, e.g.
// SystemOnly.RegisterIntSetting().
//
// Guidelines for choosing a class:
//
//   - Make sure to read the descriptions below carefully to understand the
//     differences in semantics.
//
//   - If the setting controls a user-visible aspect of SQL, it should be a
//     TenantWritable setting.
//
//   - Control settings relevant to tenant-specific internal implementation
//     should be TenantReadOnly.
//
//   - When in doubt, the first choice to consider should be TenantReadOnly.
//
//   - SystemOnly should be used with caution: even internal tenant code is
//     disallowed from using these settings at all.
type Class int8

const (
	// SystemOnly settings are associated with single-tenant clusters and host
	// clusters. Settings with this class do not exist on non-system tenants and
	// can only be used by the system tenant.
	SystemOnly Class = iota

	// TenantReadOnly settings are visible to non-system tenants but cannot be
	// modified by the tenant. Values for these settings are set from the system
	// tenant and propagated from the host cluster.
	TenantReadOnly

	// TenantWritable settings are visible to and can be modified by non-system
	// tenants. The system can still override these settings; the overrides are
	// propagated from the host cluster.
	TenantWritable
)

// Visibility describes how a user should feel confident that they can customize
// the setting.
//
// See the constant definitions below for details.
type Visibility int8

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
