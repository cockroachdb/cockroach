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
	"fmt"
)

// SettingName represents the user-visible name of a cluster setting.
// The name is suitable for:
// - SHOW/SET CLUSTER SETTING.
// - inclusion in error messages.
//
// For internal storage, use the InternalKey type instead.
type SettingName string

// InternalKey is the internal (storage) key for a cluster setting.
// The internal key is suitable for use with:
// - direct accesses to system.settings.
// - rangefeed logic associated with system.settings.
// - (in unit tests only) interchangeably with the name for SET CLUSTER SETTING.
//
// For user-visible displays, use the SettingName type instead.
type InternalKey string

// Setting is the interface exposing the metadata for a cluster setting.
//
// The values for the settings are stored separately in Values. This allows
// having a global (singleton) settings registry while allowing potentially
// multiple "instances" (values) for each setting (e.g. for multiple test
// servers in the same process).
type Setting interface {
	// Class returns the scope of the setting in multi-tenant scenarios.
	Class() Class

	// InternalKey returns the internal key used to store the setting.
	// To display the name of the setting (eg. in errors etc) or the
	// SET/SHOW CLUSTER SETTING statements, use the Name() method instead.
	//
	// The internal key is suitable for use with:
	// - direct accesses to system.settings.
	// - rangefeed logic associated with system.settings.
	// - (in unit tests only) interchangeably with the name for SET CLUSTER SETTING.
	InternalKey() InternalKey

	// Name is the user-visible (display) name of the setting.
	// The name is suitable for:
	// - SHOW/SET CLUSTER SETTING.
	// - inclusion in error messages.
	Name() SettingName

	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string

	// String returns the string representation of the setting's current value.
	// It's used when materializing results for `SHOW CLUSTER SETTINGS` or `SHOW
	// CLUSTER SETTING <setting-name>`.
	//
	// If this object implements a non-reportable setting that was retrieved for
	// reporting (see LookupForReportingByKey), String hides the actual value.
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
// LookupForReportingByKey).
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

	// ValueOrigin returns the origin of the current value.
	ValueOrigin(ctx context.Context, sv *Values) ValueOrigin
}

// Class describes the scope of a setting under cluster
// virtualization.
//
// Guidelines for choosing a class:
//
//   - Make sure to read the descriptions below carefully to
//     understand the differences in semantics.
//
//   - Rules of thumb:
//
//     1. if an end-user should be able to modify the setting in
//     CockroachCloud Serverless, AND different end-users should be
//     able to use different values, the setting should have class
//     ApplicationLevel.
//
//     2. if a setting should only be controlled by SREs in
//     CockroachCloud Serverless, OR a single value must apply to
//     multiple tenants (virtual clusters) simultaneously, the setting
//     must *not* use class ApplicationLevel.
//
//     3. if and only if a setting relevant to the KV/storage layer
//     and whose value is shared across all virtual cluster is ever
//     accessed by code in the application layer (SQL execution or
//     HTTP handlers), use SystemVisible.
//
//     4. in other cases, use SystemOnly.
type Class int8

const (
	// SystemOnly settings are specific to the KV/storage layer and
	// cannot be accessed from application layer code (in particular not
	// from SQL layer nor HTTP handlers).
	//
	// As a rule of thumb, use this class if:
	//
	//   - the setting may be accessed from the shared KV/storage layer;
	//
	//   - AND, the setting should only be controllable by SREs (not
	//     end-users) in CockroachCloud Serverless, AND a single value
	//     must apply to all virtual clusters simultaneously;
	//
	//     (If this part of the condition does not hold, consider
	//     ApplicationLevel instead.)
	//
	//   - AND, its value is never needed in the application layer (i.e.
	//     it is not read from SQL code, HTTP handlers or other code
	//     that would run in SQL pods in CC Serverless).
	//
	//     (If this part of the condition does not hold, consider
	//     SystemVisible instead.)
	SystemOnly Class = iota

	// SystemVisible settings are specific to the KV/storage layer and
	// are also visible to virtual clusters.
	//
	// As a rule of thumb, use this class if:
	//
	//   - the setting may be accessed from the shared KV/storage layer;
	//
	//   - AND the setting should only be controllable by SREs (not
	//     end-users) in CockroachCloud Serverless, AND a single value
	//     must apply to all virtual clusters simultaneously;
	//
	//     (If this part of the condition does not hold, consider
	//     ApplicationLevel instead.)
	//
	//   - AND, its value is sometimes needed in the application layer
	//     (i.e. it may be read from SQL code, HTTP handlers or other
	//     code that could run in SQL pods in CC Serverless).
	//
	//     (If this part of the condition does not hold, consider
	//     SystemOnly instead.)
	SystemVisible

	// ApplicationLevel settings are readable and can optionally be
	// modified by virtual clusters.
	//
	// As a rule of thumb, use this class if:
	//
	//   - the setting is never accessed by the shared KV/storage layer;
	//
	//   - AND, any of the following holds:
	//
	//     - end-users should legitimately be able to modify
	//       the setting in CockroachCloud Serverless;
	//
	//       (If this part of the condition does not hold, but the other
	//       part of the condition below does hold, consider still using
	//       ApplicationLevel and force an override using ALTER VIRTUAL
	//       CLUSTER SET CLUSTER SETTING. This makes the
	//       ApplicationLevel setting effectively read-only from the
	//       virtual cluster's perspective, because system overrides
	//       cannot be modified by the virtual cluster.)
	//
	//     - OR, different virtual clusters should be able to
	//       use different values for the setting.
	//
	//       (If this part of the condition does not hold, consider
	//       SystemOnly or SystemVisible instead.)
	ApplicationLevel
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

// ValueOrigin indicates the origin of the current value of a setting, e.g. if
// it is coming from the in-code default or an explicit override.
type ValueOrigin uint32

const (
	// OriginDefault indicates the value in use is the default value.
	OriginDefault ValueOrigin = iota
	// OriginExplicitlySet indicates the value is has been set explicitly.
	OriginExplicitlySet
	// OriginExternallySet indicates the value has been set externally, such as
	// via a host-cluster override for this or all tenant(s).
	OriginExternallySet
)

func (v ValueOrigin) String() string {
	if v > OriginExternallySet {
		return fmt.Sprintf("invalid (%d)", v)
	}
	return [...]string{"default", "override", "external-override"}[v]
}

// SafeValue implements the redact.SafeValue interface.
func (ValueOrigin) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (SettingName) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (InternalKey) SafeValue() {}
