// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	// DefaultString returns the default value for the setting as a string. This
	// is the same as calling String on the setting when it was never set.
	DefaultString() string

	// Description contains a helpful text explaining what the specific cluster
	// setting is for.
	Description() string

	// Visibility returns whether the setting is made publicly visible. Reserved
	// settings are still accessible to users, but they don't get listed out when
	// retrieving all settings.
	Visibility() Visibility

	// IsUnsafe returns whether the setting is unsafe, and thus requires
	// a special interlock to set.
	IsUnsafe() bool

	// ValueOrigin returns the origin of the current value.
	ValueOrigin(ctx context.Context, sv *Values) ValueOrigin
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
//     1. if an end-user may benefit from modifying the setting
//     through a SQL connection to a secondary tenant / virtual
//     cluster, AND different virtual clusters should be able to use
//     different values, the setting should have class
//     ApplicationLevel.
//
//     2. if a setting should only be controlled by SREs in a
//     multi-tenant deployment or in an organization that wishes to
//     shield DB operations from application development, OR the
//     behavior of CockroachDB is not well-defined if multiple tenants
//     (virtual clusters) use different values concurrently, the
//     setting must *not* use class ApplicationLevel.
//
//     3. if and only if a setting relevant to the KV/storage layer
//     and whose value is shared across all virtual clusters is ever
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
	//     end-users) in a multi-tenant environment, AND the behavior
	//     of CockroachDB is not well-defined if different virtual
	//     clusters were to use different values concurrently.
	//
	//     (If this part of the condition does not hold, consider
	//     ApplicationLevel instead.)
	//
	//   - AND, its value is never needed in the application layer (i.e.
	//     it is not read from SQL code, HTTP handlers or other code
	//     that would run in SQL pods in a multi-tenant environment).
	//
	//     (If this part of the condition does not hold, consider
	//     SystemVisible instead.)
	//
	//     Note: if a setting is only ever accessed in the SQL code
	//     after it has ascertained that it was running for the system
	//     interface; and we do not wish to display this setting in the
	//     list displayed by `SHOW ALL CLUSTER SETTINGS` in virtual
	//     clusters, the class SystemOnly is suitable. Hence the emphasis
	//     on "would run in SQL pods" above.
	SystemOnly Class = iota

	// SystemVisible settings are specific to the KV/storage layer and
	// are also visible to virtual clusters.
	//
	// As a rule of thumb, use this class if:
	//
	//   - the setting may be accessed from the shared KV/storage layer;
	//
	//   - AND the setting should only be controllable by operators of
	//     the storage cluster (e.g. SREs in the case of a multi-tenant
	//     environment) but having the storage cluster's value be
	//     observable by virtual clusters is desirable.
	//
	//     (If this part of the condition does not hold, consider
	//     ApplicationLevel instead.)
	//
	//   - AND, its value is sometimes needed in the application layer
	//     (i.e. it may be read from SQL code, HTTP handlers or other
	//     code that could run in SQL pods in a multi-tenant environment).
	//
	//     (If this part of the condition does not hold, consider
	//     SystemOnly instead.)
	//
	// One use cases is to enable optimizations in the KV client side of
	// virtual cluster RPCs. For example, if the storage layer uses a
	// setting to decide the size of a response, making the setting
	// visible enables the client in virtual cluster code to
	// pre-allocate response buffers to match the size they expect to
	// receive.
	//
	// Another use case is KV/storage feature flags, to enable a UX
	// improvement in virtual clusters by avoiding requesting
	// storage-level functionality when it is not enabled, such as
	// rangefeeds and provide a better error message.
	// (Without this extra logic, the error reported by KV/storage
	// might be hard to read by end-users.)
	//
	// As a reminder, the setting values observed by a virtual cluster,
	// both for its own application settings and those it observes for
	// system-visible settings, can be overridden using ALTER VIRTUAL
	// CLUSTER SET CLUSTER SETTING. This can be used during e.g.
	// troubleshooting, to allow changing the observed value for a
	// virtual cluster without a code change.
	//
	// A setting that is expected to be overridden in regular operation
	// to to control application-level behavior is still an application
	// setting, and should use the application class; system-visible is
	// for use by settings that control the storage layer but need to be
	// visible, rather than for settings that control the application
	// layer but are intended to be read-only. This latter case should
	// use ApplicationLayer, because it controls application behavior,
	// and then rely on an override to prevent modification from within
	// the virtual cluster.
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
	//     - end-users may benefit from modifying the setting
	//       through a SQL connection to a secondary tenant / virtual
	//       cluster.
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
	//
	// Note that each SQL layer has its own copy of ApplicationLevel
	// settings; including the system tenant/interface. However, they
	// are neatly partitioned such that a given virtual cluster can
	// never observe the value set for another virtual cluster nor that
	// set for the system tenant/interface.
	//
	// As a reminder, the setting values observed by a virtual cluster,
	// both for its own application settings and those it observes for
	// system-visible settings, can be overridden using ALTER VIRTUAL
	// CLUSTER SET CLUSTER SETTING. This can be used during e.g.
	// troubleshooting, to allow changing the observed value for a
	// virtual cluster without a code change.
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
	// OriginOverride indicates the value has been set via a test override.
	OriginOverride
)

func (v ValueOrigin) String() string {
	if v > OriginOverride {
		return fmt.Sprintf("invalid (%d)", v)
	}
	return [...]string{"default", "override", "external-override", "test-override"}[v]
}

// SafeValue implements the redact.SafeValue interface.
func (ValueOrigin) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (SettingName) SafeValue() {}

// SafeValue implements the redact.SafeValue interface.
func (InternalKey) SafeValue() {}
