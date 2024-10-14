// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// SettingOption is the type of an option that can be passed to Register.
type SettingOption struct {
	commonOpt          func(*common)
	validateBoolFn     func(*Values, bool) error
	validateDurationFn func(time.Duration) error
	validateInt64Fn    func(int64) error
	validateFloat64Fn  func(float64) error
	validateStringFn   func(*Values, string) error
	validateProtoFn    func(*Values, protoutil.Message) error
}

// NameStatus indicates the status of a setting name.
type NameStatus bool

const (
	// NameActive indicates that the name is currently in use.
	NameActive NameStatus = true
	// NameRetired indicates that the name is no longer in use.
	NameRetired NameStatus = false
)

// WithName configures the user-visible name of the setting.
func WithName(name SettingName) SettingOption {
	return SettingOption{commonOpt: func(c *common) {
		c.setName(name)
		registerAlias(c.key, name, NameActive)
	}}
}

// WithRetiredName configures a previous user-visible name of the setting,
// when that name was different from the key and is not in use any more.
func WithRetiredName(name SettingName) SettingOption {
	return SettingOption{commonOpt: func(c *common) {
		registerAlias(c.key, name, NameRetired)
	}}
}

// WithVisibility customizes the visibility of a setting.
func WithVisibility(v Visibility) SettingOption {
	return SettingOption{commonOpt: func(c *common) {
		c.setVisibility(v)
	}}
}

// WithUnsafe indicates that the setting is unsafe.
// Unsafe settings need an interlock to be updated. They also cannot be public.
var WithUnsafe SettingOption = SettingOption{commonOpt: func(c *common) {
	c.setUnsafe()
}}

// WithPublic sets public visibility.
var WithPublic SettingOption = WithVisibility(Public)

// WithReportable indicates whether a setting's value can show up in SHOW ALL
// CLUSTER SETTINGS and telemetry reports.
func WithReportable(reportable bool) SettingOption {
	return SettingOption{commonOpt: func(c *common) {
		c.setReportable(reportable)
	}}
}

// Retired marks the setting as obsolete. It also hides it from the
// output of SHOW CLUSTER SETTINGS. Note: in many case the setting
// definition can be removed outright, and its name added to the
// retiredSettings map (in settings/registry.go). The Retired option
// exists for cases where there is a need to maintain compatibility
// with automation.
var Retired SettingOption = SettingOption{commonOpt: func(c *common) {
	c.setRetired()
}}

// Sensitive marks the setting as sensitive, which means that it will always
// be redacted in any output. It also makes the setting non-reportable. It can
// only be used for string settings.
var Sensitive SettingOption = SettingOption{commonOpt: func(c *common) {
	c.setSensitive()
}}

// WithValidateDuration adds a validation function for a duration setting.
func WithValidateDuration(fn func(time.Duration) error) SettingOption {
	return SettingOption{validateDurationFn: fn}
}

// WithValidateInt adds a validation function for an int64 setting.
func WithValidateInt(fn func(int64) error) SettingOption {
	return SettingOption{validateInt64Fn: fn}
}

// WithValidateFloat adds a validation function for a float64 setting.
func WithValidateFloat(fn func(float64) error) SettingOption {
	return SettingOption{validateFloat64Fn: fn}
}

// WithValidateBool adds a validation function for a boolean setting.
func WithValidateBool(fn func(*Values, bool) error) SettingOption {
	return SettingOption{validateBoolFn: fn}
}

// WithValidateString adds a validation function for a string setting.
func WithValidateString(fn func(*Values, string) error) SettingOption {
	return SettingOption{validateStringFn: fn}
}

// WithValidateProto adds a validation function for a proto setting.
func WithValidateProto(fn func(*Values, protoutil.Message) error) SettingOption {
	return SettingOption{validateProtoFn: fn}
}

func (c *common) apply(opts []SettingOption) {
	for _, opt := range opts {
		if opt.commonOpt != nil {
			opt.commonOpt(c)
		}
	}
}
