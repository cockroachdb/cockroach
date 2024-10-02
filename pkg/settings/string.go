// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"

	"github.com/cockroachdb/errors"
)

// StringSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type StringSetting struct {
	defaultValue string
	validateFn   func(*Values, string) error
	common
}

var _ internalSetting = &StringSetting{}

func (s *StringSetting) String(sv *Values) string {
	return s.Get(sv)
}

// Encoded returns the encoded value of the current value of the setting.
func (s *StringSetting) Encoded(sv *Values) string {
	return s.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (s *StringSetting) EncodedDefault() string {
	return s.defaultValue
}

// DecodeToString decodes and renders an encoded value.
func (s *StringSetting) DecodeToString(encoded string) (string, error) {
	return encoded, nil
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StringSetting) Typ() string {
	return "s"
}

// Default returns default value for setting.
func (s *StringSetting) Default() string {
	return s.defaultValue
}

// DefaultString returns the default value for the setting as a string.
func (s *StringSetting) DefaultString() string {
	return s.defaultValue
}

// Defeat the linter.
var _ = (*StringSetting).Default

// Get retrieves the string value in the setting.
func (s *StringSetting) Get(sv *Values) string {
	loaded := sv.getGeneric(s.slot)
	if loaded == nil {
		return ""
	}
	return loaded.(string)
}

// Validate that a value conforms with the validation function.
func (s *StringSetting) Validate(sv *Values, v string) error {
	if s.validateFn != nil {
		if err := s.validateFn(sv, v); err != nil {
			return err
		}
	}
	return nil
}

// Override sets the setting to the given value, assuming
// it passes validation.
func (s *StringSetting) Override(ctx context.Context, sv *Values, v string) {
	sv.setValueOrigin(ctx, s.slot, OriginOverride)
	_ = s.decodeAndSet(ctx, sv, v)
	sv.setDefaultOverride(s.slot, v)
}

func (s *StringSetting) decodeAndSet(ctx context.Context, sv *Values, v string) error {
	if err := s.Validate(sv, v); err != nil {
		return err
	}
	if s.Get(sv) != v {
		sv.setGeneric(ctx, s.slot, v)
	}
	return nil
}

func (s *StringSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, v string,
) error {
	sv.setDefaultOverride(s.slot, v)
	return nil
}

func (s *StringSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(s.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = s.decodeAndSet(ctx, sv, val.(string))
		return
	}
	if err := s.decodeAndSet(ctx, sv, s.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(
	class Class, key InternalKey, desc string, defaultValue string, opts ...SettingOption,
) *StringSetting {
	validateFn := func(sv *Values, val string) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateStringFn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateStringFn(sv, val); err != nil {
				return err
			}
		}
		return nil
	}
	setting := &StringSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	// By default all string settings are considered to perhaps contain
	// PII and are thus non-reportable (to exclude them from telemetry
	// reports).
	setting.setReportable(false)
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}
