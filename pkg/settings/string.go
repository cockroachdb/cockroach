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

var _ extendedSetting = &StringSetting{}

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

// Typ returns the short (1 char) string denoting the type of setting.
func (*StringSetting) Typ() string {
	return "s"
}

// Default returns default value for setting.
func (s *StringSetting) Default() string {
	return s.defaultValue
}

// Defeat the linter.
var _ = (*StringSetting).Default

// Get retrieves the string value in the setting.
func (s *StringSetting) Get(sv *Values) string {
	loaded := sv.getGeneric(s.slotIdx)
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
	_ = s.set(ctx, sv, v)
}

func (s *StringSetting) set(ctx context.Context, sv *Values, v string) error {
	if err := s.Validate(sv, v); err != nil {
		return err
	}
	if s.Get(sv) != v {
		sv.setGeneric(ctx, s.slotIdx, v)
	}
	return nil
}

func (s *StringSetting) setToDefault(ctx context.Context, sv *Values) {
	if err := s.set(ctx, sv, s.defaultValue); err != nil {
		panic(err)
	}
}

// WithPublic sets public visibility and can be chained.
func (s *StringSetting) WithPublic() *StringSetting {
	s.SetVisibility(Public)
	return s
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(key, desc string, defaultValue string) *StringSetting {
	return RegisterValidatedStringSetting(key, desc, defaultValue, nil)
}

// RegisterValidatedStringSetting defines a new setting with type string with a
// validation function.
func RegisterValidatedStringSetting(
	key, desc string, defaultValue string, validateFn func(*Values, string) error,
) *StringSetting {
	if validateFn != nil {
		if err := validateFn(nil, defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &StringSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	// By default all string settings are considered to perhaps contain
	// PII and are thus non-reportable (to exclude them from telemetry
	// reports).
	setting.SetReportable(false)
	register(key, desc, setting)
	return setting
}
