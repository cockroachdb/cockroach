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
	"time"

	"github.com/cockroachdb/errors"
)

// DurationSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "duration" is updated.
type DurationSetting struct {
	common
	defaultValue time.Duration
	validateFn   func(time.Duration) error
}

// DurationSettingWithExplicitUnit is like DurationSetting except it requires an
// explicit unit when being set. (eg. 1s works, but 1 does not).
type DurationSettingWithExplicitUnit struct {
	DurationSetting
}

var _ extendedSetting = &DurationSetting{}

var _ extendedSetting = &DurationSettingWithExplicitUnit{}

// ErrorHint returns a hint message to be displayed on error to the user.
func (d *DurationSettingWithExplicitUnit) ErrorHint() (bool, string) {
	return true, "try using an interval value with explicit units, e.g 500ms or 1h2m"
}

// Get retrieves the duration value in the setting.
func (d *DurationSetting) Get(sv *Values) time.Duration {
	return time.Duration(sv.getInt64(d.slotIdx))
}

func (d *DurationSetting) String(sv *Values) string {
	return EncodeDuration(d.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (d *DurationSetting) Encoded(sv *Values) string {
	return d.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (d *DurationSetting) EncodedDefault() string {
	return EncodeDuration(d.defaultValue)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*DurationSetting) Typ() string {
	return "d"
}

// Default returns default value for setting.
func (d *DurationSetting) Default() time.Duration {
	return d.defaultValue
}

// Defeat the linter.
var _ = (*DurationSetting).Default

// Validate that a value conforms with the validation function.
func (d *DurationSetting) Validate(v time.Duration) error {
	if d.validateFn != nil {
		if err := d.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (d *DurationSetting) Override(ctx context.Context, sv *Values, v time.Duration) {
	sv.setInt64(ctx, d.slotIdx, int64(v))
	sv.setDefaultOverrideInt64(d.slotIdx, int64(v))
}

func (d *DurationSetting) set(ctx context.Context, sv *Values, v time.Duration) error {
	if err := d.Validate(v); err != nil {
		return err
	}
	sv.setInt64(ctx, d.slotIdx, int64(v))
	return nil
}

func (d *DurationSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	ok, val, _ := sv.getDefaultOverride(d.slotIdx)
	if ok {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = d.set(ctx, sv, time.Duration(val))
		return
	}
	if err := d.set(ctx, sv, d.defaultValue); err != nil {
		panic(err)
	}
}

// WithPublic sets the visibility to public and can be chained.
func (d *DurationSetting) WithPublic() *DurationSetting {
	d.SetVisibility(Public)
	return d
}

// WithSystemOnly marks this setting as system-only and can be chained.
func (d *DurationSetting) WithSystemOnly() *DurationSetting {
	d.common.systemOnly = true
	return d
}

// Defeat the linter.
var _ = (*DurationSetting).WithSystemOnly

// RegisterDurationSetting defines a new setting with type duration.
func RegisterDurationSetting(
	key, desc string, defaultValue time.Duration, validateFns ...func(time.Duration) error,
) *DurationSetting {
	var validateFn func(time.Duration) error
	if len(validateFns) > 0 {
		validateFn = func(v time.Duration) error {
			for _, fn := range validateFns {
				if err := fn(v); err != nil {
					return errors.Wrapf(err, "invalid value for %s", key)
				}
			}
			return nil
		}
	}

	if validateFn != nil {
		if err := validateFn(defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &DurationSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(key, desc, setting)
	return setting
}

// RegisterPublicDurationSettingWithExplicitUnit defines a new
// public setting with type duration which requires an explicit unit when being
// set.
func RegisterPublicDurationSettingWithExplicitUnit(
	key, desc string, defaultValue time.Duration, validateFn func(time.Duration) error,
) *DurationSettingWithExplicitUnit {
	var fn func(time.Duration) error

	if validateFn != nil {
		fn = func(v time.Duration) error {
			return errors.Wrapf(validateFn(v), "invalid value for %s", key)
		}
	}

	setting := &DurationSettingWithExplicitUnit{
		DurationSetting{
			defaultValue: defaultValue,
			validateFn:   fn,
		},
	}
	setting.SetVisibility(Public)
	register(key, desc, setting)
	return setting
}

// NonNegativeDuration can be passed to RegisterDurationSetting.
func NonNegativeDuration(v time.Duration) error {
	if v < 0 {
		return errors.Errorf("cannot be set to a negative duration: %s", v)
	}
	return nil
}

// NonNegativeDurationWithMaximum returns a validation function that can be
// passed to RegisterDurationSetting.
func NonNegativeDurationWithMaximum(maxValue time.Duration) func(time.Duration) error {
	return func(v time.Duration) error {
		if v < 0 {
			return errors.Errorf("cannot be set to a negative duration: %s", v)
		}
		if v > maxValue {
			return errors.Errorf("cannot be set to a value larger than %s", maxValue)
		}
		return nil
	}
}

// PositiveDuration can be passed to RegisterDurationSetting.
func PositiveDuration(v time.Duration) error {
	if v <= 0 {
		return errors.Errorf("cannot be set to a non-positive duration: %s", v)
	}
	return nil
}
