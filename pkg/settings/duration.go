// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

var _ internalSetting = &DurationSetting{}

var _ internalSetting = &DurationSettingWithExplicitUnit{}

// ErrorHint returns a hint message to be displayed on error to the user.
func (d *DurationSettingWithExplicitUnit) ErrorHint() (bool, string) {
	return true, "try using an interval value with explicit units, e.g 500ms or 1h2m"
}

// Get retrieves the duration value in the setting.
func (d *DurationSetting) Get(sv *Values) time.Duration {
	return time.Duration(sv.getInt64(d.slot))
}

func (d *DurationSetting) String(sv *Values) string {
	return EncodeDuration(d.Get(sv))
}

// DefaultString returns the default value for the setting as a string.
func (d *DurationSetting) DefaultString() string {
	return EncodeDuration(d.defaultValue)
}

// Encoded returns the encoded value of the current value of the setting.
func (d *DurationSetting) Encoded(sv *Values) string {
	return d.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (d *DurationSetting) EncodedDefault() string {
	return EncodeDuration(d.defaultValue)
}

// DecodeToString decodes and renders an encoded value.
func (d *DurationSetting) DecodeToString(encoded string) (string, error) {
	v, err := d.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return v.String(), nil
}

// DecodeValue decodes the value into a float.
func (d *DurationSetting) DecodeValue(encoded string) (time.Duration, error) {
	return time.ParseDuration(encoded)
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
	sv.setValueOrigin(ctx, d.slot, OriginOverride)
	sv.setInt64(ctx, d.slot, int64(v))
	sv.setDefaultOverride(d.slot, v)
}

func (d *DurationSetting) set(ctx context.Context, sv *Values, v time.Duration) error {
	if err := d.Validate(v); err != nil {
		return err
	}
	sv.setInt64(ctx, d.slot, int64(v))
	return nil
}

func (d *DurationSetting) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	v, err := d.DecodeValue(encoded)
	if err != nil {
		return err
	}
	return d.set(ctx, sv, v)
}

func (d *DurationSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	v, err := d.DecodeValue(encoded)
	if err != nil {
		return err
	}
	sv.setDefaultOverride(d.slot, v)
	return nil
}

func (d *DurationSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(d.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = d.set(ctx, sv, val.(time.Duration))
		return
	}
	if err := d.set(ctx, sv, d.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterDurationSetting defines a new setting with type duration.
func RegisterDurationSetting(
	class Class, key InternalKey, desc string, defaultValue time.Duration, opts ...SettingOption,
) *DurationSetting {
	validateFn := func(val time.Duration) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateDurationFn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateDurationFn(val); err != nil {
				return errors.Wrapf(err, "invalid value for %s", key)
			}
		}
		return nil
	}

	if err := validateFn(defaultValue); err != nil {
		panic(errors.Wrap(err, "invalid default"))
	}
	setting := &DurationSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}

// RegisterPublicDurationSettingWithExplicitUnit defines a new
// public setting with type duration which requires an explicit unit when being
// set.
func RegisterDurationSettingWithExplicitUnit(
	class Class, key InternalKey, desc string, defaultValue time.Duration, opts ...SettingOption,
) *DurationSettingWithExplicitUnit {
	validateFn := func(val time.Duration) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateDurationFn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateDurationFn(val); err != nil {
				return errors.Wrapf(err, "invalid value for %s", key)
			}
		}
		return nil
	}
	if err := validateFn(defaultValue); err != nil {
		panic(errors.Wrap(err, "invalid default"))
	}
	setting := &DurationSettingWithExplicitUnit{
		DurationSetting{
			defaultValue: defaultValue,
			validateFn:   validateFn,
		},
	}
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}

// NonNegativeDuration checks that the duration is greater or equal to
// zero. It can be passed to RegisterDurationSetting.
var NonNegativeDuration SettingOption = WithValidateDuration(nonNegativeDurationInternal)

func nonNegativeDurationInternal(v time.Duration) error {
	if v < 0 {
		return errors.Errorf("cannot be set to a negative duration: %s", v)
	}
	return nil
}

// DurationInRange returns a validation option that checks the value
// is within the given bounds (inclusive).
func DurationInRange(minVal, maxVal time.Duration) SettingOption {
	return WithValidateDuration(func(v time.Duration) error {
		if v < minVal || v > maxVal {
			return errors.Errorf("expected value in range [%v, %v], got: %v", minVal, maxVal, v)
		}
		return nil
	})
}

// NonNegativeDurationWithMaximum returns a validation option that
// checks the duration is in the range [0, maxValue]. It can be passed
// to RegisterDurationSetting.
func NonNegativeDurationWithMaximum(maxValue time.Duration) SettingOption {
	return DurationInRange(0, maxValue)
}

// DurationWithMinimum returns a validation option that checks the
// duration is greater or equal to the given minimum. It can be passed
// to RegisterDurationSetting.
func DurationWithMinimum(minValue time.Duration) SettingOption {
	return WithValidateDuration(func(v time.Duration) error {
		if minValue >= 0 {
			if err := nonNegativeDurationInternal(v); err != nil {
				return err
			}
		}
		if v < minValue {
			return errors.Errorf("cannot be set to a value smaller than %s", minValue)
		}
		return nil
	})
}

// DurationWithMinimumOrZeroDisable returns a validation option that
// checks the value is at least the given minimum, or zero to disable.
// It can be passed to RegisterDurationSetting.
func DurationWithMinimumOrZeroDisable(minValue time.Duration) SettingOption {
	return WithValidateDuration(func(v time.Duration) error {
		if minValue >= 0 && v < 0 {
			return errors.Errorf("cannot be set to a negative duration: %s", v)
		}
		if v != 0 && v < minValue {
			return errors.Errorf("cannot be set to a value smaller than %s",
				minValue)
		}
		return nil
	})
}

// PositiveDuration checks that the value is strictly positive. It can
// be passed to RegisterDurationSetting.
var PositiveDuration SettingOption = WithValidateDuration(func(v time.Duration) error {
	if v <= 0 {
		return errors.Errorf("cannot be set to a non-positive duration: %s", v)
	}
	return nil
})
