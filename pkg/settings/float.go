// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"math"
	"strconv"

	"github.com/cockroachdb/errors"
)

// FloatSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "float" is updated.
type FloatSetting struct {
	common
	defaultValue float64
	validateFn   func(float64) error
}

var _ internalSetting = &FloatSetting{}

// Get retrieves the float value in the setting.
func (f *FloatSetting) Get(sv *Values) float64 {
	return math.Float64frombits(uint64(sv.getInt64(f.slot)))
}

func (f *FloatSetting) String(sv *Values) string {
	return EncodeFloat(f.Get(sv))
}

// DefaultString returns the default value for the setting as a string.
func (f *FloatSetting) DefaultString() string {
	return EncodeFloat(f.defaultValue)
}

// Encoded returns the encoded value of the current value of the setting.
func (f *FloatSetting) Encoded(sv *Values) string {
	return f.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (f *FloatSetting) EncodedDefault() string {
	return EncodeFloat(f.defaultValue)
}

// DecodeToString decodes and renders an encoded value.
func (f *FloatSetting) DecodeToString(encoded string) (string, error) {
	fv, err := f.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return EncodeFloat(fv), nil
}

// DecodeValue decodes the value into a float.
func (f *FloatSetting) DecodeValue(encoded string) (float64, error) {
	return strconv.ParseFloat(encoded, 64)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*FloatSetting) Typ() string {
	return "f"
}

// Default returns default value for setting.
func (f *FloatSetting) Default() float64 {
	return f.defaultValue
}

// Defeat the linter.
var _ = (*FloatSetting).Default

// Override changes the setting panicking if validation fails and also overrides
// the default value.
//
// For testing usage only.
func (f *FloatSetting) Override(ctx context.Context, sv *Values, v float64) {
	sv.setValueOrigin(ctx, f.slot, OriginOverride)
	if err := f.set(ctx, sv, v); err != nil {
		panic(err)
	}
	sv.setDefaultOverride(f.slot, v)
}

// Validate that a value conforms with the validation function.
func (f *FloatSetting) Validate(v float64) error {
	if f.validateFn != nil {
		if err := f.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

func (f *FloatSetting) set(ctx context.Context, sv *Values, v float64) error {
	if err := f.Validate(v); err != nil {
		return err
	}
	sv.setInt64(ctx, f.slot, int64(math.Float64bits(v)))
	return nil
}

func (f *FloatSetting) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	v, err := f.DecodeValue(encoded)
	if err != nil {
		return err
	}
	return f.set(ctx, sv, v)
}

func (f *FloatSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	v, err := f.DecodeValue(encoded)
	if err != nil {
		return err
	}
	sv.setDefaultOverride(f.slot, v)
	return nil
}

func (f *FloatSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(f.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = f.set(ctx, sv, val.(float64))
		return
	}
	if err := f.set(ctx, sv, f.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(
	class Class, key InternalKey, desc string, defaultValue float64, opts ...SettingOption,
) *FloatSetting {
	validateFn := func(v float64) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateFloat64Fn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateFloat64Fn(v); err != nil {
				return err
			}
		}
		return nil
	}

	if err := validateFn(defaultValue); err != nil {
		panic(errors.Wrap(err, "invalid default"))
	}
	setting := &FloatSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}

// NonNegativeFloat checks the value is greater or equal to zero. It
// can be passed to RegisterFloatSetting.
var NonNegativeFloat SettingOption = FloatWithMinimum(0)

// FloatWithMinimum returns a validation option that checks that the
// value is at least the given minimum. It can be passed to
// RegisterFloatSetting.
func FloatWithMinimum(minVal float64) SettingOption {
	return WithValidateFloat(func(v float64) error {
		if minVal >= 0 && v < 0 {
			return errors.Errorf("cannot set to a negative value: %f", v)
		}
		if v < minVal {
			return errors.Errorf("cannot set to a value lower than %f: %f", minVal, v)
		}
		return nil
	})
}

// FloatWithMinimumOrZeroDisable returns a validation option that
// verifies the value is at least the given minimum, or zero to
// disable. It can be passed to RegisterFloatSetting.
func FloatWithMinimumOrZeroDisable(minVal float64) SettingOption {
	return WithValidateFloat(func(v float64) error {
		if minVal >= 0 && v < 0 {
			return errors.Errorf("cannot set to a negative value: %f", v)
		}
		if v != 0 && v < minVal {
			return errors.Errorf("cannot set to a value lower than %f: %f", minVal, v)
		}
		return nil
	})
}

// NonNegativeFloatWithMaximum returns a validation option that checks
// that the value is in the range [0, maxValue]. It can be passed to
// RegisterFloatSetting.
func NonNegativeFloatWithMaximum(maxValue float64) SettingOption {
	return FloatInRange(0, maxValue)
}

// PositiveFloat checks that the value is strictly greater than zero.
// It can be passed to RegisterFloatSetting.
var PositiveFloat SettingOption = WithValidateFloat(func(v float64) error {
	if v <= 0 {
		return errors.Errorf("cannot set to a non-positive value: %f", v)
	}
	return nil
})

// NonZeroFloat checks that the value is non-zero. It can be passed to
// RegisterFloatSetting.
var NonZeroFloat SettingOption = WithValidateFloat(func(v float64) error {
	if v == 0 {
		return errors.New("cannot set to zero value")
	}
	return nil
})

// Fraction requires the setting to be in the interval [0, 1]. It can
// be passed to RegisterFloatSetting.
var Fraction SettingOption = FloatInRange(0, 1)

// FloatInRange returns a validation option that checks the value is
// within the given bounds (inclusive).
func FloatInRange(minVal, maxVal float64) SettingOption {
	return WithValidateFloat(func(v float64) error {
		if v < minVal || v > maxVal {
			return errors.Errorf("expected value in range [%f, %f], got: %f", minVal, maxVal, v)
		}
		return nil
	})
}

// FractionUpperExclusive requires the setting to be in the interval
// [0, 1). It can be passed to RegisterFloatSetting.
var FractionUpperExclusive SettingOption = FloatInRangeUpperExclusive(0, 1)

// FloatInRangeUpperExclusive returns a validation option that checks
// the value is within the given bounds (inclusive lower, exclusive
// upper).
func FloatInRangeUpperExclusive(minVal, maxVal float64) SettingOption {
	return WithValidateFloat(func(v float64) error {
		if v < minVal || v >= maxVal {
			return errors.Errorf("expected value in range [%f, %f), got: %f", minVal, maxVal, v)
		}
		return nil
	})
}
