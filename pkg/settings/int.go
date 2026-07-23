// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
)

// IntSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "int" is updated.
type IntSetting struct {
	common
	defaultValue int64
	validateFn   func(int64) error
}

var _ internalSetting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get(sv *Values) int64 {
	return sv.container.getInt64(i.slot)
}

func (i *IntSetting) String(sv *Values) string {
	return EncodeInt(i.Get(sv))
}

// DefaultString returns the default value for the setting as a string.
func (i *IntSetting) DefaultString() string {
	return EncodeInt(i.defaultValue)
}

// Encoded returns the encoded value of the current value of the setting.
func (i *IntSetting) Encoded(sv *Values) string {
	return i.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (i *IntSetting) EncodedDefault() string {
	return EncodeInt(i.defaultValue)
}

// DecodeToString decodes and renders an encoded value.
func (i *IntSetting) DecodeToString(encoded string) (string, error) {
	iv, err := i.DecodeNumericValue(encoded)
	if err != nil {
		return "", err
	}
	return EncodeInt(iv), nil
}

// DecodeValue decodes the value into an integer.
func (i *IntSetting) DecodeNumericValue(value string) (int64, error) {
	return strconv.ParseInt(value, 10, 64)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*IntSetting) Typ() string {
	return "i"
}

// Default returns default value for setting.
func (i *IntSetting) Default() int64 {
	return i.defaultValue
}

// Defeat the linter.
var _ = (*IntSetting).Default

// Validate that a value conforms with the validation function.
func (i *IntSetting) Validate(v int64) error {
	if i.validateFn != nil {
		if err := i.validateFn(v); err != nil {
			return err
		}
	}
	return nil
}

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (i *IntSetting) Override(ctx context.Context, sv *Values, v int64) {
	sv.setValueOrigin(ctx, i.slot, OriginOverride)
	sv.setInt64(ctx, i.slot, v)
	sv.setDefaultOverride(i.slot, v)
}

func (i *IntSetting) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	v, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return err
	}
	if err := i.Validate(v); err != nil {
		return err
	}
	sv.setInt64(ctx, i.slot, v)
	return nil
}

func (i *IntSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	v, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return err
	}
	sv.setDefaultOverride(i.slot, v)
	return nil
}

func (i *IntSetting) set(ctx context.Context, sv *Values, v int64) error {
	if err := i.Validate(v); err != nil {
		return err
	}
	sv.setInt64(ctx, i.slot, v)
	return nil
}

func (i *IntSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(i.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = i.set(ctx, sv, val.(int64))
		return
	}
	if err := i.set(ctx, sv, i.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterIntSetting defines a new setting with type int with a
// validation function.
func RegisterIntSetting(
	class Class, key InternalKey, desc string, defaultValue int64, opts ...SettingOption,
) *IntSetting {
	validateFn := func(val int64) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateInt64Fn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateInt64Fn(val); err != nil {
				return err
			}
		}
		return nil
	}
	if err := validateFn(defaultValue); err != nil {
		panic(errors.Wrap(err, "invalid default"))
	}
	setting := &IntSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}

// PositiveInt can be passed to RegisterIntSetting.
var PositiveInt SettingOption = WithValidateInt(func(v int64) error {
	if v <= 0 {
		return errors.Errorf("cannot be set to a non-positive value: %d", v)
	}
	return nil
})

// NonNegativeInt checks the value is zero or positive. It can be
// passed to RegisterIntSetting.
var NonNegativeInt SettingOption = WithValidateInt(nonNegativeIntInternal)

func nonNegativeIntInternal(v int64) error {
	if v < 0 {
		return errors.Errorf("cannot be set to a negative value: %d", v)
	}
	return nil
}

// IntWithMinimum returns a validation option that checks
// that the value is greater or equal to the given minimum. It can be
// passed to RegisterIntSetting.
func IntWithMinimum(minVal int64) SettingOption {
	return WithValidateInt(func(v int64) error {
		if minVal >= 0 {
			if err := nonNegativeIntInternal(v); err != nil {
				return err
			}
		}
		if v < minVal {
			return errors.Errorf("cannot be set to a value lower than %d: %d", minVal, v)
		}
		return nil
	})
}

// NonNegativeIntWithMaximum returns a validation option that checks
// that the value is in the range [0, maxValue]. It can be passed to
// RegisterIntSetting.
func NonNegativeIntWithMaximum(maxValue int64) SettingOption {
	return IntInRange(0, maxValue)
}

// IntInRange returns a validation option that checks the value is
// within the given bounds (inclusive). It can be passed to
// RegisterIntSetting.
func IntInRange(minVal, maxVal int64) SettingOption {
	return WithValidateInt(func(v int64) error {
		if v < minVal || v > maxVal {
			return errors.Errorf("expected value in range [%d, %d], got: %d", minVal, maxVal, v)
		}
		return nil
	})
}

// IntInRangeOrZeroDisable returns a validation option that checks the
// value is within the given bounds (inclusive) or is zero (disabled).
// It can be passed to RegisterIntSetting.
func IntInRangeOrZeroDisable(minVal, maxVal int64) SettingOption {
	return WithValidateInt(func(v int64) error {
		if v != 0 && (v < minVal || v > maxVal) {
			return errors.Errorf("expected value in range [%d, %d] or 0 to disable, got: %d", minVal, maxVal, v)
		}
		return nil
	})
}
