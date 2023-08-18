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

var _ numericSetting = &IntSetting{}

// Get retrieves the int value in the setting.
func (i *IntSetting) Get(sv *Values) int64 {
	return sv.container.getInt64(i.slot)
}

func (i *IntSetting) String(sv *Values) string {
	return EncodeInt(i.Get(sv))
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
	iv, err := i.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return EncodeInt(iv), nil
}

// DecodeValue decodes the value into an integer.
func (i *IntSetting) DecodeValue(value string) (int64, error) {
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
	sv.setInt64(ctx, i.slot, v)
	sv.setDefaultOverride(i.slot, v)
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
			if opt.validateDurationFn != nil ||
				opt.validateFloat64Fn != nil ||
				opt.validateStringFn != nil ||
				opt.validateProtoFn != nil {
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if fn := opt.validateInt64Fn; fn != nil {
				if err := fn(val); err != nil {
					return err
				}
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

// NonNegativeInt can be passed to RegisterIntSetting.
var NonNegativeInt SettingOption = WithValidateInt(nonNegativeIntInternal)

func nonNegativeIntInternal(v int64) error {
	if v < 0 {
		return errors.Errorf("cannot be set to a negative value: %d", v)
	}
	return nil
}

// NonNegativeIntWithMinimum can be passed to RegisterIntSetting.
func NonNegativeIntWithMinimum(minVal int64) SettingOption {
	return WithValidateInt(func(v int64) error {
		if err := nonNegativeIntInternal(v); err != nil {
			return err
		}
		if v < minVal {
			return errors.Errorf("cannot be set to a value lower than %d: %d", minVal, v)
		}
		return nil
	})
}

// NonNegativeIntWithMaximum returns a validation function that can be
// passed to RegisterIntSetting.
func NonNegativeIntWithMaximum(maxValue int64) SettingOption {
	return IntInRange(0, maxValue)
}

// IntInRange returns a validation function that checks the value
// is within the given bounds (inclusive).
func IntInRange(minVal, maxVal int64) SettingOption {
	return WithValidateInt(func(v int64) error {
		if v < minVal || v > maxVal {
			return errors.Errorf("expected value in range [%d, %d], got: %d", minVal, maxVal, v)
		}
		return nil
	})
}

// IntInRangeOrZeroDisable returns a validation function that checks the value
// is within the given bounds (inclusive) or is zero (disabled).
func IntInRangeOrZeroDisable(minVal, maxVal int64) SettingOption {
	return WithValidateInt(func(v int64) error {
		if v != 0 && (v < minVal || v > maxVal) {
			return errors.Errorf("expected value in range [%d, %d] or 0 to disable, got: %d", minVal, maxVal, v)
		}
		return nil
	})
}
