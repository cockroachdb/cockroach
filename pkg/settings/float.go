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

// WithPublic sets public visibility and can be chained.
func (f *FloatSetting) WithPublic() *FloatSetting {
	f.SetVisibility(Public)
	return f
}

// RegisterFloatSetting defines a new setting with type float.
func RegisterFloatSetting(
	class Class, key, desc string, defaultValue float64, validateFns ...func(float64) error,
) *FloatSetting {
	var validateFn func(float64) error
	if len(validateFns) > 0 {
		validateFn = func(v float64) error {
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
	setting := &FloatSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	register(class, key, desc, setting)
	return setting
}

// NonNegativeFloat can be passed to RegisterFloatSetting.
func NonNegativeFloat(v float64) error {
	if v < 0 {
		return errors.Errorf("cannot set to a negative value: %f", v)
	}
	return nil
}

// PositiveFloat can be passed to RegisterFloatSetting.
func PositiveFloat(v float64) error {
	if v <= 0 {
		return errors.Errorf("cannot set to a non-positive value: %f", v)
	}
	return nil
}
