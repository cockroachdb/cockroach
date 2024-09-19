// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"context"
	"strconv"
)

// BoolSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "bool" is updated.
type BoolSetting struct {
	common
	defaultValue bool
	validateFn   func(*Values, bool) error
}

var _ internalSetting = &BoolSetting{}

// Get retrieves the bool value in the setting.
func (b *BoolSetting) Get(sv *Values) bool {
	return sv.getInt64(b.slot) != 0
}

func (b *BoolSetting) String(sv *Values) string {
	return EncodeBool(b.Get(sv))
}

// Encoded returns the encoded value of the current value of the setting.
func (b *BoolSetting) Encoded(sv *Values) string {
	return b.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (b *BoolSetting) EncodedDefault() string {
	return EncodeBool(b.defaultValue)
}

// DecodeToString decodes and renders an encoded value.
func (b *BoolSetting) DecodeToString(encoded string) (string, error) {
	bv, err := b.DecodeValue(encoded)
	if err != nil {
		return "", err
	}
	return EncodeBool(bv), nil
}

// DecodeValue decodes the value into a float.
func (b *BoolSetting) DecodeValue(encoded string) (bool, error) {
	return strconv.ParseBool(encoded)
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*BoolSetting) Typ() string {
	return "b"
}

// Default returns default value for setting.
func (b *BoolSetting) Default() bool {
	return b.defaultValue
}

// Defeat the linter.
var _ = (*BoolSetting).Default

// Override changes the setting without validation and also overrides the
// default value.
//
// For testing usage only.
func (b *BoolSetting) Override(ctx context.Context, sv *Values, v bool) {
	_ = b.set(ctx, sv, v)
	sv.setDefaultOverride(b.slot, v)
}

// Validate that a value conforms with the validation function.
func (b *BoolSetting) Validate(sv *Values, v bool) error {
	if b.validateFn != nil {
		if err := b.validateFn(sv, v); err != nil {
			return err
		}
	}
	return nil
}

func (b *BoolSetting) set(ctx context.Context, sv *Values, v bool) error {
	if err := b.Validate(sv, v); err != nil {
		return err
	}
	vInt := int64(0)
	if v {
		vInt = 1
	}
	sv.setInt64(ctx, b.slot, vInt)
	return nil
}

func (b *BoolSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(b.slot); val != nil {
		_ = b.set(ctx, sv, val.(bool))
		return
	}
	// intentionally do not panic on startup
	_ = b.set(ctx, sv, b.defaultValue)
}

// WithPublic sets public visibility and can be chained.
func (b *BoolSetting) WithPublic() *BoolSetting {
	b.SetVisibility(Public)
	return b
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(
	class Class, key, desc string, defaultValue bool, validateFns ...func(*Values, bool) error,
) *BoolSetting {
	validateFn := func(sv *Values, val bool) error {
		for _, fn := range validateFns {
			if err := fn(sv, val); err != nil {
				return err
			}
		}
		return nil
	}
	setting := &BoolSetting{defaultValue: defaultValue, validateFn: validateFn}
	register(class, key, desc, setting)
	return setting
}
