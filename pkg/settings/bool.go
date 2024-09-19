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

// DefaultString returns the default value for the setting as a string.
func (b *BoolSetting) DefaultString() string {
	return EncodeBool(b.defaultValue)
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
	sv.setValueOrigin(ctx, b.slot, OriginOverride)
	b.setOnValues(ctx, sv, v)
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
	b.setOnValues(ctx, sv, v)
	return nil
}

func (b *BoolSetting) setOnValues(ctx context.Context, sv *Values, v bool) {
	vInt := int64(0)
	if v {
		vInt = 1
	}
	sv.setInt64(ctx, b.slot, vInt)
}

func (b *BoolSetting) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	v, err := strconv.ParseBool(encoded)
	if err != nil {
		return err
	}
	return b.set(ctx, sv, v)
}

func (b *BoolSetting) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	v, err := strconv.ParseBool(encoded)
	if err != nil {
		return err
	}
	sv.setDefaultOverride(b.slot, v)
	return nil
}

func (b *BoolSetting) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(b.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		_ = b.set(ctx, sv, val.(bool))
		return
	}
	if err := b.set(ctx, sv, b.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterBoolSetting defines a new setting with type bool.
func RegisterBoolSetting(
	class Class, key InternalKey, desc string, defaultValue bool, opts ...SettingOption,
) *BoolSetting {
	validateFn := func(sv *Values, val bool) error {
		for _, opt := range opts {
			switch {
			case opt.commonOpt != nil:
				continue
			case opt.validateBoolFn != nil:
			default:
				panic(errors.AssertionFailedf("wrong validator type"))
			}
			if err := opt.validateBoolFn(sv, val); err != nil {
				return err
			}
		}
		return nil
	}
	// what to put here?
	if err := validateFn(&Values{}, defaultValue); err != nil {
		panic(errors.Wrap(err, "invalid default"))
	}
	setting := &BoolSetting{defaultValue: defaultValue, validateFn: validateFn}
	register(class, key, desc, setting)
	setting.apply(opts)
	return setting
}
