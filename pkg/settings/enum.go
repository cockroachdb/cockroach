// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/exp/constraints"
)

// EnumSetting is a StringSetting that restricts the values to be one of the `enumValues`
type EnumSetting[T constraints.Integer] struct {
	common
	defaultValue T
	// enumValues maps each valid value of T to a lowercase string.
	enumValues map[T]string
}

// AnyEnumSetting is an interface that is used when the specific enum type T is
// not known.
type AnyEnumSetting interface {
	internalSetting

	// ParseEnum parses a string that is either a string in enumValues or an
	// integer and returns the enum value as an int64 (and a boolean that
	// indicates if it was parseable).
	ParseEnum(raw string) (int64, bool)

	// GetAvailableValuesAsHint returns the possible enum settings as a string that
	// can be provided as an error hint to a user.
	GetAvailableValuesAsHint() string
}

var _ AnyEnumSetting = &EnumSetting[int]{}

// Typ returns the short (1 char) string denoting the type of setting.
func (e *EnumSetting[T]) Typ() string {
	return "e"
}

// Get retrieves the int value in the setting.
func (e *EnumSetting[T]) Get(sv *Values) T {
	return T(sv.container.getInt64(e.slot))
}

// Override changes the setting without validation and also overrides the
// default value.
func (e *EnumSetting[T]) Override(ctx context.Context, sv *Values, v T) {
	sv.setValueOrigin(ctx, e.slot, OriginOverride)
	sv.setInt64(ctx, e.slot, int64(v))
	sv.setDefaultOverride(e.slot, int64(v))
}

// String returns the enum's string value.
func (e *EnumSetting[T]) String(sv *Values) string {
	enumID := e.Get(sv)
	if str, ok := e.enumValues[enumID]; ok {
		return str
	}
	return fmt.Sprintf("unknown(%d)", enumID)
}

// DefaultString returns the default value for the setting as a string.
func (e *EnumSetting[T]) DefaultString() string {
	return e.enumValues[e.defaultValue]
}

func (e *EnumSetting[T]) Encoded(sv *Values) string {
	return EncodeInt(int64(e.Get(sv)))
}

func (e *EnumSetting[T]) EncodedDefault() string {
	return EncodeInt(int64(e.defaultValue))
}

func (e *EnumSetting[T]) setToDefault(ctx context.Context, sv *Values) {
	// See if the default value was overridden.
	if val := sv.getDefaultOverride(e.slot); val != nil {
		// As per the semantics of override, these values don't go through
		// validation.
		sv.setInt64(ctx, e.slot, val.(int64))
		return
	}
	sv.setInt64(ctx, e.slot, int64(e.defaultValue))
}

func (e *EnumSetting[T]) decodeAndSet(ctx context.Context, sv *Values, encoded string) error {
	v, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return err
	}
	sv.setInt64(ctx, e.slot, v)
	return nil
}

func (e *EnumSetting[T]) decodeAndSetDefaultOverride(
	ctx context.Context, sv *Values, encoded string,
) error {
	v, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return err
	}
	sv.setDefaultOverride(e.slot, v)
	return nil
}

// DecodeToString decodes and renders an encoded value.
func (e *EnumSetting[T]) DecodeToString(encoded string) (string, error) {
	v, err := strconv.ParseInt(encoded, 10, 64)
	if err != nil {
		return "", err
	}
	if str, ok := e.enumValues[T(v)]; ok {
		return str, nil
	}
	return encoded, nil
}

// ParseEnum parses a string that is either a string in enumValues or an integer
// and returns the enum value as an int64 (and a boolean that indicates if it
// was parseable).
func (e *EnumSetting[T]) ParseEnum(raw string) (int64, bool) {
	rawLower := strings.ToLower(raw)
	for k, v := range e.enumValues {
		if v == rawLower {
			return int64(k), true
		}
	}
	// Attempt to parse the string as an integer since it isn't a valid enum string.
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	_, ok := e.enumValues[T(v)]
	return v, ok
}

// GetAvailableValuesAsHint returns the possible enum settings as a string that
// can be provided as an error hint to a user.
func (e *EnumSetting[T]) GetAvailableValuesAsHint() string {
	// First stabilize output by sorting by key.
	valIdxs := make([]int, 0, len(e.enumValues))
	for i := range e.enumValues {
		valIdxs = append(valIdxs, int(i))
	}
	sort.Ints(valIdxs)

	// Now use those indices
	vals := make([]string, 0, len(e.enumValues))
	for _, enumIdx := range valIdxs {
		vals = append(vals, fmt.Sprintf("%d: %s", enumIdx, e.enumValues[T(enumIdx)]))
	}
	return "Available values: " + strings.Join(vals, ", ")
}

// GetAvailableValues returns the possible enum settings as a string
// slice.
func (e *EnumSetting[T]) GetAvailableValues() []string {
	// First stabilize output by sorting by key.
	valIdxs := make([]int, 0, len(e.enumValues))
	for i := range e.enumValues {
		valIdxs = append(valIdxs, int(i))
	}
	sort.Ints(valIdxs)

	// Now use those indices
	vals := make([]string, 0, len(e.enumValues))
	for _, enumIdx := range valIdxs {
		vals = append(vals, e.enumValues[T(enumIdx)])
	}
	return vals
}

func enumValuesToDesc[T constraints.Integer](enumValues map[T]string) string {
	var buffer bytes.Buffer
	values := make([]T, 0, len(enumValues))
	for k := range enumValues {
		values = append(values, k)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

	buffer.WriteString("[")
	for i, k := range values {
		if i > 0 {
			buffer.WriteString(", ")
		}
		fmt.Fprintf(&buffer, "%s = %d", strings.ToLower(enumValues[k]), k)
	}
	buffer.WriteString("]")
	return buffer.String()
}

// RegisterEnumSetting defines a new setting with type int.
func RegisterEnumSetting[T constraints.Integer](
	class Class,
	key InternalKey,
	desc string,
	defaultValue string,
	enumValues map[T]string,
	opts ...SettingOption,
) *EnumSetting[T] {
	enumValuesLower := make(map[T]string, len(enumValues))
	for k, v := range enumValues {
		enumValuesLower[k] = strings.ToLower(v)
	}

	defaultVal := func() T {
		for k, v := range enumValues {
			if v == defaultValue {
				return k
			}
		}
		panic(fmt.Sprintf("enum registered with default value %s not in map %s", defaultValue, enumValuesToDesc(enumValuesLower)))
	}()

	setting := &EnumSetting[T]{
		defaultValue: defaultVal,
		enumValues:   enumValuesLower,
	}

	register(class, key, fmt.Sprintf("%s %s", desc, enumValuesToDesc(enumValues)), setting)
	setting.apply(opts)
	return setting
}
