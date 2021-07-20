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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// EnumSetting is a StringSetting that restricts the values to be one of the `enumValues`
type EnumSetting struct {
	IntSetting
	enumValues map[int64]string
}

var _ extendedSetting = &EnumSetting{}

// Typ returns the short (1 char) string denoting the type of setting.
func (e *EnumSetting) Typ() string {
	return "e"
}

// String returns the enum's string value.
func (e *EnumSetting) String(sv *Values) string {
	enumID := e.Get(sv)
	if str, ok := e.enumValues[enumID]; ok {
		return str
	}
	return fmt.Sprintf("unknown(%d)", enumID)
}

// ParseEnum returns the enum value, and a boolean that indicates if it was parseable.
func (e *EnumSetting) ParseEnum(raw string) (int64, bool) {
	rawLower := strings.ToLower(raw)
	for k, v := range e.enumValues {
		if v == rawLower {
			return k, true
		}
	}
	// Attempt to parse the string as an integer since it isn't a valid enum string.
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	_, ok := e.enumValues[v]
	return v, ok
}

// GetAvailableValuesAsHint returns the possible enum settings as a string that
// can be provided as an error hint to a user.
func (e *EnumSetting) GetAvailableValuesAsHint() string {
	// First stabilize output by sorting by key.
	valIdxs := make([]int, 0, len(e.enumValues))
	for i := range e.enumValues {
		valIdxs = append(valIdxs, int(i))
	}
	sort.Ints(valIdxs)

	// Now use those indices
	vals := make([]string, 0, len(e.enumValues))
	for _, enumIdx := range valIdxs {
		vals = append(vals, fmt.Sprintf("%d: %s", enumIdx, e.enumValues[int64(enumIdx)]))
	}
	return "Available values: " + strings.Join(vals, ", ")
}

func (e *EnumSetting) set(ctx context.Context, sv *Values, k int64) error {
	if _, ok := e.enumValues[k]; !ok {
		return errors.Errorf("unrecognized value %d", k)
	}
	return e.IntSetting.set(ctx, sv, k)
}

func enumValuesToDesc(enumValues map[int64]string) string {
	var buffer bytes.Buffer
	values := make([]int64, 0, len(enumValues))
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

// WithPublic sets public visibility and can be chained.
func (e *EnumSetting) WithPublic() *EnumSetting {
	e.SetVisibility(Public)
	return e
}

// WithSystemOnly indicates system-usage only and can be chained.
func (e *EnumSetting) WithSystemOnly() *EnumSetting {
	e.common.systemOnly = true
	return e
}

// Defeat the linter.
var _ = (*EnumSetting).WithSystemOnly

// RegisterEnumSetting defines a new setting with type int.
func RegisterEnumSetting(
	key, desc string, defaultValue string, enumValues map[int64]string,
) *EnumSetting {
	enumValuesLower := make(map[int64]string)
	var i int64
	var found bool
	for k, v := range enumValues {
		enumValuesLower[k] = strings.ToLower(v)
		if v == defaultValue {
			i = k
			found = true
		}
	}

	if !found {
		panic(fmt.Sprintf("enum registered with default value %s not in map %s", defaultValue, enumValuesToDesc(enumValuesLower)))
	}

	setting := &EnumSetting{
		IntSetting: IntSetting{defaultValue: i},
		enumValues: enumValuesLower,
	}

	register(key, fmt.Sprintf("%s %s", desc, enumValuesToDesc(enumValues)), setting)
	return setting
}
