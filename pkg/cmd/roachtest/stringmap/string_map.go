// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stringmap

import (
	"strconv"
	"time"
)

// StringMap is a wrapper around a map[string]string which provides typed access to
// the string values within.
// Current implementation is opinionated and will return the default value if
// the key is not present, or if the value is not a valid representation of the
// requested type.
// TODO: log errors, signatures which return errors
type StringMap struct {
	M map[string]string
}

func Get[T string | int | bool | time.Duration](m map[string]string, key string, dflt T) T {
	var ret any
	var dfl any = dflt
	switch any(dflt).(type) {
	case string:
		ret = GetAsString(m, key, dfl.(string))
	case int:
		ret = GetAsInt(m, key, dfl.(int))
	case bool:
		ret = GetAsBoolean(m, key, dfl.(bool))
	case time.Duration:
		ret = GetAsDuration(m, key, dfl.(time.Duration))
	default:
		panic("unsupported type")
	}

	return ret.(T)
}

func GetAsString(m map[string]string, key string, dflt string) string {
	strVal, ok := m[key]
	if !ok {
		return dflt
	}
	return strVal
}

func GetAsInt(m map[string]string, key string, dflt int) int {
	strVal, ok := m[key]
	if !ok {
		return dflt
	}

	intVal, err := strconv.Atoi(strVal)
	if err != nil {
		return dflt
	}
	return intVal
}

func GetAsBoolean(m map[string]string, key string, dflt bool) bool {
	strVal, ok := m[key]
	if !ok {
		return dflt
	}

	boolVal, err := strconv.ParseBool(strVal)
	if err != nil {
		return dflt
	}
	return boolVal
}

func GetAsDuration(m map[string]string, key string, dflt time.Duration) time.Duration {
	strVal, ok := m[key]
	if !ok {
		return dflt
	}

	duration, err := time.ParseDuration(strVal)
	if err != nil {
		return dflt
	}
	return duration
}

func (sm StringMap) String(key string, dflt string) string {
	return GetAsString(sm.M, key, dflt)
}

func (sm StringMap) Duration(key string, dflt time.Duration) time.Duration {
	return GetAsDuration(sm.M, key, dflt)
}

func (sm StringMap) Int(key string, dflt int) int {
	return GetAsInt(sm.M, key, dflt)
}

func (sm StringMap) Boolean(key string, dflt bool) bool {
	return GetAsBoolean(sm.M, key, dflt)
}
