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

// StringMap Until generics are ok to use, we'll provide this convenience map
// to allow for easy conversion of string values passed from the CLI to other types.
//
// Current implementation is opinionated and will return the default value if
// the key is not present, or if the value is not a valid representation of the
// requested type.
//
// TODO: log errors, signatures which return errors
type StringMap struct {
	StringMap map[string]string
}

func (m StringMap) AsString(key string, dflt string) string {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt
	}
	return strVal
}

func (m StringMap) AsDuration(key string, dflt time.Duration) time.Duration {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt
	}

	duration, err := time.ParseDuration(strVal)
	if err != nil {
		return dflt
	}
	return duration
}

func (m StringMap) AsInt(key string, dflt int) int {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt
	}

	intVal, err := strconv.Atoi(strVal)
	if err != nil {
		return dflt
	}
	return intVal
}

func (m StringMap) AsBoolean(key string, dflt bool) bool {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt
	}

	boolVal, err := strconv.ParseBool(strVal)
	if err != nil {
		return dflt
	}
	return boolVal
}
