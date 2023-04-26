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
	"fmt"
	"strconv"
	"time"
)

// StringMap Until generics are ok to use, we'll provide this convenience map
// to allow for easy conversion of string values passed from the CLI to other types.
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

func (m StringMap) AsDuration(key string, dflt time.Duration) (time.Duration, error) {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt, nil
	}

	duration, err := time.ParseDuration(strVal)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q for key %q: %w", strVal, key, err)
	}
	return duration, nil
}

func (m StringMap) AsInt(key string, dflt int) (int, error) {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt, nil
	}

	intVal, err := strconv.Atoi(strVal)
	if err != nil {
		return 0, fmt.Errorf("invalid int %q for key %q: %w", strVal, key, err)
	}
	return intVal, nil
}

func (m StringMap) AsBoolean(key string, dflt bool) (bool, error) {
	strVal, ok := m.StringMap[key]
	if !ok {
		return dflt, nil
	}

	boolVal, err := strconv.ParseBool(strVal)
	if err != nil {
		return false, fmt.Errorf("invalid boolean %q for key %q: %w", strVal, key, err)
	}
	return boolVal, nil
}
