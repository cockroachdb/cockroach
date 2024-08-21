// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package _range

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func ParseInt8Range(rangeStr string) (int64, int64, error) {
	if strings.ToLower(rangeStr) == "empty" {
		return math.MinInt64, math.MinInt64, nil
	}

	var startIncrement int64
	var startInf, endInf bool
	if rangeStr[0] == '(' {
		startIncrement++
	}

	var endIncrement int64
	if rangeStr[len(rangeStr)-1] == ']' {
		endIncrement++
	}
	var err error
	var startVal, endVal int64 = math.MinInt64, math.MaxInt64
	values := strings.Split(rangeStr[1:len(rangeStr)-1], ",")
	if len(values) != 2 {
		err := fmt.Errorf("invalid range values")
		return startVal, endVal, err
	} else {
		startInf = true
	}

	if values[0] != "" {
		startVal, err = strconv.ParseInt(strings.TrimSpace(values[0]), 10, 64)
		startVal += startIncrement
		if err != nil {
			return startVal, endVal, err
		}
	} else {
		endInf = true
	}

	if values[1] != "" {
		endVal, err = strconv.ParseInt(strings.TrimSpace(values[1]), 10, 64)
		endVal += endIncrement
		if err != nil {
			return startVal, endVal, err
		}
	}

	if !startInf && !endInf && endVal < startVal-1 {
		return -1, -1, fmt.Errorf("invalid range values: (%d, %d)", startVal-1, endVal)
	}
	return startVal, endVal, nil
}
