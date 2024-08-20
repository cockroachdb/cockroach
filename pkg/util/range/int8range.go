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

func ParseInt8Range(rangeStr string) (int8, int8, error) {
	startIncrement := 0
	if rangeStr[0] == '(' {
		startIncrement++
	}

	endIncrement := 0
	if rangeStr[len(rangeStr)-1] == ']' {
		endIncrement++
	}
	var err error
	startVal, endVal := math.MinInt8, math.MaxInt8
	values := strings.Split(rangeStr[1:len(rangeStr)-1], ",")
	if len(values) != 2 {
		err := fmt.Errorf("invalid range values")
		return int8(startVal), int8(endVal), err
	}

	if values[0] != "" {
		startVal, err = strconv.Atoi(strings.TrimSpace(values[0]))
		startVal += startIncrement
		if err != nil {
			return int8(startVal), int8(endVal), err
		}
	}

	if values[1] != "" {
		endVal, err = strconv.Atoi(strings.TrimSpace(values[1]))
		endVal += endIncrement
		if err != nil {
			return int8(startVal), int8(endVal), err
		}
	}

	if endVal < startVal {
		return -1, -1, fmt.Errorf("invalid range values")
	}
	return int8(startVal), int8(endVal), nil
}
