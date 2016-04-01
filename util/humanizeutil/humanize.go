// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package humanizeutil

import (
	"fmt"
	"math"

	"github.com/dustin/go-humanize"
)

// IBytes is an int64 version of go-humanize's IBytes.
func IBytes(value int64) string {
	if value < 0 {
		return fmt.Sprintf("-%s", humanize.IBytes(uint64(-value)))
	}
	return humanize.IBytes(uint64(value))
}

// ParseBytes is an int64 version of go-humanize's ParseBytes.
func ParseBytes(s string) (int64, error) {
	if len(s) == 0 {
		return 0, fmt.Errorf("parsing \"\": invalid syntax")
	}
	var startIndex int
	var negative bool
	if s[0] == '-' {
		negative = true
		startIndex = 1
	}
	value, err := humanize.ParseBytes(s[startIndex:])
	if err != nil {
		return 0, err
	}
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("too large: %s", s)
	}
	if negative {
		return -int64(value), nil
	}
	return int64(value), nil
}
