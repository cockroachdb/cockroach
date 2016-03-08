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

package util

import (
	"fmt"

	"github.com/dustin/go-humanize"
)

// IBytes is an int64 version of go-humanize's IBytes.
func IBytes(value int64) string {
	if value < 0 {
		return fmt.Sprintf("-%s", humanize.IBytes(uint64(0-value)))
	}
	return humanize.IBytes(uint64(value))
}

// ParseBytes is an int64 version of go-humanize's ParseBytes.
func ParseBytes(s string) (int64, error) {
	value, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, err
	}
	valueInt64 := int64(value)
	if value < 0 {
		return 0, fmt.Errorf("unsupported disk size %s", s)
	}
	return valueInt64, nil
}
