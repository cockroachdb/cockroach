// Copyright 2018 The Cockroach Authors.
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

// Package intsize exists to prevent dependency cycles.  This is
// transitional code which can be removed as part of
// XXX Link to cleanup issue.
package intsize

import (
	"fmt"
	"strings"
)

// IntSize is an enum that represents the size, in bytes, of an INT.
// This exists to allow us to transition from INT := INT8 in
// version <= 2.2 to INT := INT4.
type IntSize uint64

const (
	// Unknown INT size; this should only be seen when getting SessionData
	// from an older node version.  Should never be seen once all nodes
	// have reached 2.2.
	Unknown IntSize = 0
	// Four byte INT.
	Four IntSize = 4
	// Eight byte INT.
	Eight IntSize = 8
)

// FromString can be used to parse user-provided input to
func FromString(val string) (_ IntSize, ok bool) {
	switch strings.ToUpper(val) {
	case "INT4":
		return Four, true
	case "INT8":
		return Eight, true
	default:
		return 0, false
	}
}

// FromWidth is the inverse of IntSize.Width().
func FromWidth(width int) (_ IntSize, ok bool) {
	switch width {
	case 32:
		return Four, true
	case 64:
		return Eight, true
	default:
		return 0, false
	}
}

func (s IntSize) String() string {
	switch s {
	// TODO(bob): Version 2.3: Change the default to INT4.
	case Unknown, Eight:
		return "INT8"
	case Four:
		return "INT4"
	default:
		panic(fmt.Sprintf("unknown IntSize(%d)", s))
	}
}

// Width returns the number of bits that the given size represents.
func (s IntSize) Width() int {
	switch s {
	// TODO(bob): Version 2.3: Change the default to INT4.
	case Unknown, Eight:
		return 64
	case Four:
		return 32
	default:
		panic(fmt.Sprintf("unknown IntSize(%d)", s))
	}
}
