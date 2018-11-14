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

// IntSize is an enum that represents the size, in bits, of an INT.
type IntSize int

const (
	// Unknown INT size; this should only be seen when getting SessionData
	// from an older node version.  Should never be seen once all nodes
	// have reached 2.2.
	Unknown IntSize = 0
	// INT2 is a two byte INT.
	INT2 IntSize = 8 << iota
	// INT4 is a four byte INT.
	INT4
	// INT8 is an eight byte INT.
	INT8
)

// FromString can be used to parse user-provided input.
func FromString(val string) (_ IntSize, ok bool) {
	switch strings.ToUpper(val) {
	case "INT2":
		return INT2, true
	case "INT4":
		return INT4, true
	case "INT8":
		return INT8, true
	default:
		return 0, false
	}
}

// FromWidth is the inverse of IntSize.Width().
func FromWidth(width int) (_ IntSize, ok bool) {
	if width == 16 || width == 32 || width == 64 {
		return IntSize(width), true
	}
	return 0, false
}

func (s IntSize) String() string {
	switch s {
	// TODO(bob): Version 2.3: Change the default to INT4.
	case Unknown, INT8:
		return "INT8"
	case INT4:
		return "INT4"
	case INT2:
		return "INT2"
	default:
		return fmt.Sprintf("unknown IntSize(%d)", s)
	}
}

// Width returns the number of bits that the given size represents.
func (s IntSize) Width() int {
	if s == Unknown {
		// TODO(bob): Version 2.3: Change the default to 32.
		return 64
	}
	return int(s)
}
