// Copyright 2015 The Cockroach Authors.
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

package types

import (
	"fmt"

	"github.com/lib/pq/oid"
)

// TCollatedString is the type of strings with a locale.
type TCollatedString struct {
	Locale string
	_      [0][]byte // Prevents use of the == operator.
}

// String implements the fmt.Stringer interface.
func (t TCollatedString) String() string { return fmt.Sprintf("collatedstring{%s}", t.Locale) }

// Identical implements the T interface.
func (t TCollatedString) Identical(other T) bool {
	u, ok := UnwrapType(other).(TCollatedString)
	return ok && (t.Locale == "" || u.Locale == "" || t.Locale == u.Locale)
}

// Equivalent implements the T interface.
func (t TCollatedString) Equivalent(other T) bool {
	return t.Identical(other) || Any.Identical(other)
}

// FamilyEqual implements the T interface.
func (TCollatedString) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TCollatedString)
	return ok
}

// Oid implements the T interface.
func (TCollatedString) Oid() oid.Oid { return oid.T_text }

// SQLName implements the T interface.
func (TCollatedString) SQLName() string { return "text" }

// IsAmbiguous implements the T interface.
func (t TCollatedString) IsAmbiguous() bool {
	return t.Locale == ""
}
