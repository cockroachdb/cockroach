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

// TPlaceholder is the type of a placeholder.
type TPlaceholder struct {
	Name string
	_    [0][]byte // Prevents use of the == operator.
}

// String implements the fmt.Stringer interface.
func (t TPlaceholder) String() string { return fmt.Sprintf("placeholder{%s}", t.Name) }

// Identical implements the T interface.
func (t TPlaceholder) Identical(other T) bool {
	u, ok := UnwrapType(other).(TPlaceholder)
	return ok && t.Name == u.Name
}

// Equivalent implements the T interface.
func (t TPlaceholder) Equivalent(other T) bool {
	return other.IsAmbiguous() || Any.Identical(other) || t.Identical(other)
}

// FamilyEqual implements the T interface.
func (TPlaceholder) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TPlaceholder)
	return ok
}

// Oid implements the T interface.
func (TPlaceholder) Oid() oid.Oid { panic("TPlaceholder.Oid() is undefined") }

// SQLName implements the T interface.
func (TPlaceholder) SQLName() string { panic("TPlaceholder.SQLName() is undefined") }

// IsAmbiguous implements the T interface.
func (TPlaceholder) IsAmbiguous() bool { panic("TPlaceholder.IsAmbiguous() is undefined") }
