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
	"bytes"

	"github.com/lib/pq/oid"
)

// TTuple is the type of a DTuple.
type TTuple struct {
	Types  []T
	Labels []string
}

// String implements the fmt.Stringer interface.
func (t TTuple) String() string {
	var buf bytes.Buffer
	buf.WriteString("tuple")
	if t.Types != nil {
		buf.WriteByte('{')
		for i, typ := range t.Types {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(typ.String())
			if t.Labels != nil {
				buf.WriteString(" AS ")
				buf.WriteString(t.Labels[i])
			}
		}
		buf.WriteByte('}')
	}
	return buf.String()
}

// Identical implements the T interface.
func (t TTuple) Identical(other T) bool {
	u, ok := UnwrapType(other).(TTuple)
	if !ok {
		return false
	}
	if len(t.Types) != len(u.Types) {
		return false
	}
	for i, typ := range t.Types {
		if !typ.Identical(u.Types[i]) {
			return false
		}
	}
	return true
}

// Equivalent implements the T interface.
func (t TTuple) Equivalent(other T) bool {
	if Any.Identical(other) {
		return true
	}
	u, ok := UnwrapType(other).(TTuple)
	if !ok {
		return false
	}
	if len(t.Types) == 0 || len(u.Types) == 0 {
		// Tuples that aren't fully specified (have a nil subtype list) are always
		// equivalent to other tuples, to allow overloads to specify that they take
		// an arbitrary tuple type.
		return true
	}
	if len(t.Types) != len(u.Types) {
		return false
	}
	for i, typ := range t.Types {
		if !typ.Equivalent(u.Types[i]) {
			return false
		}
	}
	return true
}

// FamilyEqual implements the T interface.
func (TTuple) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TTuple)
	return ok
}

// Oid implements the T interface.
func (TTuple) Oid() oid.Oid { return oid.T_record }

// SQLName implements the T interface.
func (TTuple) SQLName() string { return "record" }

// IsAmbiguous implements the T interface.
func (t TTuple) IsAmbiguous() bool {
	for _, typ := range t.Types {
		if typ == nil || typ.IsAmbiguous() {
			return true
		}
	}
	return len(t.Types) == 0
}
