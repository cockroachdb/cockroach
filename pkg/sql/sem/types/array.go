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
	"github.com/lib/pq/oid"
)

// TArray is the type of a DArray.
type TArray struct{ Typ T }

func (a TArray) String() string { return a.Typ.String() + "[]" }

// Equivalent implements the T interface.
func (a TArray) Equivalent(other T) bool {
	if other == Any {
		return true
	}
	if u, ok := UnwrapType(other).(TArray); ok {
		return a.Typ.Equivalent(u.Typ)
	}
	return false
}

// FamilyEqual implements the T interface.
func (TArray) FamilyEqual(other T) bool {
	_, ok := UnwrapType(other).(TArray)
	return ok
}

const noArrayType = 0

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	for _, v := range oidToArrayOid {
		ArrayOids[v] = struct{}{}
	}
}

// Oid implements the T interface.
func (a TArray) Oid() oid.Oid {
	if o, ok := oidToArrayOid[a.Typ.Oid()]; ok {
		return o
	}
	return noArrayType
}

// SQLName implements the T interface.
func (a TArray) SQLName() string {
	return a.Typ.SQLName() + "[]"
}

// IsAmbiguous implements the T interface.
func (a TArray) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}
