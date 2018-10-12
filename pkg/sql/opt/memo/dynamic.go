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

package memo

import "github.com/cockroachdb/cockroach/pkg/sql/opt"

// DynamicID is used when dynamically creating expressions using the MakeXXXExpr
// methods, as well as the normalizing factory's DynamicConstruct method. Each
// operand, whether it be a group, a private, or a list, is first converted to a
// DynamicID and then passed as one of the DynamicOperands.
type DynamicID uint64

// MakeDynamicListID constructs a DynamicID from a ListID.
func MakeDynamicListID(id ListID) DynamicID {
	return (DynamicID(id.Offset) << 32) | DynamicID(id.Length)
}

// ListID converts the DynamicID to a ListID.
func (id DynamicID) ListID() ListID {
	return ListID{Offset: uint32(id >> 32), Length: uint32(id & 0xffffffff)}
}

// DynamicOperands is the list of operands passed to the expression creation
// method in order to dynamically create an operator.
type DynamicOperands [opt.MaxOperands]DynamicID
