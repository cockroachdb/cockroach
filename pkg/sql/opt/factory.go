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

package opt

//go:generate optgen -out factory.og.go ifactory ops/scalar.opt ops/relational.opt ops/enforcer.opt

// GroupID identifies a memo group. Groups have numbers greater than 0; a
// GroupID of 0 indicates an invalid group.
type GroupID uint32

// PrivateID identifies custom private data used by a memo expression and
// stored by the memo. Privates have numbers greater than 0; a PrivateID of 0
// indicates an unknown private.
type PrivateID uint32

// ListID identifies a variable-sized list used by a memo expression and stored
// by the memo. The ID consists of an offset into the memo's lists slice, plus
// the number of elements in the list. Valid lists have offsets greater than 0;
// a ListID with offset 0 indicates an undefined list (probable indicator of a
// bug).
type ListID struct {
	Offset uint32
	Length uint32
}

// EmptyList is a list with zero elements. It begins at offset 1 because offset
// 0 is reserved to indicate an invalid, uninitialized list.
var EmptyList = ListID{Offset: 1, Length: 0}
