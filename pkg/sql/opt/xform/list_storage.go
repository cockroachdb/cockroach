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

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
)

// listStorage stores lists of memo group ids. Each list is interned, which
// means that each unique list is stored at most once. If the same list is
// added twice to storage, the same storage is used, and the same list id is
// returned by the intern method.
//
// To use listStorage, first call the init method to initialize storage. Call
// the intern method to add lists to storage and get back a unique list id.
// Call the lookup method with an id to retrieve previously added lists.
//
// listStorage uses a prefix tree index to perform efficient interning. Each
// unique list prefix is added to the index, and maps to the offset in the
// lists slice that stores that sequence of group ids. If a list is interned,
// then future interns of that same list, or any prefix of that list, will
// find the existing list (or a prefix of it) in the index. For example,
// consider the following lists interned in this order:
//
//   [1 2]
//   [1]
//   [2 3]
//   [2 3 4]
//   [2 4]
//   [2 3]
//
// The resulting lists slice looks like this (0th item is always unused):
//   [0 1 2 2 3 4 2 4]
//
// The resulting prefix tree is equivalent to this:
//   []: EmptyList
//    ├── [1]: ListID{Offset: 1, Length: 1}
//    │    └── [2]: ListID{Offset: 1, Length: 2]
//    │
//    └── [2]: ListID{Offset: 3, Length: 1}
//         ├── [3]: ListID{Offset: 3, Length: 2}
//         │    └── [4]: ListID{Offset: 3, Length: 3}
//         │
//         └── [4]: ListID{Offset: 6, Length: 2}
//
// Future calls to intern would return these list ids:
//   [1]    : ListID{Offset: 1, Length: 1}
//   [1 2]  : ListID{Offset: 1, Length: 2}
//   [2]    : ListID{Offset: 3, Length: 1}
//   [2 3]  : ListID{Offset: 3, Length: 2}
//   [2 3 4]: ListID{Offset: 3, Length: 3}
//   [2 4]  : ListID{Offset: 6, Length: 2}
//
type listStorage struct {
	index map[listStorageKey]uint32
	lists []opt.GroupID
}

// init must be called before using other methods on listStorage.
func (ls *listStorage) init() {
	ls.index = make(map[listStorageKey]uint32)
	ls.lists = make([]opt.GroupID, 1)
}

// intern adds the given list to storage and returns an id that can later be
// used to retrieve the list by calling the lookup method. If the list has been
// previously added to storage, then intern always returns the same list id
// that was returned from the previous call. intern is an O(N) operation, where
// N is the length of the list.
func (ls *listStorage) intern(list []opt.GroupID) opt.ListID {
	// Start with empty prefix.
	prefix := opt.EmptyList

	for i, item := range list {
		// Is there an existing list for the prefix + next item?
		key := listStorageKey{prefix: prefix, group: item}
		existing := ls.index[key]
		if existing == 0 {
			// No, so append the list now.
			return ls.appendList(prefix, list)
		}

		// Yes, so set the new prefix and keep looping.
		prefix = opt.ListID{Offset: existing, Length: uint32(i + 1)}
	}

	// Found an existing list, so return it.
	return prefix
}

// lookup returns a list that was previously interned by listStorage. Do not
// change the elements of the returned list or append to it. lookup is an O(1)
// operation.
func (ls *listStorage) lookup(id opt.ListID) []opt.GroupID {
	return ls.lists[id.Offset:id.Offset+id.Length]
}

func (ls *listStorage) appendList(prefix opt.ListID, list []opt.GroupID) opt.ListID {
	var offset uint32

	// If prefix is the last list in the slice, then optimize by appending only
	// the suffix.
	if prefix.Offset + prefix.Length == uint32(len(ls.lists)) {
		offset = prefix.Offset
		ls.lists = append(ls.lists, list[prefix.Length:]...)
	} else {
		offset = uint32(len(ls.lists))
		ls.lists = append(ls.lists, list...)
	}

	// Add rest of list to the index (prefix is already in the index).
	for i := prefix.Length; i < uint32(len(list)); i++ {
		key := listStorageKey{prefix: prefix, group: list[i]}
		ls.index[key] = offset
		prefix = opt.ListID{Offset: offset, Length: i + 1}
	}

	return opt.ListID{Offset: offset, Length: uint32(len(list))}
}

type listStorageKey struct {
	prefix opt.ListID
	group  opt.GroupID
}
