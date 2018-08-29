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
// The entire prefix tree is stored in a single Go map, using 8 byte keys and
// values, which are highly optimized in Go.
//
type listStorage struct {
	// index maps the path of each node in the prefix tree to an Offset value
	// (biased index of the list in the lists slice). See listStorageKey comment
	// for more details about node path format.
	index map[listStorageKey]listStorageVal
	lists []GroupID

	// unique is an increasing counter that's used to generate unique ids for
	// lists in the prefix tree.
	unique prefixID
}

type prefixID uint32

const minStorageCap = 16

// listStorageKey is the path to a node in the prefix tree. The path is a
// (prefix, item) pair, where prefix uniquely identifies the list of edges from
// root to parent node, and item is the last edge from parent to child. This
// representation fits in 8 bytes, and so allows Go to use mapaccess1_fast64
// for fast lookups.
type listStorageKey struct {
	prefix prefixID
	item   GroupID
}

// listStorageVal stores the offset of this node's list in the "lists" slice.
// The offset is 1-based, so to index into the slice, first subtract one from
// this value (0 is reserved to mean "undefined" list). listStorageValue also
// stores a unique prefix identifier, which can be used to look up lists that
// are one greater in length, and that have this list as a prefix.
type listStorageVal struct {
	prefix prefixID
	offset uint32
}

// intern adds the given list to storage and returns an id that can later be
// used to retrieve the list by calling the lookup method. If the list has been
// previously added to storage, then intern always returns the same list id
// that was returned from the previous call. intern is an O(N) operation, where
// N is the length of the list.
func (ls *listStorage) intern(list []GroupID) ListID {
	if len(list) == 0 {
		return EmptyList
	}

	if ls.index == nil {
		ls.index = make(map[listStorageKey]listStorageVal)
	}

	var val listStorageVal
	for i, item := range list {
		// Is there an existing list for the prefix + next item?
		key := listStorageKey{prefix: val.prefix, item: item}
		existing := ls.index[key]
		if existing.offset == 0 {
			// No, so append the list now.
			return ls.appendList(val, i, list)
		}

		// Yes, so keep looping.
		val = existing
	}

	// Found an existing list, so return it.
	return ListID{Offset: val.offset, Length: uint32(len(list))}
}

// lookup returns a list that was previously interned by listStorage. Do not
// change the elements of the returned list or append to it. lookup is an O(1)
// operation.
func (ls *listStorage) lookup(id ListID) []GroupID {
	// Convert the offset from being 1-based to being 0-based.
	return ls.lists[id.Offset-1 : id.Offset+id.Length-1]
}

func (ls *listStorage) appendList(val listStorageVal, prefixLen int, list []GroupID) ListID {
	var offset uint32

	// If prefix is the last list in the slice, then optimize by appending only
	// the suffix.
	if int(val.offset)+prefixLen-1 == len(ls.lists) {
		offset = val.offset
		ls.lists = append(ls.lists, list[prefixLen:]...)
	} else {
		offset = uint32(len(ls.lists)) + 1
		ls.lists = append(ls.lists, list...)
	}

	// Add rest of list to the index (prefix is already in the index).
	for i := prefixLen; i < len(list); i++ {
		key := listStorageKey{prefix: val.prefix, item: list[i]}
		ls.unique++
		val.prefix = ls.unique
		ls.index[key] = listStorageVal{prefix: val.prefix, offset: offset}
	}

	return ListID{Offset: offset, Length: uint32(len(list))}
}
