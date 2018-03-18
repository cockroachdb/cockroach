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

import (
	"bytes"
	"testing"
)

func TestListStorage(t *testing.T) {
	var ls listStorage
	ls.init()

	catID := ls.intern(stringToGroups("cat"))
	if catID != (ListID{Offset: 1, Length: 3}) {
		t.Fatalf("unexpected id: %v", catID)
	}

	// Should return sublist since "c" is prefix of "cat".
	cID := ls.intern(stringToGroups("c"))
	if cID != (ListID{Offset: 1, Length: 1}) {
		t.Fatalf("unexpected id: %v", cID)
	}

	// Should append to existing "cat" list, since it's last in the slice.
	catalogID := ls.intern(stringToGroups("catalog"))
	if catalogID != (ListID{Offset: 1, Length: 7}) {
		t.Fatalf("unexpected id: %v", catalogID)
	}

	// Should create new list.
	dogID := ls.intern(stringToGroups("dog"))
	if dogID != (ListID{Offset: 8, Length: 3}) {
		t.Fatalf("unexpected id: %v", dogID)
	}

	// Should create new list, since "catalog" is no longer last in the slice.
	catalogingID := ls.intern(stringToGroups("cataloging"))
	if catalogingID != (ListID{Offset: 11, Length: 10}) {
		t.Fatalf("unexpected id: %v", catalogingID)
	}

	// Should create new list even though it's a substring of existing list.
	logID := ls.intern(stringToGroups("log"))
	if logID != (ListID{Offset: 21, Length: 3}) {
		t.Fatalf("unexpected id: %v", logID)
	}

	if s := groupsToString(ls.lookup(catID)); s != "cat" {
		t.Fatalf("unexpected lookup: %v", s)
	}

	if s := groupsToString(ls.lookup(dogID)); s != "dog" {
		t.Fatalf("unexpected lookup: %v", s)
	}

	if s := groupsToString(ls.lookup(catalogID)); s != "catalog" {
		t.Fatalf("unexpected lookup: %v", s)
	}
}

func stringToGroups(s string) []GroupID {
	groups := make([]GroupID, len(s))
	for i, rune := range s {
		groups[i] = GroupID(rune)
	}
	return groups
}

func groupsToString(groups []GroupID) string {
	var buf bytes.Buffer
	for _, group := range groups {
		buf.WriteRune(rune(group))
	}
	return buf.String()
}
