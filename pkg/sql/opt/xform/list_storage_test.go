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
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
)

func TestListStorage(t *testing.T) {
	var ls listStorage
	ls.init()

	catId := ls.intern(stringToGroups("cat"))
	if catId != (opt.ListID{Offset: 1, Length: 3}) {
		t.Fatalf("unexpected id: %v", catId)
	}

	// Should return sublist since "c" is prefix of "cat".
	cId := ls.intern(stringToGroups("c"))
	if cId != (opt.ListID{Offset: 1, Length: 1}) {
		t.Fatalf("unexpected id: %v", cId)
	}

	// Should append to existing "cat" list, since it's last in the slice.
	catalogId := ls.intern(stringToGroups("catalog"))
	if catalogId != (opt.ListID{Offset: 1, Length: 7}) {
		t.Fatalf("unexpected id: %v", catalogId)
	}

	// Should create new list.
	dogId := ls.intern(stringToGroups("dog"))
	if dogId != (opt.ListID{Offset: 8, Length: 3}) {
		t.Fatalf("unexpected id: %v", dogId)
	}

	// Should create new list, since "catalog" is no longer last in the slice.
	catalogueId := ls.intern(stringToGroups("catalogue"))
	if catalogueId != (opt.ListID{Offset: 11, Length: 9}) {
		t.Fatalf("unexpected id: %v", catalogueId)
	}

	// Should create new list even though it's a substring of existing list.
	logId := ls.intern(stringToGroups("log"))
	if logId != (opt.ListID{Offset: 20, Length: 3}) {
		t.Fatalf("unexpected id: %v", logId)
	}

	if s := groupsToString(ls.lookup(catId)); s != "cat" {
		t.Fatalf("unexpected lookup: %v", s)
	}

	if s := groupsToString(ls.lookup(dogId)); s != "dog" {
		t.Fatalf("unexpected lookup: %v", s)
	}

	if s := groupsToString(ls.lookup(catalogId)); s != "catalog" {
		t.Fatalf("unexpected lookup: %v", s)
	}
}

func stringToGroups(s string) []opt.GroupID {
	groups := make([]opt.GroupID, len(s))
	for i, rune := range s {
		groups[i] = opt.GroupID(rune)
	}
	return groups
}

func groupsToString(groups []opt.GroupID) string {
	var buf bytes.Buffer
	for _, group := range groups {
		buf.WriteRune(rune(group))
	}
	return buf.String()
}
