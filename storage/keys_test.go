// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"bytes"
	"testing"
)

// TestLocalKeySorting is a sanity check to make sure that
// the non-replicated part of a store sorts before the meta.
func TestKeySorting(t *testing.T) {
	// Reminder: Increasing the last byte by one < adding a null byte.
	if !(Key("").Less(Key("\x00")) && Key("\x00").Less(Key("\x01")) &&
		Key("\x01").Less(Key("\x01\x00"))) {
		t.Fatalf("something is seriously wrong with this machine")
	}
	if !KeyLocalPrefix.Less(KeyMetaPrefix) {
		t.Fatalf("local key spilling into replicable ranges")
	}
	if !bytes.Equal(Key(""), Key(nil)) || !bytes.Equal(Key(""), Key(nil)) {
		t.Fatalf("equality between keys failed")
	}
}

// TestPrevAndNextKey tests that the methods for creating
// successors and predecessors of a Key work as expected.
func TestPrevNextKey(t *testing.T) {
	testCases := []struct {
		key  Key
		prev Key
		next Key
	}{
		{nil, Key(""), Key("\x00")},
		{Key(""), Key(""), Key("\x00")},
		{Key("test key"), Key("test ke\x78"), Key("test ke\x7a")},
		{Key(KeyMax), Key("\xfe"), Key("\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo"), Key("xoxo\x01")},
	}
	for i, c := range testCases {
		if !bytes.Equal(PrevKey(c.key), c.prev) || !bytes.Equal(NextKey(c.key), (c.next)) {
			t.Fatalf("%d: unexpected prev/next key for \"%s\": %s, %s", i, c.key, PrevKey(c.key), NextKey(c.key))
		}
	}

}
