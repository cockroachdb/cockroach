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

// TestNextKey tests that the method for creating successors of a Key
// works as expected.
func TestNextKey(t *testing.T) {
	testCases := []struct {
		key  Key
		next Key
	}{
		{nil, Key("\x00")},
		{Key(""), Key("\x00")},
		{Key("test key"), Key("test key\x00")},
		{Key(KeyMax), Key("\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !bytes.Equal(NextKey(c.key), (c.next)) {
			t.Fatalf("%d: unexpected next key for \"%s\": %s", i, NextKey(c.key))
		}
	}
}

func TestMakeKey(t *testing.T) {
	if !bytes.Equal(MakeKey(Key("A"), Key("B")), Key("AB")) ||
		!bytes.Equal(MakeKey(Key("A")), Key("A")) ||
		!bytes.Equal(MakeKey(Key("A"), Key("B"), Key("C")), Key("ABC")) {
		t.Fatalf("MakeKey is broken")
	}
}
