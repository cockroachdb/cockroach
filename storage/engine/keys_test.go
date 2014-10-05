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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

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

func TestKeyEncodeDecode(t *testing.T) {
	a := Key("a")
	aEnc := a.Encode(nil)
	b := Key("b")
	bEnc := b.Encode(nil)
	if bytes.Compare(bEnc, aEnc) < 0 {
		t.Errorf("expected bEnc > aEnc")
	}
	aLeftover, aDec := DecodeKey(aEnc)
	if !aDec.Equal(a) {
		t.Errorf("exected a == aDec")
	}
	if len(aLeftover) != 0 {
		t.Errorf("expected leftover to be empty; got %q", aLeftover)
	}
}

// TestKeyNext tests that the method for creating lexicographic
// successors to byte slices works as expected.
func TestKeyNext(t *testing.T) {
	a := Key("a")
	aNext := a.Next()
	if a.Equal(aNext) {
		t.Errorf("expected key not equal to next")
	}
	if !a.Less(aNext) {
		t.Errorf("expected next key to be greater")
	}

	testCases := []struct {
		key  Key
		next Key
	}{
		{nil, Key("\x00")},
		{Key(""), Key("\x00")},
		{Key("test key"), Key("test key\x00")},
		{Key("\xff"), Key("\xff\x00")},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !bytes.Equal(c.key.Next(), c.next) {
			t.Errorf("%d: unexpected next bytes for %q: %q", i, c.key, c.key.Next())
		}
	}
}

func TestKeyPrefixEnd(t *testing.T) {
	a := Key("a1")
	aNext := a.Next()
	aEnd := a.PrefixEnd()
	if !a.Less(aEnd) {
		t.Errorf("expected end key to be greater")
	}
	if !aNext.Less(aEnd) {
		t.Errorf("expected end key to be greater than next")
	}

	testCases := []struct {
		key Key
		end Key
	}{
		{Key{}, KeyMax},
		{Key{0}, Key{0x01}},
		{Key{0xff}, Key{0xff}},
		{Key{0xff, 0xff}, Key{0xff, 0xff}},
		{KeyMax, KeyMax},
		{Key{0xff, 0xfe}, Key{0xff, 0xff}},
		{Key{0x00, 0x00}, Key{0x00, 0x01}},
		{Key{0x00, 0xff}, Key{0x01, 0x00}},
		{Key{0x00, 0xff, 0xff}, Key{0x01, 0x00, 0x00}},
	}
	for i, c := range testCases {
		if !bytes.Equal(c.key.PrefixEnd(), c.end) {
			t.Errorf("%d: unexpected prefix end bytes for %q: %q", i, c.key, c.key.PrefixEnd())
		}
	}
}

func TestKeyEqual(t *testing.T) {
	a1 := Key("a1")
	a2 := Key("a2")
	if !a1.Equal(a1) {
		t.Errorf("expected keys equal")
	}
	if a1.Equal(a2) {
		t.Errorf("expected different keys not equal")
	}
}

func TestKeyLess(t *testing.T) {
	testCases := []struct {
		a, b Key
		less bool
	}{
		{nil, Key("\x00"), true},
		{Key(""), Key("\x00"), true},
		{Key("a"), Key("b"), true},
		{Key("a\x00"), Key("a"), false},
		{Key("a\x00"), Key("a\x01"), true},
	}
	for i, c := range testCases {
		if c.a.Less(c.b) != c.less {
			t.Fatalf("%d: unexpected %q < %q: %t", i, c.a, c.b, c.less)
		}
	}
}

func TestKeyCompare(t *testing.T) {
	testCases := []struct {
		a, b    Key
		compare int
	}{
		{nil, nil, 0},
		{nil, Key("\x00"), -1},
		{Key("\x00"), Key("\x00"), 0},
		{Key(""), Key("\x00"), -1},
		{Key("a"), Key("b"), -1},
		{Key("a\x00"), Key("a"), 1},
		{Key("a\x00"), Key("a\x01"), -1},
	}
	for i, c := range testCases {
		if c.a.Compare(c.b) != c.compare {
			t.Fatalf("%d: unexpected %q.Compare(%q): %d", i, c.a, c.b, c.compare)
		}
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
		{Key(KeyMax), MakeKey(KeyMax, []byte("\x00"))},
		{Key("xoxo\x00"), Key("xoxo\x00\x00")},
	}
	for i, c := range testCases {
		if !c.key.Next().Equal(c.next) {
			t.Fatalf("%d: unexpected next key for %q: %s", i, c.key, c.key.Next())
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

func TestRangeMetaKey(t *testing.T) {
	testCases := []struct {
		key, expKey Key
	}{
		{
			key:    Key{},
			expKey: KeyMin,
		},
		{
			key:    MakeLocalKey(KeyLocalTransactionPrefix, Key("foo")),
			expKey: Key("\x00\x00meta2foo"),
		},
		{
			key:    MakeLocalKey(KeyLocalResponseCachePrefix, Key("bar")),
			expKey: Key("\x00\x00meta2bar"),
		},
		{
			key:    MakeKey(KeyConfigAccountingPrefix, Key("foo")),
			expKey: Key("\x00\x00meta2\x00acctfoo"),
		},
		{
			key:    Key("\x00\x00meta2\x00acctfoo"),
			expKey: Key("\x00\x00meta1\x00acctfoo"),
		},
		{
			key:    Key("\x00\x00meta1\x00acctfoo"),
			expKey: KeyMin,
		},
		{
			key:    Key("foo"),
			expKey: Key("\x00\x00meta2foo"),
		},
		{
			key:    Key("foo"),
			expKey: Key("\x00\x00meta2foo"),
		},
		{
			key:    Key("\x00\x00meta2foo"),
			expKey: Key("\x00\x00meta1foo"),
		},
		{
			key:    Key("\x00\x00meta1foo"),
			expKey: KeyMin,
		},
	}
	for i, test := range testCases {
		result := RangeMetaKey(test.key)
		if !result.Equal(test.expKey) {
			t.Errorf("%d: expected range meta for key %q doesn't match %q", i, test.key, test.expKey)
		}
	}
}
