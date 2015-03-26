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

	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestLocalKeySorting is a sanity check to make sure that
// the non-replicated part of a store sorts before the meta.
func TestKeySorting(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Reminder: Increasing the last byte by one < adding a null byte.
	if !(proto.Key("").Less(proto.Key("\x00")) && proto.Key("\x00").Less(proto.Key("\x01")) &&
		proto.Key("\x01").Less(proto.Key("\x01\x00"))) {
		t.Fatalf("something is seriously wrong with this machine")
	}
	if !KeyLocalPrefix.Less(KeyMetaPrefix) {
		t.Fatalf("local key spilling into replicable ranges")
	}
	if !bytes.Equal(proto.Key(""), proto.Key(nil)) || !bytes.Equal(proto.Key(""), proto.Key(nil)) {
		t.Fatalf("equality between keys failed")
	}
}

func TestMakeKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	if !bytes.Equal(MakeKey(proto.Key("A"), proto.Key("B")), proto.Key("AB")) ||
		!bytes.Equal(MakeKey(proto.Key("A")), proto.Key("A")) ||
		!bytes.Equal(MakeKey(proto.Key("A"), proto.Key("B"), proto.Key("C")), proto.Key("ABC")) {
		t.Fatalf("MakeKey is broken")
	}
}

func TestKeyAddress(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		key, expAddress proto.Key
	}{
		{proto.Key{}, KeyMin},
		{proto.Key("123"), proto.Key("123")},
		{MakeKey(KeyConfigAccountingPrefix, proto.Key("foo")), proto.Key("\x00acctfoo")},
		{RangeDescriptorKey(proto.Key("foo")), proto.Key("foo")},
		{TransactionKey(proto.Key("baz"), proto.Key(uuid.New())), proto.Key("baz")},
		{TransactionKey(KeyMax, proto.Key(uuid.New())), KeyMax},
	}
	for i, test := range testCases {
		result := KeyAddress(test.key)
		if !result.Equal(test.expAddress) {
			t.Errorf("%d: expected address for key %q doesn't match %q", i, test.key, test.expAddress)
		}
	}
}

func TestRangeMetaKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		key, expKey proto.Key
	}{
		{
			key:    proto.Key{},
			expKey: KeyMin,
		},
		{
			key:    MakeKey(KeyConfigAccountingPrefix, proto.Key("foo")),
			expKey: proto.Key("\x00\x00meta2\x00acctfoo"),
		},
		{
			key:    proto.Key("\x00\x00meta2\x00acctfoo"),
			expKey: proto.Key("\x00\x00meta1\x00acctfoo"),
		},
		{
			key:    proto.Key("\x00\x00meta1\x00acctfoo"),
			expKey: KeyMin,
		},
		{
			key:    proto.Key("foo"),
			expKey: proto.Key("\x00\x00meta2foo"),
		},
		{
			key:    proto.Key("foo"),
			expKey: proto.Key("\x00\x00meta2foo"),
		},
		{
			key:    proto.Key("\x00\x00meta2foo"),
			expKey: proto.Key("\x00\x00meta1foo"),
		},
		{
			key:    proto.Key("\x00\x00meta1foo"),
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
