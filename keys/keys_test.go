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

package keys

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/uuid"
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
	if !LocalPrefix.Less(MetaPrefix) {
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
		{proto.Key{}, proto.KeyMin},
		{proto.Key("123"), proto.Key("123")},
		{RangeDescriptorKey(proto.Key("foo")), proto.Key("foo")},
		{TransactionKey(proto.Key("baz"), proto.Key(uuid.NewUUID4())), proto.Key("baz")},
		{TransactionKey(proto.KeyMax, proto.Key(uuid.NewUUID4())), proto.KeyMax},
		{nil, nil},
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
			expKey: proto.KeyMin,
		},
		{
			key:    proto.Key("\x00\x00meta2\x00zonefoo"),
			expKey: proto.Key("\x00\x00meta1\x00zonefoo"),
		},
		{
			key:    proto.Key("\x00\x00meta1\x00zonefoo"),
			expKey: proto.KeyMin,
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
			expKey: proto.KeyMin,
		},
	}
	for i, test := range testCases {
		result := RangeMetaKey(test.key)
		if !result.Equal(test.expKey) {
			t.Errorf("%d: expected range meta for key %q doesn't match %q", i, test.key, test.expKey)
		}
	}
}

// TestMetaPrefixLen asserts that both levels of meta keys have the same prefix length,
// as MetaScanBounds, MetaReverseScanBounds and validateRangeMetaKey depend on this fact.
func TestMetaPrefixLen(t *testing.T) {
	if len(Meta1Prefix) != len(Meta2Prefix) {
		t.Fatalf("Meta1Prefix %q and Meta2Prefix %q are not of equal length!", Meta1Prefix, Meta2Prefix)
	}
}

func TestMetaScanBounds(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		key, expStart, expEnd proto.Key
		expError              string
	}{
		{
			key:      proto.Key{},
			expStart: Meta1Prefix,
			expEnd:   Meta1Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      proto.MakeKey(Meta2Prefix, proto.Key("foo")),
			expStart: proto.MakeKey(Meta2Prefix, proto.Key("foo\x00")),
			expEnd:   Meta2Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      proto.MakeKey(Meta1Prefix, proto.Key("foo")),
			expStart: proto.MakeKey(Meta1Prefix, proto.Key("foo\x00")),
			expEnd:   Meta1Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      proto.MakeKey(Meta1Prefix, proto.KeyMax),
			expStart: proto.MakeKey(Meta1Prefix, proto.KeyMax),
			expEnd:   Meta1Prefix.PrefixEnd(),
			expError: "",
		},
		{
			key:      Meta2KeyMax,
			expStart: nil,
			expEnd:   nil,
			expError: "Meta2KeyMax can't be used as the key of scan",
		},
		{
			key:      Meta2KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
		{
			key:      Meta1KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
	}
	for i, test := range testCases {
		resStart, resEnd, err := MetaScanBounds(test.key)

		if err != nil && !testutils.IsError(err, test.expError) {
			t.Errorf("expected error: %s ; got %s", test.expError, err)
		} else if err == nil && test.expError != "" {
			t.Errorf("expected error: %s", test.expError)
		}

		if !resStart.Equal(test.expStart) || !resEnd.Equal(test.expEnd) {
			t.Errorf("%d: range bounds %q-%q don't match expected bounds %q-%q for key %q", i, resStart, resEnd, test.expStart, test.expEnd, test.key)
		}
	}
}

func TestMetaReverseScanBounds(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		key, expStart, expEnd proto.Key
		expError              string
	}{
		{
			key:      proto.Key{},
			expStart: nil,
			expEnd:   nil,
			expError: "KeyMin and Meta1Prefix can't be used as the key of reverse scan",
		},
		{
			key:      Meta1Prefix,
			expStart: nil,
			expEnd:   nil,
			expError: "KeyMin and Meta1Prefix can't be used as the key of reverse scan",
		},
		{
			key:      Meta2KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
		{
			key:      Meta1KeyMax.Next(),
			expStart: nil,
			expEnd:   nil,
			expError: "body of meta key range lookup is",
		},
		{
			key:      proto.MakeKey(Meta2Prefix, proto.Key("foo")),
			expStart: Meta2Prefix,
			expEnd:   proto.MakeKey(Meta2Prefix, proto.Key("foo\x00")),
			expError: "",
		},
		{
			key:      proto.MakeKey(Meta1Prefix, proto.Key("foo")),
			expStart: Meta1Prefix,
			expEnd:   proto.MakeKey(Meta1Prefix, proto.Key("foo\x00")),
			expError: "",
		},
		{
			key:      Meta2Prefix,
			expStart: Meta1Prefix,
			expEnd:   Meta2Prefix.Next(),
			expError: "",
		},
		{
			key:      Meta2KeyMax,
			expStart: Meta2Prefix,
			expEnd:   Meta2KeyMax.Next(),
			expError: "",
		},
	}
	for i, test := range testCases {
		resStart, resEnd, err := MetaReverseScanBounds(test.key)

		if err != nil && !testutils.IsError(err, test.expError) {
			t.Errorf("expected error: %s ; got %s", test.expError, err)
		} else if err == nil && test.expError != "" {
			t.Errorf("expected error: %s", test.expError)
		}

		if !resStart.Equal(test.expStart) || !resEnd.Equal(test.expEnd) {
			t.Errorf("%d: range bounds %q-%q don't match expected bounds %q-%q for key %q", i, resStart, resEnd, test.expStart, test.expEnd, test.key)
		}
	}
}

func TestValidateRangeMetaKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		key    proto.Key
		expErr bool
	}{
		{proto.KeyMin, false},
		{proto.Key("\x00"), true},
		{Meta1Prefix[:len(Meta1Prefix)-1], true},
		{Meta1Prefix, false},
		{proto.MakeKey(Meta1Prefix, proto.KeyMax), false},
		{proto.MakeKey(Meta2Prefix, proto.KeyMax), false},
		{proto.MakeKey(Meta2Prefix, proto.KeyMax.Next()), true},
	}
	for i, test := range testCases {
		err := validateRangeMetaKey(test.key)
		if err != nil != test.expErr {
			t.Errorf("%d: expected error? %t: %s", i, test.expErr, err)
		}
	}
}
