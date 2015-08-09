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
	"github.com/cockroachdb/cockroach/util/encoding"
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
		{MakeKey(ConfigAccountingPrefix, proto.Key("foo")), proto.Key("\x00acctfoo")},
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

func TestMakeTableIndexKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	key := MakeTableIndexKey(12, 345, []byte("foo"), []byte("bar"))
	expKey := MakeKey(TableDataPrefix, encoding.EncodeUvarint(nil, 12), encoding.EncodeUvarint(nil, 345), encoding.EncodeBytes(nil, []byte("foo")), encoding.EncodeBytes(nil, []byte("bar")))
	if !key.Equal(expKey) {
		t.Errorf("key %q doesn't match expected %q", key, expKey)
	}
	// Check that keys are ordered
	keys := []proto.Key{
		MakeTableIndexKey(0, 0, []byte("foo")),
		MakeTableIndexKey(0, 0, []byte("fooo")),
		MakeTableIndexKey(0, 1, []byte("bar")),
		MakeTableIndexKey(1, 0, []byte("bar")),
		MakeTableIndexKey(1, 0, []byte("bar"), []byte("foo")),
		MakeTableIndexKey(1, 1, []byte("bar"), []byte("fo")),
		MakeTableIndexKey(1, 1, []byte("bar"), []byte("foo")),
		MakeTableIndexKey(1, 2, []byte("bar")),
		MakeTableIndexKey(2, 2, []byte("ba")),
	}
	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			t.Errorf("key %d >= key %d", i-1, i)
		}
	}
}

func TestMakeTableDataKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	key := MakeTableDataKey(12, 345, 6, []byte("foo"), []byte("bar"))
	// Expected key is the TableIndexKey + ColumnID
	expKey := MakeKey(MakeTableIndexKey(12, 345, []byte("foo"), []byte("bar")), encoding.EncodeUvarint(nil, 6))
	if !key.Equal(expKey) {
		t.Errorf("key %q doesn't match expected %q", key, expKey)
	}
	// Check that keys are ordered
	keys := []proto.Key{
		// Data-key follows Index key order
		MakeTableDataKey(0, 0, 0, []byte("foo")),
		MakeTableDataKey(0, 0, 8, []byte("fooo")),
		MakeTableDataKey(0, 1, 10, []byte("bar")),
		MakeTableDataKey(1, 0, 3, []byte("bar")),
		MakeTableDataKey(1, 0, 5, []byte("bar"), []byte("foo")),
		MakeTableDataKey(1, 1, 7, []byte("bar"), []byte("fo")),
		MakeTableDataKey(1, 1, 4, []byte("bar"), []byte("foo")),
		MakeTableDataKey(1, 2, 89, []byte("bar")),
		MakeTableDataKey(2, 2, 23, []byte("ba")),
		// For the same Index key, Data-key follows column ID order.
		MakeTableDataKey(2, 2, 0, []byte("bar"), []byte("foo")),
		MakeTableDataKey(2, 2, 7, []byte("bar"), []byte("foo")),
		MakeTableDataKey(2, 2, 23, []byte("bar"), []byte("foo")),
		MakeTableDataKey(2, 2, 45, []byte("bar"), []byte("foo")),
	}

	for i := 1; i < len(keys); i++ {
		if bytes.Compare(keys[i-1], keys[i]) >= 0 {
			t.Errorf("key %d >= key %d", i-1, i)
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
			key:    MakeKey(ConfigAccountingPrefix, proto.Key("foo")),
			expKey: proto.Key("\x00\x00meta2\x00acctfoo"),
		},
		{
			key:    proto.Key("\x00\x00meta2\x00acctfoo"),
			expKey: proto.Key("\x00\x00meta1\x00acctfoo"),
		},
		{
			key:    proto.Key("\x00\x00meta1\x00acctfoo"),
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
// as MetaScanBounds and ValidateRangeMetaKey depend on this fact.
func TestMetaPrefixLen(t *testing.T) {
	if len(Meta1Prefix) != len(Meta2Prefix) {
		t.Fatalf("Meta1Prefix %q and Meta2Prefix %q are not of equal length!", Meta1Prefix, Meta2Prefix)
	}
}

func TestMetaScanBounds(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		key, expStart, expEnd proto.Key
	}{
		{
			key:      proto.Key{},
			expStart: Meta1Prefix,
			expEnd:   Meta1Prefix.PrefixEnd(),
		},
		{
			key:      proto.Key("foo"),
			expStart: proto.Key("foo").Next(),
			expEnd:   proto.Key("foo")[:len(Meta1Prefix)].PrefixEnd(),
		},
		{
			key:      proto.MakeKey(Meta1Prefix, proto.KeyMax),
			expStart: proto.MakeKey(Meta1Prefix, proto.KeyMax),
			expEnd:   Meta1Prefix.PrefixEnd(),
		},
	}
	for i, test := range testCases {
		resStart, resEnd := MetaScanBounds(test.key)
		if !resStart.Equal(test.expStart) || !resEnd.Equal(test.expEnd) {
			t.Errorf("%d: range bounds %q-%q don't match expected bounds %q-%q for key %q", i, resStart, resEnd, test.expStart, test.expEnd, test.key)
		}
	}
}

func TestMetaReverseScanBounds(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		key, expStart, expEnd proto.Key
		err                   error
	}{
		{
			key:      proto.Key{},
			expStart: nil,
			expEnd:   nil,
			err:      NewInvalidRangeMetaKeyError("KeyMin or Meta1Prefix can't be used as the key of reverse scan", proto.Key{}),
		},
		{
			key:      Meta1Prefix,
			expStart: nil,
			expEnd:   nil,
			err:      NewInvalidRangeMetaKeyError("KeyMin or Meta1Prefix can't be used as the key of reverse scan", Meta1Prefix),
		},
		{
			key:      proto.MakeKey(Meta2Prefix, proto.Key("foo")),
			expStart: Meta2Prefix,
			expEnd:   proto.MakeKey(Meta2Prefix, proto.Key("foo\x00")),
			err:      nil,
		},
		{
			key:      proto.MakeKey(Meta1Prefix, proto.Key("foo")),
			expStart: Meta1Prefix,
			expEnd:   proto.MakeKey(Meta1Prefix, proto.Key("foo\x00")),
			err:      nil,
		},
		{
			key:      Meta2Prefix,
			expStart: Meta1Prefix,
			expEnd:   Meta2Prefix.Next(),
			err:      nil,
		},
	}
	for i, test := range testCases {
		resStart, resEnd, _ := MetaReverseScanBounds(test.key)
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
		{proto.MakeKey(Meta2Prefix, proto.KeyMax), true},
		{proto.MakeKey(Meta2Prefix, proto.KeyMax.Next()), true},
	}
	for i, test := range testCases {
		err := ValidateRangeMetaKey(test.key)
		if err != nil != test.expErr {
			t.Errorf("%d: expected error? %t: %s", i, test.expErr, err)
		}
	}
}
