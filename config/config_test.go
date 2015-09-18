// Copyright 2015 The Cockroach Authors.
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
// Author: Marc Berhault (marc@cockroachlabs.com)

package config_test

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func plainKV(k, v string) proto.KeyValue {
	return kv([]byte(k), []byte(v))
}

func sqlKV(tableID uint32, indexID, descriptorID uint64) proto.KeyValue {
	k := keys.MakeTablePrefix(tableID)
	k = encoding.EncodeUvarint(k, indexID)
	k = encoding.EncodeUvarint(k, descriptorID)
	k = encoding.EncodeUvarint(k, 12345) // Column ID, but could be anything.
	return kv(k, nil)
}

func descriptor(descriptorID uint32) proto.KeyValue {
	return sqlKV(uint32(keys.DescriptorTableID), 1, uint64(descriptorID))
}

func kv(k, v []byte) proto.KeyValue {
	return proto.KeyValue{
		Key:   k,
		Value: proto.Value{Bytes: v},
	}
}

func TestObjectIDForKey(t *testing.T) {
	defer leaktest.AfterTest(t)

	testCases := []struct {
		key     proto.Key
		success bool
		id      uint32
	}{
		// Before the structured span.
		{proto.Key(""), false, 0},
		{keys.SystemMax, false, 0},

		// Boundaries of structured span.
		{keys.TableDataPrefix, false, 0},
		{proto.KeyMax, false, 0},

		// In system span, but no Uvarint ID.
		{keys.MakeKey(keys.TableDataPrefix, proto.Key("foo")), false, 0},

		// Valid, even if there are things after the ID.
		{keys.MakeKey(keys.MakeTablePrefix(42), proto.Key("foo")), true, 42},
		{keys.MakeTablePrefix(0), true, 0},
		{keys.MakeTablePrefix(999), true, 999},
	}

	for tcNum, tc := range testCases {
		id, success := config.ObjectIDForKey(tc.key)
		if success != tc.success {
			t.Errorf("#%d: expected success=%t", tcNum, tc.success)
			continue
		}
		if id != tc.id {
			t.Errorf("#%d: expected id=%d, got %d", tcNum, tc.id, id)
		}
	}
}

func TestGet(t *testing.T) {
	defer leaktest.AfterTest(t)

	emptyKeys := []proto.KeyValue{}
	someKeys := []proto.KeyValue{
		plainKV("a", "vala"),
		plainKV("c", "valc"),
		plainKV("d", "vald"),
	}

	testCases := []struct {
		values []proto.KeyValue
		key    string
		found  bool
		value  string
	}{
		{emptyKeys, "a", false, ""},
		{emptyKeys, "b", false, ""},
		{emptyKeys, "c", false, ""},
		{emptyKeys, "d", false, ""},
		{emptyKeys, "e", false, ""},

		{someKeys, "", false, ""},
		{someKeys, "b", false, ""},
		{someKeys, "e", false, ""},
		{someKeys, "a0", false, ""},

		{someKeys, "a", true, "vala"},
		{someKeys, "c", true, "valc"},
		{someKeys, "d", true, "vald"},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		val, found := cfg.GetValue([]byte(tc.key))
		if found != tc.found {
			t.Errorf("#%d: expected found=%t", tcNum, tc.found)
			continue
		}
		if string(val) != tc.value {
			t.Errorf("#%d: expected value=%s, found %s", tcNum, tc.value, string(val))
		}
	}
}

func TestGetLargestID(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		values  []proto.KeyValue
		largest uint32
		errStr  string
	}{
		// No data.
		{nil, 0, "empty system values"},

		// Some data, but not from the system span.
		{[]proto.KeyValue{plainKV("a", "b")}, 0, "descriptor table not found"},

		// Some real data, but no descriptors.
		{[]proto.KeyValue{
			sqlKV(keys.NamespaceTableID, 1, 1),
			sqlKV(keys.NamespaceTableID, 1, 2),
			sqlKV(keys.UsersTableID, 1, 3),
		}, 0, "descriptor table not found"},

		// Single correct descriptor entry.
		{[]proto.KeyValue{sqlKV(keys.DescriptorTableID, 1, 1)}, 1, ""},

		// Surrounded by other data.
		{[]proto.KeyValue{
			sqlKV(keys.NamespaceTableID, 1, 20),
			sqlKV(keys.NamespaceTableID, 1, 30),
			sqlKV(keys.DescriptorTableID, 1, 8),
			sqlKV(keys.ZonesTableID, 1, 40),
		}, 8, ""},

		// Descriptors with holes. Index ID does not matter.
		{[]proto.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 12, ""},

		// Real SQL layout.
		{sql.GetInitialSystemValues(), keys.ZonesTableID, ""},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		ret, err := cfg.GetLargestObjectID()
		if tc.errStr == "" {
			if err != nil {
				t.Errorf("#%d: error: %v", tcNum, err)
				continue
			}
		} else if !testutils.IsError(err, tc.errStr) {
			t.Errorf("#%d: expected err=%s, got %v", tcNum, tc.errStr, err)
			continue
		}
		if ret != tc.largest {
			t.Errorf("#%d: expected largest=%d, got %d", tcNum, tc.largest, ret)
		}
	}
}

func TestComputeSplits(t *testing.T) {
	defer leaktest.AfterTest(t)

	start := uint32(keys.MaxReservedDescID + 1)

	// Real SQL system tables only.
	baseSql := sql.GetInitialSystemValues()
	// Real SQL system tables plus some user stuff.
	userSql := append(sql.GetInitialSystemValues(),
		descriptor(start), descriptor(start+1), descriptor(start+5))

	allSplits := []uint32{start, start + 1, start + 2, start + 3, start + 4, start + 5}

	testCases := []struct {
		values     []proto.KeyValue
		start, end proto.Key
		// Use ints in the testcase definitions, more readable.
		splits []uint32
	}{
		// No data.
		{nil, proto.KeyMin, proto.KeyMax, nil},
		{nil, keys.MakeTablePrefix(start), proto.KeyMax, nil},
		{nil, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), nil},
		{nil, proto.KeyMin, keys.MakeTablePrefix(start + 10), nil},

		// No user data.
		{baseSql, proto.KeyMin, proto.KeyMax, nil},
		{baseSql, keys.MakeTablePrefix(start), proto.KeyMax, nil},
		{baseSql, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), nil},
		{baseSql, proto.KeyMin, keys.MakeTablePrefix(start + 10), nil},

		// User descriptors.
		{userSql, proto.KeyMin, proto.KeyMax, allSplits},
		{userSql, keys.MakeTablePrefix(start), proto.KeyMax, allSplits[1:]},
		{userSql, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), allSplits[1:]},
		{userSql, proto.KeyMin, keys.MakeTablePrefix(start + 10), allSplits},
		{userSql, keys.MakeTablePrefix(start + 4), keys.MakeTablePrefix(start + 10), allSplits[5:]},
		{userSql, keys.MakeTablePrefix(start + 5), keys.MakeTablePrefix(start + 10), nil},
		{userSql, keys.MakeTablePrefix(start + 6), keys.MakeTablePrefix(start + 10), nil},
		{userSql, keys.MakeKey(keys.MakeTablePrefix(start), proto.Key("foo")),
			keys.MakeTablePrefix(start + 10), allSplits[1:]},
		{userSql, keys.MakeKey(keys.MakeTablePrefix(start), proto.Key("foo")),
			keys.MakeTablePrefix(start + 5), allSplits[1:5]},
		{userSql, keys.MakeKey(keys.MakeTablePrefix(start), proto.Key("foo")),
			keys.MakeKey(keys.MakeTablePrefix(start+5), proto.Key("bar")), allSplits[1:]},
		{userSql, keys.MakeKey(keys.MakeTablePrefix(start), proto.Key("foo")),
			keys.MakeKey(keys.MakeTablePrefix(start), proto.Key("morefoo")), nil},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		splits := cfg.ComputeSplitKeys(tc.start, tc.end)
		if len(splits) == 0 && len(tc.splits) == 0 {
			continue
		}

		// Convert ints to actual keys.
		expected := []proto.Key{}
		if tc.splits != nil {
			for _, s := range tc.splits {
				expected = append(expected, keys.MakeTablePrefix(s))
			}
		}
		if !reflect.DeepEqual(splits, expected) {
			t.Errorf("#%d: bad splits:\ngot: %v\nexpected: %v", tcNum, splits, expected)
		}
	}
}
