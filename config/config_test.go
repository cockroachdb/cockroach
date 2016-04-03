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
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

package config_test

import (
	"reflect"
	"sort"
	"testing"

	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/privilege"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func plainKV(k, v string) roachpb.KeyValue {
	return kv([]byte(k), []byte(v))
}

func sqlKV(tableID uint32, indexID, descriptorID uint64) roachpb.KeyValue {
	k := keys.MakeTablePrefix(tableID)
	k = encoding.EncodeUvarintAscending(k, indexID)
	k = encoding.EncodeUvarintAscending(k, descriptorID)
	k = encoding.EncodeUvarintAscending(k, 12345) // Column ID, but could be anything.
	return kv(k, nil)
}

func descriptor(descriptorID uint64) roachpb.KeyValue {
	return sqlKV(uint32(keys.DescriptorTableID), 1, descriptorID)
}

func kv(k, v []byte) roachpb.KeyValue {
	return roachpb.KeyValue{
		Key:   k,
		Value: roachpb.MakeValueFromBytes(v),
	}
}

func TestObjectIDForKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		key     roachpb.RKey
		success bool
		id      uint32
	}{
		// Before the structured span.
		{roachpb.RKeyMin, false, 0},

		// Boundaries of structured span.
		{roachpb.RKeyMax, false, 0},

		// Valid, even if there are things after the ID.
		{testutils.MakeKey(keys.MakeTablePrefix(42), roachpb.RKey("\xff")), true, 42},
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
	defer leaktest.AfterTest(t)()

	emptyKeys := []roachpb.KeyValue{}
	someKeys := []roachpb.KeyValue{
		plainKV("a", "vala"),
		plainKV("c", "valc"),
		plainKV("d", "vald"),
	}

	aVal := roachpb.MakeValueFromString("vala")
	bVal := roachpb.MakeValueFromString("valc")
	cVal := roachpb.MakeValueFromString("vald")

	testCases := []struct {
		values []roachpb.KeyValue
		key    string
		value  *roachpb.Value
	}{
		{emptyKeys, "a", nil},
		{emptyKeys, "b", nil},
		{emptyKeys, "c", nil},
		{emptyKeys, "d", nil},
		{emptyKeys, "e", nil},

		{someKeys, "", nil},
		{someKeys, "b", nil},
		{someKeys, "e", nil},
		{someKeys, "a0", nil},

		{someKeys, "a", &aVal},
		{someKeys, "c", &bVal},
		{someKeys, "d", &cVal},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		if val := cfg.GetValue([]byte(tc.key)); !proto.Equal(val, tc.value) {
			t.Errorf("#%d: expected=%s, found=%s", tcNum, tc.value, val)
		}
	}
}

func TestGetLargestID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		values  []roachpb.KeyValue
		largest uint32
		maxID   uint32
		errStr  string
	}{
		// No data.
		{nil, 0, 0, "descriptor table not found"},

		// Some data, but not from the system span.
		{[]roachpb.KeyValue{plainKV("a", "b")}, 0, 0, "descriptor table not found"},

		// Some real data, but no descriptors.
		{[]roachpb.KeyValue{
			sqlKV(keys.NamespaceTableID, 1, 1),
			sqlKV(keys.NamespaceTableID, 1, 2),
			sqlKV(keys.UsersTableID, 1, 3),
		}, 0, 0, "descriptor table not found"},

		// Single correct descriptor entry.
		{[]roachpb.KeyValue{sqlKV(keys.DescriptorTableID, 1, 1)}, 1, 0, ""},

		// Surrounded by other data.
		{[]roachpb.KeyValue{
			sqlKV(keys.NamespaceTableID, 1, 20),
			sqlKV(keys.NamespaceTableID, 1, 30),
			sqlKV(keys.DescriptorTableID, 1, 8),
			sqlKV(keys.ZonesTableID, 1, 40),
		}, 8, 0, ""},

		// Descriptors with holes. Index ID does not matter.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 12, 0, ""},

		// Real SQL layout.
		{sql.MakeMetadataSchema().GetInitialValues(), keys.MaxSystemConfigDescID + 4, 0, ""},

		// Test non-zero max.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 8, 8, ""},

		// Test non-zero max.
		{[]roachpb.KeyValue{
			sqlKV(keys.DescriptorTableID, 1, 1),
			sqlKV(keys.DescriptorTableID, 2, 5),
			sqlKV(keys.DescriptorTableID, 3, 8),
			sqlKV(keys.DescriptorTableID, 4, 12),
		}, 5, 7, ""},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		ret, err := cfg.GetLargestObjectID(tc.maxID)
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
	defer leaktest.AfterTest(t)()

	const (
		start         = keys.MaxReservedDescID + 1
		reservedStart = keys.MaxSystemConfigDescID + 1
	)

	schema := sql.MakeMetadataSchema()
	// Real SQL system tables only.
	baseSql := schema.GetInitialValues()
	// Real SQL system tables plus some user stuff.
	userSql := append(schema.GetInitialValues(),
		descriptor(start), descriptor(start+1), descriptor(start+5))
	// Real SQL system with reserved non-system tables.
	schema.AddTable(reservedStart+1, "CREATE TABLE system.test1 (i INT PRIMARY KEY)",
		privilege.List{privilege.ALL})
	schema.AddTable(reservedStart+2, "CREATE TABLE system.test2 (i INT PRIMARY KEY)",
		privilege.List{privilege.ALL})
	reservedSql := schema.GetInitialValues()
	// Real SQL system with reserved non-system and user database.
	allSql := append(schema.GetInitialValues(),
		descriptor(start), descriptor(start+1), descriptor(start+5))
	sort.Sort(roachpb.KeyValueByKey(allSql))

	allUserSplits := []uint32{start, start + 1, start + 2, start + 3, start + 4, start + 5}
	var allReservedSplits []uint32
	for i := 0; i < schema.TableCount(); i++ {
		allReservedSplits = append(allReservedSplits, reservedStart+uint32(i))
	}
	allSplits := append(allReservedSplits, allUserSplits...)

	testCases := []struct {
		values     []roachpb.KeyValue
		start, end roachpb.RKey
		// Use ints in the testcase definitions, more readable.
		splits []uint32
	}{
		// No data.
		{nil, roachpb.RKeyMin, roachpb.RKeyMax, nil},
		{nil, keys.MakeTablePrefix(start), roachpb.RKeyMax, nil},
		{nil, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), nil},
		{nil, roachpb.RKeyMin, keys.MakeTablePrefix(start + 10), nil},

		// No user data.
		{baseSql, roachpb.RKeyMin, roachpb.RKeyMax, allReservedSplits},
		{baseSql, keys.MakeTablePrefix(start), roachpb.RKeyMax, nil},
		{baseSql, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), nil},
		{baseSql, roachpb.RKeyMin, keys.MakeTablePrefix(start + 10), allReservedSplits},

		// User descriptors.
		{userSql, keys.MakeTablePrefix(start - 1), roachpb.RKeyMax, allUserSplits},
		{userSql, keys.MakeTablePrefix(start), roachpb.RKeyMax, allUserSplits[1:]},
		{userSql, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), allUserSplits[1:]},
		{userSql, keys.MakeTablePrefix(start - 1), keys.MakeTablePrefix(start + 10), allUserSplits},
		{userSql, keys.MakeTablePrefix(start + 4), keys.MakeTablePrefix(start + 10), allUserSplits[5:]},
		{userSql, keys.MakeTablePrefix(start + 5), keys.MakeTablePrefix(start + 10), nil},
		{userSql, keys.MakeTablePrefix(start + 6), keys.MakeTablePrefix(start + 10), nil},
		{userSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			keys.MakeTablePrefix(start + 10), allUserSplits[1:]},
		{userSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			keys.MakeTablePrefix(start + 5), allUserSplits[1:5]},
		{userSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start+5), roachpb.RKey("bar")), allUserSplits[1:5]},
		{userSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("morefoo")), nil},

		// Reserved descriptors.
		{reservedSql, roachpb.RKeyMin, roachpb.RKeyMax, allReservedSplits},
		{reservedSql, keys.MakeTablePrefix(reservedStart), roachpb.RKeyMax, allReservedSplits[1:]},
		{reservedSql, keys.MakeTablePrefix(start), roachpb.RKeyMax, nil},
		{reservedSql, keys.MakeTablePrefix(reservedStart), keys.MakeTablePrefix(start + 10), allReservedSplits[1:]},
		{reservedSql, roachpb.RKeyMin, keys.MakeTablePrefix(reservedStart + 2), allReservedSplits[:2]},
		{reservedSql, roachpb.RKeyMin, keys.MakeTablePrefix(reservedStart + 10), allReservedSplits},
		{reservedSql, keys.MakeTablePrefix(reservedStart), keys.MakeTablePrefix(reservedStart + 2), allReservedSplits[1:2]},
		{reservedSql, testutils.MakeKey(keys.MakeTablePrefix(reservedStart), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start+10), roachpb.RKey("foo")), allReservedSplits[1:]},

		// Reserved/User mix.
		{allSql, roachpb.RKeyMin, roachpb.RKeyMax, allSplits},
		{allSql, keys.MakeTablePrefix(reservedStart + 1), roachpb.RKeyMax, allSplits[2:]},
		{allSql, keys.MakeTablePrefix(start), roachpb.RKeyMax, allUserSplits[1:]},
		{allSql, keys.MakeTablePrefix(reservedStart), keys.MakeTablePrefix(start + 10), allSplits[1:]},
		{allSql, roachpb.RKeyMin, keys.MakeTablePrefix(start + 2), allSplits[:6]},
		{allSql, testutils.MakeKey(keys.MakeTablePrefix(reservedStart), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start+5), roachpb.RKey("foo")), allSplits[1:9]},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		splits := cfg.ComputeSplitKeys(tc.start, tc.end)
		if len(splits) == 0 && len(tc.splits) == 0 {
			continue
		}

		// Convert ints to actual keys.
		expected := []roachpb.RKey{}
		for _, s := range tc.splits {
			expected = append(expected, keys.MakeNonColumnKey(keys.MakeTablePrefix(s)))
		}
		if !reflect.DeepEqual(splits, expected) {
			t.Errorf("#%d: bad splits:\ngot: %v\nexpected: %v", tcNum, splits, expected)
		}
	}
}

func TestZoneConfigValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		cfg      config.ZoneConfig
		expected string
	}{
		{
			config.ZoneConfig{},
			"attributes for at least one replica must be specified in zone config",
		},
		{
			config.ZoneConfig{
				ReplicaAttrs: make([]roachpb.Attributes, 2),
			},
			"at least 3 replicas are required for multi-replica configurations",
		},
		{
			config.ZoneConfig{
				ReplicaAttrs: make([]roachpb.Attributes, 1),
			},
			"RangeMaxBytes 0 less than minimum allowed",
		},
		{
			config.ZoneConfig{
				ReplicaAttrs:  make([]roachpb.Attributes, 1),
				RangeMaxBytes: config.DefaultZoneConfig().RangeMaxBytes,
			},
			"",
		},
		{
			config.ZoneConfig{
				ReplicaAttrs:  make([]roachpb.Attributes, 1),
				RangeMinBytes: config.DefaultZoneConfig().RangeMaxBytes,
				RangeMaxBytes: config.DefaultZoneConfig().RangeMaxBytes,
			},
			"is greater than or equal to RangeMaxBytes",
		},
	}
	for i, c := range testCases {
		err := c.cfg.Validate()
		if c.expected == "" {
			if err != nil {
				t.Fatalf("%d: expected success, but got %v", i, err)
			}
		} else if !testutils.IsError(err, c.expected) {
			t.Fatalf("%d: expected %s, but got %v", i, c.expected, err)
		}
	}
}
