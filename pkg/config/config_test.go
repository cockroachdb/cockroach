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
	"gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
		{sqlbase.MakeMetadataSchema().GetInitialValues(), keys.MaxSystemConfigDescID + 4, 0, ""},

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
		if !testutils.IsError(err, tc.errStr) {
			t.Errorf("#%d: expected err=%q, got %v", tcNum, tc.errStr, err)
			continue
		}
		if err != nil {
			continue
		}
		if ret != tc.largest {
			t.Errorf("#%d: expected largest=%d, got %d", tcNum, tc.largest, ret)
		}
	}
}

func TestStaticSplits(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for i := range config.StaticSplits {
		if config.StaticSplits[i].SplitKey.Less(config.StaticSplits[i].SplitPoint) {
			t.Errorf("SplitKey %q should not be less than SplitPoint %q",
				config.StaticSplits[i].SplitKey, config.StaticSplits[i].SplitPoint)
		}
		if i == 0 {
			continue
		}
		if !config.StaticSplits[i-1].SplitKey.Less(config.StaticSplits[i].SplitPoint) {
			t.Errorf("previous SplitKey %q should be less than next SplitPoint %q",
				config.StaticSplits[i-1].SplitKey, config.StaticSplits[i].SplitPoint)
		}
	}
}

// TestComputeSplitKeyTableIDs tests ComputeSplitKey for cases where the split
// is within the system ranges. Other cases are tested below by
// TestComputeSplitKeyTableIDs.
func TestComputeSplitKeySystemRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		start, end roachpb.RKey
		split      roachpb.Key
	}{
		{roachpb.RKeyMin, roachpb.RKeyMax, keys.SystemPrefix},
		{roachpb.RKeyMin, keys.MakeTablePrefix(1), keys.SystemPrefix},
		{roachpb.RKeyMin, roachpb.RKey(keys.TimeseriesPrefix), keys.SystemPrefix},
		{roachpb.RKeyMin, roachpb.RKey(keys.SystemPrefix.Next()), keys.SystemPrefix},
		{roachpb.RKeyMin, roachpb.RKey(keys.SystemPrefix), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.Meta2KeyMax), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.Meta1KeyMax), nil},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKey(keys.SystemPrefix), nil},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKey(keys.SystemPrefix.Next()), keys.SystemPrefix},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKeyMax, keys.SystemPrefix},
		{roachpb.RKey(keys.SystemPrefix), roachpb.RKey(keys.SystemPrefix), nil},
		{roachpb.RKey(keys.SystemPrefix), roachpb.RKey(keys.SystemPrefix.Next()), nil},
		{roachpb.RKey(keys.SystemPrefix), roachpb.RKeyMax, keys.TimeseriesPrefix},
		{roachpb.RKey(keys.MigrationPrefix), roachpb.RKey(keys.NodeLivenessPrefix), nil},
		{roachpb.RKey(keys.MigrationPrefix), roachpb.RKey(keys.NodeLivenessKeyMax), nil},
		{roachpb.RKey(keys.MigrationPrefix), roachpb.RKey(keys.StoreIDGenerator), nil},
		{roachpb.RKey(keys.MigrationPrefix), roachpb.RKey(keys.TimeseriesPrefix), nil},
		{roachpb.RKey(keys.MigrationPrefix), roachpb.RKey(keys.TimeseriesPrefix.Next()), keys.TimeseriesPrefix},
		{roachpb.RKey(keys.MigrationPrefix), roachpb.RKeyMax, keys.TimeseriesPrefix},
		{roachpb.RKey(keys.TimeseriesPrefix), roachpb.RKey(keys.TimeseriesPrefix.Next()), nil},
		{roachpb.RKey(keys.TimeseriesPrefix), roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), nil},
		{roachpb.RKey(keys.TimeseriesPrefix), roachpb.RKeyMax, keys.TimeseriesPrefix.PrefixEnd()},
		{roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), nil},
		{roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), roachpb.RKeyMax, keys.SystemConfigSplitKey},
	}

	cfg := config.SystemConfig{
		Values: sqlbase.MakeMetadataSchema().GetInitialValues(),
	}
	for tcNum, tc := range testCases {
		splitKey := cfg.ComputeSplitKey(tc.start, tc.end)
		expected := roachpb.RKey(tc.split)
		if !splitKey.Equal(expected) {
			t.Errorf("#%d: bad split:\ngot: %v\nexpected: %v", tcNum, splitKey, expected)
		}
	}
}

// TestComputeSplitKeyTableIDs tests ComputeSplitKey for cases where the split
// is at the start of a SQL table. Other cases are tested above by
// TestComputeSplitKeySystemRanges.
func TestComputeSplitKeyTableIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		start         = keys.MaxReservedDescID + 1
		reservedStart = keys.MaxSystemConfigDescID + 1
	)

	// Used in place of roachpb.RKeyMin in order to test the behavior of splits
	// at the start of the system config and user tables rather than within the
	// system ranges that come earlier in the keyspace. Those splits are tested
	// separately above.
	minKey := roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd())

	schema := sqlbase.MakeMetadataSchema()
	// Real system tables only.
	baseSql := schema.GetInitialValues()
	// Real system tables plus some user stuff.
	allSql := append(schema.GetInitialValues(),
		descriptor(start), descriptor(start+1), descriptor(start+5))
	sort.Sort(roachpb.KeyValueByKey(allSql))

	testCases := []struct {
		values     []roachpb.KeyValue
		start, end roachpb.RKey
		split      int32 // -1 to indicate no split is expected
	}{
		// No data.
		{nil, minKey, roachpb.RKeyMax, 0},
		{nil, keys.MakeTablePrefix(start), roachpb.RKeyMax, -1},
		{nil, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), -1},
		{nil, minKey, keys.MakeTablePrefix(start + 10), 0},

		// Reserved descriptors.
		{baseSql, minKey, roachpb.RKeyMax, 0},
		{baseSql, keys.MakeTablePrefix(start), roachpb.RKeyMax, -1},
		{baseSql, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), -1},
		{baseSql, minKey, keys.MakeTablePrefix(start + 10), 0},
		{baseSql, keys.MakeTablePrefix(reservedStart), roachpb.RKeyMax, reservedStart + 1},
		{baseSql, keys.MakeTablePrefix(reservedStart), keys.MakeTablePrefix(start + 10), reservedStart + 1},
		{baseSql, minKey, keys.MakeTablePrefix(reservedStart + 2), 0},
		{baseSql, minKey, keys.MakeTablePrefix(reservedStart + 10), 0},
		{baseSql, keys.MakeTablePrefix(reservedStart), keys.MakeTablePrefix(reservedStart + 2), reservedStart + 1},
		{baseSql, testutils.MakeKey(keys.MakeTablePrefix(reservedStart), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start+10), roachpb.RKey("foo")), reservedStart + 1},

		// Reserved + User descriptors.
		{allSql, keys.MakeTablePrefix(start - 1), roachpb.RKeyMax, start},
		{allSql, keys.MakeTablePrefix(start), roachpb.RKeyMax, start + 1},
		{allSql, keys.MakeTablePrefix(start), keys.MakeTablePrefix(start + 10), start + 1},
		{allSql, keys.MakeTablePrefix(start - 1), keys.MakeTablePrefix(start + 10), start},
		{allSql, keys.MakeTablePrefix(start + 4), keys.MakeTablePrefix(start + 10), start + 5},
		{allSql, keys.MakeTablePrefix(start + 5), keys.MakeTablePrefix(start + 10), -1},
		{allSql, keys.MakeTablePrefix(start + 6), keys.MakeTablePrefix(start + 10), -1},
		{allSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			keys.MakeTablePrefix(start + 10), start + 1},
		{allSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			keys.MakeTablePrefix(start + 5), start + 1},
		{allSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start+5), roachpb.RKey("bar")), start + 1},
		{allSql, testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start), roachpb.RKey("morefoo")), -1},
		{allSql, minKey, roachpb.RKeyMax, 0},
		{allSql, keys.MakeTablePrefix(reservedStart + 1), roachpb.RKeyMax, reservedStart + 2},
		{allSql, keys.MakeTablePrefix(reservedStart), keys.MakeTablePrefix(start + 10), reservedStart + 1},
		{allSql, minKey, keys.MakeTablePrefix(start + 2), 0},
		{allSql, testutils.MakeKey(keys.MakeTablePrefix(reservedStart), roachpb.RKey("foo")),
			testutils.MakeKey(keys.MakeTablePrefix(start+5), roachpb.RKey("foo")), reservedStart + 1},
	}

	cfg := config.SystemConfig{}
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		splitKey := cfg.ComputeSplitKey(tc.start, tc.end)
		if splitKey == nil && tc.split == -1 {
			continue
		}
		var expected roachpb.RKey
		if tc.split != -1 {
			expected = keys.MakeRowSentinelKey(keys.MakeTablePrefix(uint32(tc.split)))
		}
		if !splitKey.Equal(expected) {
			t.Errorf("#%d: bad split:\ngot: %v\nexpected: %v", tcNum, splitKey, expected)
		}
	}
}

func TestGetZoneConfigForKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		key        roachpb.RKey
		expectedID uint32
	}{
		{roachpb.RKeyMin, keys.MetaRangesID},
		{roachpb.RKey(keys.Meta1Prefix), keys.MetaRangesID},
		{roachpb.RKey(keys.Meta1Prefix.Next()), keys.MetaRangesID},
		{roachpb.RKey(keys.Meta2Prefix), keys.MetaRangesID},
		{roachpb.RKey(keys.Meta2Prefix.Next()), keys.MetaRangesID},
		{roachpb.RKey(keys.MetaMax), keys.SystemRangesID},
		{roachpb.RKey(keys.SystemPrefix), keys.SystemRangesID},
		{roachpb.RKey(keys.SystemPrefix.Next()), keys.SystemRangesID},
		{roachpb.RKey(keys.MigrationLease), keys.SystemRangesID},
		{roachpb.RKey(keys.NodeLivenessPrefix), keys.SystemRangesID},
		{roachpb.RKey(keys.DescIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.NodeIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.RangeIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.StoreIDGenerator), keys.SystemRangesID},
		{roachpb.RKey(keys.StatusPrefix), keys.SystemRangesID},
		{roachpb.RKey(keys.TimeseriesPrefix), keys.TimeseriesRangesID},
		{roachpb.RKey(keys.TimeseriesPrefix.Next()), keys.TimeseriesRangesID},
		{roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd()), keys.SystemRangesID},
		{roachpb.RKey(keys.TableDataMin), keys.SystemDatabaseID},
		{roachpb.RKey(keys.SystemConfigSplitKey), keys.SystemDatabaseID},
		{keys.MakeTablePrefix(keys.NamespaceTableID), keys.SystemDatabaseID},
		{keys.MakeTablePrefix(keys.ZonesTableID), keys.SystemDatabaseID},
		{keys.MakeTablePrefix(keys.LeaseTableID), keys.SystemDatabaseID},
		{keys.MakeTablePrefix(keys.JobsTableID), keys.SystemDatabaseID},
		{keys.MakeTablePrefix(keys.MaxReservedDescID + 1), keys.MaxReservedDescID + 1},
		{keys.MakeTablePrefix(keys.MaxReservedDescID + 23), keys.MaxReservedDescID + 23},
		{roachpb.RKeyMax, keys.RootNamespaceID},
	}

	originalZoneConfigHook := config.ZoneConfigHook
	defer func() {
		config.ZoneConfigHook = originalZoneConfigHook
	}()
	cfg := config.SystemConfig{
		Values: sqlbase.MakeMetadataSchema().GetInitialValues(),
	}
	for tcNum, tc := range testCases {
		var objectID uint32
		config.ZoneConfigHook = func(_ config.SystemConfig, id uint32) (config.ZoneConfig, bool, error) {
			objectID = id
			return config.ZoneConfig{}, false, nil
		}
		_, err := cfg.GetZoneConfigForKey(tc.key)
		if err != nil {
			t.Errorf("#%d: GetZoneConfigForKey(%v) got error: %v", tcNum, tc.key, err)
		}
		if objectID != tc.expectedID {
			t.Errorf("#%d: GetZoneConfigForKey(%v) got %d; want %d", tcNum, tc.key, objectID, tc.expectedID)
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
				NumReplicas: 2,
			},
			"at least 3 replicas are required for multi-replica configurations",
		},
		{
			config.ZoneConfig{
				NumReplicas: 1,
			},
			"RangeMaxBytes 0 less than minimum allowed",
		},
		{
			config.ZoneConfig{
				NumReplicas:   1,
				RangeMaxBytes: config.DefaultZoneConfig().RangeMaxBytes,
			},
			"",
		},
		{
			config.ZoneConfig{
				NumReplicas:   1,
				RangeMinBytes: config.DefaultZoneConfig().RangeMaxBytes,
				RangeMaxBytes: config.DefaultZoneConfig().RangeMaxBytes,
			},
			"is greater than or equal to RangeMaxBytes",
		},
	}
	for i, c := range testCases {
		err := c.cfg.Validate()
		if !testutils.IsError(err, c.expected) {
			t.Fatalf("%d: expected %q, got %v", i, c.expected, err)
		}
	}
}

// TestZoneConfigMarshalYAML makes sure that ZoneConfig is correctly marshaled
// to YAML and back.
func TestZoneConfigMarshalYAML(t *testing.T) {
	defer leaktest.AfterTest(t)()

	original := config.ZoneConfig{
		RangeMinBytes: 1,
		RangeMaxBytes: 1,
		GC: config.GCPolicy{
			TTLSeconds: 1,
		},
		NumReplicas: 1,
		Constraints: config.Constraints{
			Constraints: []config.Constraint{
				{
					Type:  config.Constraint_POSITIVE,
					Value: "foo",
				},
				{
					Type:  config.Constraint_REQUIRED,
					Key:   "duck",
					Value: "foo",
				},
				{
					Type:  config.Constraint_PROHIBITED,
					Key:   "duck",
					Value: "foo",
				},
			},
		},
	}

	expected := `range_min_bytes: 1
range_max_bytes: 1
gc:
  ttlseconds: 1
num_replicas: 1
constraints: [foo, +duck=foo, -duck=foo]
`

	body, err := yaml.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != expected {
		t.Fatalf("yaml.Marshal(%+v) = %s; not %s", original, body, expected)
	}

	var unmarshaled config.ZoneConfig
	if err := yaml.Unmarshal(body, &unmarshaled); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(unmarshaled, original) {
		t.Errorf("yaml.Unmarshal(%q) = %+v; not %+v", body, unmarshaled, original)
	}
}
