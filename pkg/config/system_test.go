// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config_test

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

// TODO(benesch): Don't reinvent the key encoding here.

func plainKV(k, v string) roachpb.KeyValue {
	return kv([]byte(k), []byte(v))
}

func tkey(tableID uint32, chunks ...string) []byte {
	key := keys.MakeTablePrefix(tableID)
	for _, c := range chunks {
		key = append(key, []byte(c)...)
	}
	return key
}

func sqlKV(tableID uint32, indexID, descriptorID uint64) roachpb.KeyValue {
	k := tkey(tableID)
	k = encoding.EncodeUvarintAscending(k, indexID)
	k = encoding.EncodeUvarintAscending(k, descriptorID)
	k = encoding.EncodeUvarintAscending(k, 12345) // Column ID, but could be anything.
	return kv(k, nil)
}

func descriptor(descriptorID uint64) roachpb.KeyValue {
	return kv(sqlbase.MakeDescMetadataKey(sqlbase.ID(descriptorID)), nil)
}

func zoneConfig(descriptorID uint32, spans ...config.SubzoneSpan) roachpb.KeyValue {
	kv := roachpb.KeyValue{
		Key: config.MakeZoneKey(descriptorID),
	}
	if err := kv.Value.SetProto(&config.ZoneConfig{SubzoneSpans: spans}); err != nil {
		panic(err)
	}
	return kv
}

func subzone(start, end string) config.SubzoneSpan {
	return config.SubzoneSpan{Key: []byte(start), EndKey: []byte(end)}
}

func kv(k, v []byte) roachpb.KeyValue {
	return roachpb.KeyValue{
		Key:   k,
		Value: roachpb.MakeValueFromBytes(v),
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

	cfg := config.NewSystemConfig(config.DefaultZoneConfigRef())
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		if val := cfg.GetValue([]byte(tc.key)); !proto.Equal(val, tc.value) {
			t.Errorf("#%d: expected=%s, found=%s", tcNum, tc.value, val)
		}
	}
}

func TestGetLargestID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		values  []roachpb.KeyValue
		largest uint32
		maxID   uint32
		errStr  string
	}

	testCases := []testCase{
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
		func() testCase {
			ms := sqlbase.MakeMetadataSchema(config.DefaultZoneConfigRef(), config.DefaultSystemZoneConfigRef())
			descIDs := ms.DescriptorIDs()
			maxDescID := descIDs[len(descIDs)-1]
			kvs, _ /* splits */ := ms.GetInitialValues(cluster.TestingClusterVersion)
			return testCase{kvs, uint32(maxDescID), 0, ""}
		}(),

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

	cfg := config.NewSystemConfig(config.DefaultZoneConfigRef())
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

	splits := config.StaticSplits()
	for i := 1; i < len(splits); i++ {
		if !splits[i-1].Less(splits[i]) {
			t.Errorf("previous split %q should be less than next split %q", splits[i-1], splits[i])
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
		{roachpb.RKeyMin, roachpb.RKeyMax, keys.NodeLivenessPrefix},
		{roachpb.RKeyMin, tkey(1), keys.NodeLivenessPrefix},
		{roachpb.RKeyMin, roachpb.RKey(keys.TimeseriesPrefix), keys.NodeLivenessPrefix},
		{roachpb.RKeyMin, roachpb.RKey(keys.SystemPrefix.Next()), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.SystemPrefix), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.MetaMax), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.Meta2KeyMax), nil},
		{roachpb.RKeyMin, roachpb.RKey(keys.Meta1KeyMax), nil},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKey(keys.SystemPrefix), nil},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKey(keys.SystemPrefix.Next()), nil},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKey(keys.NodeLivenessPrefix), nil},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKey(keys.NodeLivenessPrefix.Next()), keys.NodeLivenessPrefix},
		{roachpb.RKey(keys.Meta1KeyMax), roachpb.RKeyMax, keys.NodeLivenessPrefix},
		{roachpb.RKey(keys.SystemPrefix), roachpb.RKey(keys.SystemPrefix), nil},
		{roachpb.RKey(keys.SystemPrefix), roachpb.RKey(keys.SystemPrefix.Next()), nil},
		{roachpb.RKey(keys.SystemPrefix), roachpb.RKeyMax, keys.NodeLivenessPrefix},
		{roachpb.RKey(keys.NodeLivenessPrefix), roachpb.RKey(keys.NodeLivenessPrefix.Next()), nil},
		{roachpb.RKey(keys.NodeLivenessPrefix), roachpb.RKey(keys.NodeLivenessKeyMax), nil},
		{roachpb.RKey(keys.NodeLivenessPrefix), roachpb.RKeyMax, keys.NodeLivenessKeyMax},
		{roachpb.RKey(keys.NodeLivenessKeyMax), roachpb.RKeyMax, keys.TimeseriesPrefix},
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

	cfg := config.NewSystemConfig(config.DefaultZoneConfigRef())
	kvs, _ /* splits */ := sqlbase.MakeMetadataSchema(cfg.DefaultZoneConfig, config.DefaultSystemZoneConfigRef()).GetInitialValues(cluster.TestingClusterVersion)
	cfg.SystemConfigEntries = config.SystemConfigEntries{
		Values: kvs,
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
		start         = keys.MinUserDescID
		reservedStart = keys.MaxSystemConfigDescID + 1
	)

	// Used in place of roachpb.RKeyMin in order to test the behavior of splits
	// at the start of the system config and user tables rather than within the
	// system ranges that come earlier in the keyspace. Those splits are tested
	// separately above.
	minKey := roachpb.RKey(keys.TimeseriesPrefix.PrefixEnd())

	schema := sqlbase.MakeMetadataSchema(config.DefaultZoneConfigRef(), config.DefaultSystemZoneConfigRef())
	// Real system tables only.
	baseSql, _ /* splits */ := schema.GetInitialValues(cluster.TestingClusterVersion)
	// Real system tables plus some user stuff.
	kvs, _ /* splits */ := schema.GetInitialValues(cluster.TestingClusterVersion)
	userSQL := append(kvs, descriptor(start), descriptor(start+1), descriptor(start+5))
	// Real system tables and partitioned user tables.
	var subzoneSQL = make([]roachpb.KeyValue, len(userSQL))
	copy(subzoneSQL, userSQL)
	subzoneSQL = append(subzoneSQL,
		zoneConfig(start+1, subzone("a", ""), subzone("c", "e")),
		zoneConfig(start+5, subzone("b", ""), subzone("c", "d"), subzone("d", "")))

	sort.Sort(roachpb.KeyValueByKey(userSQL))
	sort.Sort(roachpb.KeyValueByKey(subzoneSQL))

	testCases := []struct {
		values     []roachpb.KeyValue
		start, end roachpb.RKey
		split      roachpb.RKey // nil to indicate no split is expected
	}{
		// No data.
		{nil, minKey, roachpb.RKeyMax, tkey(0)},
		{nil, tkey(start), roachpb.RKeyMax, nil},
		{nil, tkey(start), tkey(start + 10), nil},
		{nil, minKey, tkey(start + 10), tkey(0)},

		// Reserved descriptors.
		{baseSql, minKey, roachpb.RKeyMax, tkey(0)},
		{baseSql, tkey(start), roachpb.RKeyMax, nil},
		{baseSql, tkey(start), tkey(start + 10), nil},
		{baseSql, minKey, tkey(start + 10), tkey(0)},
		{baseSql, tkey(reservedStart), roachpb.RKeyMax, tkey(reservedStart + 1)},
		{baseSql, tkey(reservedStart), tkey(start + 10), tkey(reservedStart + 1)},
		{baseSql, minKey, tkey(reservedStart + 2), tkey(0)},
		{baseSql, minKey, tkey(reservedStart + 10), tkey(0)},
		{baseSql, tkey(reservedStart), tkey(reservedStart + 2), tkey(reservedStart + 1)},
		{baseSql, tkey(reservedStart, "foo"), tkey(start+10, "foo"), tkey(reservedStart + 1)},

		// Reserved + User descriptors.
		{userSQL, tkey(start - 1), roachpb.RKeyMax, tkey(start)},
		{userSQL, tkey(start), roachpb.RKeyMax, tkey(start + 1)},
		{userSQL, tkey(start), tkey(start + 10), tkey(start + 1)},
		{userSQL, tkey(start - 1), tkey(start + 10), tkey(start)},
		{userSQL, tkey(start + 4), tkey(start + 10), tkey(start + 5)},
		{userSQL, tkey(start + 5), tkey(start + 10), nil},
		{userSQL, tkey(start + 6), tkey(start + 10), nil},
		{userSQL, tkey(start, "foo"), tkey(start + 10), tkey(start + 1)},
		{userSQL, tkey(start, "foo"), tkey(start + 5), tkey(start + 1)},
		{userSQL, tkey(start, "foo"), tkey(start+5, "bar"), tkey(start + 1)},
		{userSQL, tkey(start, "foo"), tkey(start, "morefoo"), nil},
		{userSQL, minKey, roachpb.RKeyMax, tkey(0)},
		{userSQL, tkey(reservedStart + 1), roachpb.RKeyMax, tkey(reservedStart + 2)},
		{userSQL, tkey(reservedStart), tkey(start + 10), tkey(reservedStart + 1)},
		{userSQL, minKey, tkey(start + 2), tkey(0)},
		{userSQL, tkey(reservedStart, "foo"), tkey(start+5, "foo"), tkey(reservedStart + 1)},

		// Partitioned user descriptors.
		{subzoneSQL, tkey(start), roachpb.RKeyMax, tkey(start + 1)},
		{subzoneSQL, tkey(start), tkey(start + 1), nil},
		{subzoneSQL, tkey(start + 1), tkey(start + 2), tkey(start+1, "a")},
		{subzoneSQL, tkey(start+1, "a"), tkey(start + 2), tkey(start+1, "b")},
		{subzoneSQL, tkey(start+1, "b"), tkey(start + 2), tkey(start+1, "c")},
		{subzoneSQL, tkey(start+1, "b"), tkey(start+1, "c"), nil},
		{subzoneSQL, tkey(start+1, "ba"), tkey(start+1, "bb"), nil},
		{subzoneSQL, tkey(start+1, "c"), tkey(start + 2), tkey(start+1, "e")},
		{subzoneSQL, tkey(start+1, "e"), tkey(start + 2), nil},
		{subzoneSQL, tkey(start + 4), tkey(start + 6), tkey(start + 5)},
		{subzoneSQL, tkey(start + 5), tkey(start + 5), nil},
		{subzoneSQL, tkey(start + 5), tkey(start + 6), tkey(start+5, "b")},
		{subzoneSQL, tkey(start+5, "a"), tkey(start+5, "ae"), nil},
		{subzoneSQL, tkey(start+5, "b"), tkey(start + 6), tkey(start+5, "c")},
		{subzoneSQL, tkey(start+5, "c"), tkey(start + 6), tkey(start+5, "d")},
		{subzoneSQL, tkey(start+5, "d"), tkey(start + 6), tkey(start+5, "e")},
		{subzoneSQL, tkey(start+5, "e"), tkey(start + 6), nil},

		// Testing that no splits are required for IDs that
		// that do not map to descriptors.
		{userSQL, tkey(start + 1), tkey(start + 5), nil},
	}

	cfg := config.NewSystemConfig(config.DefaultZoneConfigRef())
	for tcNum, tc := range testCases {
		cfg.Values = tc.values
		splitKey := cfg.ComputeSplitKey(tc.start, tc.end)
		if !splitKey.Equal(tc.split) {
			t.Errorf("#%d: bad split:\ngot: %v\nexpected: %v", tcNum, splitKey, tc.split)
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
		{roachpb.RKey(keys.NodeLivenessPrefix), keys.LivenessRangesID},
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

		// Gossiped system tables should refer to the SystemDatabaseID.
		{tkey(keys.ZonesTableID), keys.SystemDatabaseID},

		// Non-gossiped system tables should refer to themselves.
		{tkey(keys.LeaseTableID), keys.LeaseTableID},
		{tkey(keys.JobsTableID), keys.JobsTableID},
		{tkey(keys.LocationsTableID), keys.LocationsTableID},
		{tkey(keys.NamespaceTableID), keys.NamespaceTableID},

		// Pseudo-tables should refer to the SystemDatabaseID.
		{tkey(keys.MetaRangesID), keys.SystemDatabaseID},
		{tkey(keys.SystemRangesID), keys.SystemDatabaseID},
		{tkey(keys.TimeseriesRangesID), keys.SystemDatabaseID},
		{tkey(keys.LivenessRangesID), keys.SystemDatabaseID},

		// User tables should refer to themselves.
		{tkey(keys.MinUserDescID), keys.MinUserDescID},
		{tkey(keys.MinUserDescID + 22), keys.MinUserDescID + 22},
		{roachpb.RKeyMax, keys.RootNamespaceID},
	}

	originalZoneConfigHook := config.ZoneConfigHook
	defer func() {
		config.ZoneConfigHook = originalZoneConfigHook
	}()
	cfg := config.NewSystemConfig(config.DefaultZoneConfigRef())

	kvs, _ /* splits */ := sqlbase.MakeMetadataSchema(cfg.DefaultZoneConfig, config.DefaultSystemZoneConfigRef()).GetInitialValues(cluster.TestingClusterVersion)
	cfg.SystemConfigEntries = config.SystemConfigEntries{
		Values: kvs,
	}
	for tcNum, tc := range testCases {
		var objectID uint32
		config.ZoneConfigHook = func(
			_ *config.SystemConfig, id uint32,
		) (*config.ZoneConfig, *config.ZoneConfig, bool, error) {
			objectID = id
			return &config.ZoneConfig{}, nil, false, nil
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
