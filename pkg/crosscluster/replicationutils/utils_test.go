// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func sortMVCCKVs(kvs []storage.MVCCKeyValue) {
	sort.Slice(kvs, func(i, j int) bool {
		if !kvs[i].Key.Timestamp.Equal(kvs[j].Key.Timestamp) {
			return kvs[i].Key.Timestamp.Compare(kvs[j].Key.Timestamp) < 0
		}
		return kvs[i].Key.Key.Compare(kvs[j].Key.Key) < 0
	})
}

func sortMVCCRangeKeys(rangeKey []storage.MVCCRangeKey) {
	sort.Slice(rangeKey, func(i, j int) bool {
		if !rangeKey[i].Timestamp.Equal(rangeKey[j].Timestamp) {
			return rangeKey[i].Timestamp.Compare(rangeKey[j].Timestamp) < 0
		}
		if !rangeKey[i].StartKey.Equal(rangeKey[j].StartKey) {
			return rangeKey[i].StartKey.Compare(rangeKey[j].StartKey) < 0
		}
		return rangeKey[i].EndKey.Compare(rangeKey[j].EndKey) < 0
	})
}

func TestScanSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	require.True(t, roachpb.Key("ca").Compare(roachpb.Key("c")) > 0)

	cs := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		clusterversion.MinSupported.Version(),
		true, /* initializeVersion */
	)
	data, start, end := storageutils.MakeSST(t, cs, []interface{}{
		storageutils.PointKV("ba", 30, "30"),
		storageutils.PointKV("c", 5, "5"),
		storageutils.PointKV("ca", 30, "30"),
		// Delete range from t1e - t2s, emitting t1s - t1e.
		storageutils.RangeKV("a", "c", 10, ""),
		// Delete range from t1e - t2enn, emitting t2s - t2e.
		storageutils.RangeKV("b", "cb", 15, ""),
		// Delete range for t2sn - t2en, emitting t2sn - t2e.
		storageutils.RangeKV("ca", "da", 20, ""),
		// Delete range for t3s - t3e, emitting nothing.
		storageutils.RangeKV("e", "f", 25, ""),
		storageutils.PointKVWithImportEpoch("g", 25, 1, "val"),
	})

	checkScan := func(scanWithin roachpb.Span,
		expectedPointKVs []storage.MVCCKeyValue,
		expectedRangeKeys []storage.MVCCRangeKey,
	) {
		actualPointKVs := make([]storage.MVCCKeyValue, 0, len(expectedPointKVs))
		actualRangeKVs := make([]storage.MVCCRangeKey, 0, len(expectedRangeKeys))
		require.NoError(t, ScanSST(&kvpb.RangeFeedSSTable{
			Data:    data,
			Span:    roachpb.Span{Key: start, EndKey: end},
			WriteTS: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		}, scanWithin, func(mvccKV storage.MVCCKeyValue) error {
			actualPointKVs = append(actualPointKVs, mvccKV)
			return nil
		}, func(mvccRangeKV storage.MVCCRangeKeyValue) error {
			actualRangeKVs = append(actualRangeKVs, mvccRangeKV.RangeKey)
			return nil
		}))
		sortMVCCKVs(actualPointKVs)
		sortMVCCRangeKeys(actualRangeKVs)
		require.Equal(t, expectedPointKVs, actualPointKVs)
		require.Equal(t, expectedRangeKeys, actualRangeKVs)
	}

	checkScan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
		[]storage.MVCCKeyValue{},
		[]storage.MVCCRangeKey{
			storageutils.RangeKey("a", "b", 10),
		})
	checkScan(roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")},
		[]storage.MVCCKeyValue{
			storageutils.PointKVWithImportEpoch("g", 25, 1, "val"),
		},
		[]storage.MVCCRangeKey{})

	checkScan(roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		[]storage.MVCCKeyValue{
			storageutils.PointKV("c", 5, "5"),
			storageutils.PointKV("ca", 30, "30"),
		}, []storage.MVCCRangeKey{
			storageutils.RangeKey("c", "cb", 15),
			storageutils.RangeKey("ca", "d", 20),
		})

	checkScan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")},
		[]storage.MVCCKeyValue{
			storageutils.PointKV("c", 5, "5"),
			storageutils.PointKV("ba", 30, "30"),
			storageutils.PointKV("ca", 30, "30"),
		}, []storage.MVCCRangeKey{
			storageutils.RangeKey("a", "c", 10),
			storageutils.RangeKey("b", "cb", 15),
			storageutils.RangeKey("ca", "d", 20),
		})

	checkScan(roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		[]storage.MVCCKeyValue{
			storageutils.PointKV("ba", 30, "30"),
		}, []storage.MVCCRangeKey{
			storageutils.RangeKey("a", "c", 10),
			storageutils.RangeKey("b", "c", 15),
		})

	checkScan(roachpb.Span{Key: roachpb.Key("da"), EndKey: roachpb.Key("e")},
		[]storage.MVCCKeyValue{}, []storage.MVCCRangeKey{})
}
