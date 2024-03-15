// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestMergeQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCtx := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tsc := TestStoreConfig(nil)
	testCtx.StartWithStoreConfig(ctx, t, stopper, tsc)

	mq := newMergeQueue(testCtx.store, testCtx.store.DB())
	kvserverbase.MergeQueueEnabled.Override(ctx, &testCtx.store.ClusterSettings().SV, true)

	tableKey := func(offset uint32) []byte {
		return keys.SystemSQLCodec.TablePrefix(bootstrap.TestingUserDescID(offset))
	}

	config.TestingSetZoneConfig(config.ObjectID(bootstrap.TestingUserDescID(0)), *zonepb.NewZoneConfig())
	config.TestingSetZoneConfig(config.ObjectID(bootstrap.TestingUserDescID(1)), *zonepb.NewZoneConfig())

	type testCase struct {
		startKey, endKey []byte
		minBytes         int64
		bytes            int64
		expShouldQ       bool
		expPriority      float64
	}

	testCases := []testCase{
		// The last range of table 1 should not be mergeable because table 2 exists.
		{
			startKey: tableKey(0),
			endKey:   tableKey(1),
			minBytes: 1,
		},
		{
			startKey: append(tableKey(0), 'z'),
			endKey:   tableKey(1),
			minBytes: 1,
		},

		// Unlike the last range of table 1, the last range of table 2 is mergeable
		// because there is no table that follows. (In this test, the system only
		// knows about tables on which TestingSetZoneConfig has been called.)
		{
			startKey:    tableKey(1),
			endKey:      tableKey(2),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    append(tableKey(1), 'z'),
			endKey:      tableKey(2),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},

		// The last range is never mergeable.
		{
			startKey: tableKey(2),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},
		{
			startKey: append(tableKey(2), 'z'),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},

		// An interior range of a table is not mergeable if it meets or exceeds the
		// minimum byte threshold.
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		// Edge case: a minimum byte threshold of zero. This effectively disables
		// the threshold, as an empty range is no longer considered mergeable.
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    0,
			bytes:       0,
			expShouldQ:  false,
			expPriority: 0,
		},

		// An interior range of a table is mergeable if it does not meet the minimum
		// byte threshold. Its priority is inversely related to its size.
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       0,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    tableKey(0),
			endKey:      append(tableKey(0), 'a'),
			minBytes:    1024,
			bytes:       768,
			expShouldQ:  true,
			expPriority: 0.25,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			repl := &Replica{store: testCtx.store}
			repl.mu.state.Desc = &roachpb.RangeDescriptor{StartKey: tc.startKey, EndKey: tc.endKey}
			repl.mu.state.Stats = &enginepb.MVCCStats{KeyBytes: tc.bytes}
			zoneConfig := zonepb.DefaultZoneConfigRef()
			zoneConfig.RangeMinBytes = proto.Int64(tc.minBytes)
			repl.SetSpanConfig(zoneConfig.AsSpanConfig())
			shouldQ, priority := mq.shouldQueue(ctx, hlc.ClockTimestamp{}, repl, config.NewSystemConfig(zoneConfig))
			if tc.expShouldQ != shouldQ {
				t.Errorf("incorrect shouldQ: expected %v but got %v", tc.expShouldQ, shouldQ)
			}
			if tc.expPriority != priority {
				t.Errorf("incorrect priority: expected %v but got %v", tc.expPriority, priority)
			}
		})
	}
}

// TestMergeWithExternalFiles tests that while a range has external
// file bytes, we calculate an estimate of the stats during splits.
func TestMergeShouldQueueWithExternalFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	for _, skipExternal := range []bool{true, false} {
		t.Run(fmt.Sprintf("kv.range_merge.skip_external_bytes.enabled=%v", skipExternal), func(t *testing.T) {
			ctx := context.Background()
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &StoreTestingKnobs{
						DisableMergeQueue:              true,
						DisableSplitQueue:              true,
						DisableCanAckBeforeApplication: true,
					},
				},
			})

			_, err := db.Exec("SET CLUSTER SETTING kv.range_merge.skip_external_bytes=$1", skipExternal)
			require.NoError(t, err)
			const externURI = "nodelocal://1/external-files"

			extStore, err := cloud.EarlyBootExternalStorageFromURI(ctx,
				externURI,
				base.ExternalIODirConfig{},
				s.ClusterSettings(),
				nil, /* limiters */
				cloud.NilMetrics)
			require.NoError(t, err)

			defer s.Stopper().Stop(ctx)

			scratchKey, err := s.ScratchRangeWithExpirationLease()
			require.NoError(t, err)

			lhsDesc, _, err := s.SplitRange(scratchKey.Next().Next())
			require.NoError(t, err)

			// Write an external SST the the LHS.
			fileName := "external-1.sst"
			writeKey := lhsDesc.StartKey.AsRawKey().Next()
			mvccKV := storage.MVCCKeyValue{
				Key: storage.MVCCKey{
					Key:       writeKey,
					Timestamp: hlc.Timestamp{WallTime: 1},
				},
				Value: []byte("hello"),
			}
			sst, _, _ := storageutils.MakeSST(t, s.ClusterSettings(), []interface{}{mvccKV})
			w, err := extStore.Writer(ctx, fileName)
			require.NoError(t, err)
			_, err = w.Write(sst)
			require.NoError(t, err)
			require.NoError(t, w.Close())

			size, err := extStore.Size(ctx, fileName)
			require.NoError(t, err)

			_, _, err = s.DB().AddRemoteSSTable(ctx, roachpb.Span{
				Key:    writeKey,
				EndKey: writeKey.Next(),
			}, kvpb.AddSSTableRequest_RemoteFile{
				Locator:                 externURI,
				Path:                    fileName,
				ApproximatePhysicalSize: uint64(size),
				BackingFileSize:         uint64(size),
			}, &enginepb.MVCCStats{
				ContainsEstimates: 1,
				KeyBytes:          2,
				ValBytes:          10,
				KeyCount:          2,
				LiveCount:         2,
			}, s.DB().Clock().Now())
			require.NoError(t, err)

			store, err := s.GetStores().(*Stores).GetStore(s.GetFirstStoreID())
			require.NoError(t, err)

			lhsRepl := store.LookupReplica(lhsDesc.StartKey.Next())
			mq := newMergeQueue(store, store.DB())

			zoneConfig := zonepb.DefaultZoneConfigRef()
			lhsRepl.SetSpanConfig(zoneConfig.AsSpanConfig())
			shouldq, _ := mq.shouldQueue(ctx, s.Clock().NowAsClockTimestamp(), lhsRepl, config.NewSystemConfig(zoneConfig))
			require.Equal(t, !skipExternal, shouldq)

		})
	}
}
