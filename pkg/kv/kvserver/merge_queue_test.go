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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/gogo/protobuf/proto"
)

func TestMergeQueueShouldQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	testCtx := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	testCtx.Start(t, stopper)

	mq := newMergeQueue(testCtx.store, testCtx.store.DB(), testCtx.gossip)
	kvserverbase.MergeQueueEnabled.Override(ctx, &testCtx.store.ClusterSettings().SV, true)

	tableKey := func(i uint32) []byte {
		return keys.SystemSQLCodec.TablePrefix(keys.MaxReservedDescID + i)
	}

	config.TestingSetZoneConfig(keys.MaxReservedDescID+1, *zonepb.NewZoneConfig())
	config.TestingSetZoneConfig(keys.MaxReservedDescID+2, *zonepb.NewZoneConfig())

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
			startKey: tableKey(1),
			endKey:   tableKey(2),
			minBytes: 1,
		},
		{
			startKey: append(tableKey(1), 'z'),
			endKey:   tableKey(2),
			minBytes: 1,
		},

		// Unlike the last range of table 1, the last range of table 2 is mergeable
		// because there is no table that follows. (In this test, the system only
		// knows about tables on which TestingSetZoneConfig has been called.)
		{
			startKey:    tableKey(2),
			endKey:      tableKey(3),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    append(tableKey(2), 'z'),
			endKey:      tableKey(3),
			minBytes:    1,
			expShouldQ:  true,
			expPriority: 1,
		},

		// The last range is never mergeable.
		{
			startKey: tableKey(3),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},
		{
			startKey: append(tableKey(3), 'z'),
			endKey:   roachpb.KeyMax,
			minBytes: 1,
		},

		// An interior range of a table is not mergeable if it meets or exceeds the
		// minimum byte threshold.
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       1024,
			expShouldQ:  false,
			expPriority: 0,
		},
		// Edge case: a minimum byte threshold of zero. This effectively disables
		// the threshold, as an empty range is no longer considered mergeable.
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    0,
			bytes:       0,
			expShouldQ:  false,
			expPriority: 0,
		},

		// An interior range of a table is mergeable if it does not meet the minimum
		// byte threshold. Its priority is inversely related to its size.
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       0,
			expShouldQ:  true,
			expPriority: 1,
		},
		{
			startKey:    tableKey(1),
			endKey:      append(tableKey(1), 'a'),
			minBytes:    1024,
			bytes:       768,
			expShouldQ:  true,
			expPriority: 0.25,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			repl := &Replica{}
			repl.mu.state.Desc = &roachpb.RangeDescriptor{StartKey: tc.startKey, EndKey: tc.endKey}
			repl.mu.state.Stats = &enginepb.MVCCStats{KeyBytes: tc.bytes}
			zoneConfig := zonepb.DefaultZoneConfigRef()
			zoneConfig.RangeMinBytes = proto.Int64(tc.minBytes)
			repl.SetZoneConfig(zoneConfig)
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
