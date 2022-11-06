// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	gosql "database/sql"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestApplier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		// Disable replication to avoid AdminChangeReplicas complaining about
		// replication queues being active.
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()
	sqlDB := tc.ServerConn(0)
	env := &Env{SQLDBs: []*gosql.DB{sqlDB}}

	type testCase struct {
		name string
		step Step
	}

	var sstValueHeader enginepb.MVCCValueHeader
	sstValueHeader.KVNemesisSeq.Set(1)
	sstSpan := roachpb.Span{Key: roachpb.Key(k1), EndKey: roachpb.Key(k4)}
	sstTS := hlc.Timestamp{WallTime: 1}
	sstFile := &storage.MemObject{}
	{
		st := cluster.MakeTestingClusterSettings()
		storage.ValueBlocksEnabled.Override(ctx, &st.SV, true)
		w := storage.MakeIngestionSSTWriter(ctx, st, sstFile)
		defer w.Close()

		require.NoError(t, w.PutMVCC(storage.MVCCKey{Key: roachpb.Key(k1), Timestamp: sstTS},
			storage.MVCCValue{MVCCValueHeader: sstValueHeader, Value: roachpb.MakeValueFromString("v1")}))
		require.NoError(t, w.PutMVCC(storage.MVCCKey{Key: roachpb.Key(k2), Timestamp: sstTS},
			storage.MVCCValue{MVCCValueHeader: sstValueHeader}))
		require.NoError(t, w.PutMVCCRangeKey(
			storage.MVCCRangeKey{StartKey: roachpb.Key(k3), EndKey: roachpb.Key(k4), Timestamp: sstTS},
			storage.MVCCValue{MVCCValueHeader: sstValueHeader}))
		require.NoError(t, w.Finish())
	}

	a := MakeApplier(env, db)

	tests := []testCase{
		{
			"get", step(get(k1)),
		},
		{
			"scan", step(scan(k1, k3)),
		},
		{
			"put", step(put(k1, 1)),
		},
		{
			"get-for-update", step(getForUpdate(k1)),
		},
		{
			"get-skip-locked", step(getSkipLocked(k1)),
		},
		{
			"scan-for-update", step(scanForUpdate(k1, k3)),
		},
		{
			"scan-skip-locked", step(scanSkipLocked(k1, k3)),
		},
		{
			"batch", step(batch(put(k1, 21), delRange(k2, k3, 22))),
		},
		{
			"rscan", step(reverseScan(k1, k3)),
		},
		{
			"rscan-for-update", step(reverseScanForUpdate(k1, k2)),
		},
		{
			"rscan-skip-locked", step(reverseScanSkipLocked(k1, k2)),
		},
		{
			"del", step(del(k2, 1)),
		},
		{
			"delrange", step(delRange(k1, k3, 6)),
		},
		{
			"txn-ssi-delrange", step(closureTxn(ClosureTxnType_Commit, isolation.Serializable, delRange(k2, k4, 1))),
		},
		{
			"txn-si-delrange", step(closureTxn(ClosureTxnType_Commit, isolation.Snapshot, delRange(k2, k4, 1))),
		},
		{
			"get-err", step(get(k1)),
		},
		{
			"get-for-update-err", step(getForUpdate(k1)),
		},
		{
			"get-skip-locked-err", step(getSkipLocked(k1)),
		},
		{
			"put-err", step(put(k1, 1)),
		},
		{
			"scan-for-update-err", step(scanForUpdate(k1, k3)),
		},
		{
			"scan-skip-locked-err", step(scanSkipLocked(k1, k3)),
		},
		{
			"rscan-err", step(reverseScan(k1, k3)),
		},
		{
			"rscan-for-update-err", step(reverseScanForUpdate(k1, k3)),
		},
		{
			"rscan-skip-locked-err", step(reverseScanSkipLocked(k1, k3)),
		},
		{
			"del-err", step(del(k2, 1)),
		},
		{
			"delrange-err", step(delRange(k2, k3, 12)),
		},
		{
			"txn-ssi-err", step(closureTxn(ClosureTxnType_Commit, isolation.Serializable, delRange(k2, k4, 1))),
		},
		{
			"txn-si-err", step(closureTxn(ClosureTxnType_Commit, isolation.Snapshot, delRange(k2, k4, 1))),
		},
		{
			"batch-mixed", step(batch(put(k2, 2), get(k1), del(k2, 1), del(k3, 1), scan(k1, k3), reverseScanForUpdate(k1, k5))),
		},
		{
			"batch-mixed-err", step(batch(put(k2, 2), getForUpdate(k1), scanForUpdate(k1, k3), reverseScan(k1, k3))),
		},
		{
			"txn-ssi-commit-mixed", step(closureTxn(ClosureTxnType_Commit, isolation.Serializable, put(k5, 5), batch(put(k6, 6), delRange(k3, k5, 1)))),
		},
		{
			"txn-si-commit-mixed", step(closureTxn(ClosureTxnType_Commit, isolation.Snapshot, put(k5, 5), batch(put(k6, 6), delRange(k3, k5, 1)))),
		},
		{
			"txn-ssi-commit-batch", step(closureTxnCommitInBatch(isolation.Serializable, opSlice(get(k1), put(k6, 6)), put(k5, 5))),
		},
		{
			"txn-si-commit-batch", step(closureTxnCommitInBatch(isolation.Snapshot, opSlice(get(k1), put(k6, 6)), put(k5, 5))),
		},
		{
			"txn-ssi-rollback", step(closureTxn(ClosureTxnType_Rollback, isolation.Serializable, put(k5, 5))),
		},
		{
			"txn-si-rollback", step(closureTxn(ClosureTxnType_Rollback, isolation.Snapshot, put(k5, 5))),
		},
		{
			"split", step(split(k2)),
		},
		{
			"merge", step(merge(k1)), // NB: this undoes the split at k2
		},
		{
			"split-again", step(split(k2)),
		},
		{
			"merge-again", step(merge(k1)), // ditto
		},
		{
			"transfer", step(transferLease(k6, 1)),
		},
		{
			"transfer-again", step(transferLease(k6, 1)),
		},
		{
			"zcfg", step(changeZone(ChangeZoneType_ToggleGlobalReads)),
		},
		{
			"zcfg-again", step(changeZone(ChangeZoneType_ToggleGlobalReads)),
		},
		{
			"addsstable", step(addSSTable(sstFile.Data(), sstSpan, sstTS, sstValueHeader.KVNemesisSeq.Get(), true)),
		},
		{
			"change-replicas", step(changeReplicas(k1, kvpb.ReplicationChange{ChangeType: roachpb.ADD_VOTER, Target: roachpb.ReplicationTarget{NodeID: 1, StoreID: 1}})),
		},
	}

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	defer w.Check(t)
	for _, test := range tests {
		s := test.step
		t.Run(test.name, w.Run(t, test.name, func(t *testing.T) string {
			isErr := strings.HasSuffix(test.name, "-err") || strings.HasSuffix(test.name, "-again")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if isErr {
				cancel()
			}

			var buf strings.Builder
			trace, err := a.Apply(ctx, &s)
			require.NoError(t, err)

			actual := strings.TrimLeft(s.String(), "\n")

			if isErr {
				// Trim out context canceled location, which can be non-deterministic.
				// The wrapped string around the context canceled error depends on where
				// the context cancellation was noticed.
				actual = regexp.MustCompile(` (aborted .*|txn exec): context canceled`).ReplaceAllString(actual, ` context canceled`)
			} else {
				// Trim out the txn to avoid nondeterminism.
				actual = regexp.MustCompile(` txnpb:\(.*\)`).ReplaceAllLiteralString(actual, ` txnpb:<txn>`)
				// Replace timestamps.
				actual = regexp.MustCompile(`[0-9]+\.[0-9]+,[0-9]+`).ReplaceAllLiteralString(actual, `<ts>`)
			}
			buf.WriteString(actual)

			t.Log(buf.String())
			t.Log(trace)

			return buf.String()
		}))
	}
}

func TestUpdateZoneConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		before   zonepb.ZoneConfig
		change   ChangeZoneType
		expAfter zonepb.ZoneConfig
	}{
		{
			before:   zonepb.ZoneConfig{NumReplicas: proto.Int32(3)},
			change:   ChangeZoneType_ToggleGlobalReads,
			expAfter: zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(true)},
		},
		{
			before:   zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(false)},
			change:   ChangeZoneType_ToggleGlobalReads,
			expAfter: zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(true)},
		},
		{
			before:   zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(true)},
			change:   ChangeZoneType_ToggleGlobalReads,
			expAfter: zonepb.ZoneConfig{NumReplicas: proto.Int32(3), GlobalReads: proto.Bool(false)},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			zone := test.before
			updateZoneConfig(&zone, test.change)
			require.Equal(t, test.expAfter, zone)
		})
	}
}
