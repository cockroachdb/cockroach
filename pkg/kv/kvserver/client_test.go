// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
	Package storage_test provides a means of testing store

functionality which depends on a fully-functional KV client. This
cannot be done within the storage package because of circular
dependencies.

By convention, tests in package storage_test have names of the form
client_*.go.
*/
package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// getArgs returns a GetRequest for the specified key.
func getArgs(key roachpb.Key) *kvpb.GetRequest {
	return &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	}
}

// scanArgs returns a ScanRequest for the specified key and end key.
func scanArgs(key, endKey roachpb.Key) *kvpb.ScanRequest {
	return &kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
	}
}

// revScanArgs returns a ReverseScanRequest for the specified key and end key.
func revScanArgs(key, endKey roachpb.Key) *kvpb.ReverseScanRequest {
	return &kvpb.ReverseScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
	}
}

// putArgs returns a PutRequest for the specified key / value.
func putArgs(key roachpb.Key, value []byte) *kvpb.PutRequest {
	return &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: key},
		Value:         roachpb.MakeValueFromBytes(value),
	}
}

// cPutArgs returns a ConditionPutRequest to the default replica
// for the specified key and value, with the given expected value.
func cPutArgs(key roachpb.Key, value, expValue []byte) *kvpb.ConditionalPutRequest {
	var expBytes []byte
	if expValue != nil {
		expBytes = roachpb.MakeValueFromBytes(expValue).TagAndDataBytes()
	}

	return &kvpb.ConditionalPutRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpBytes: expBytes,
	}
}

// incrementArgs returns an IncrementRequest addressed to the default replica
// for the specified key.
func incrementArgs(key roachpb.Key, inc int64) *kvpb.IncrementRequest {
	return &kvpb.IncrementRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

// delRangeArgs returns a DeleteRangeRequest for the specified span.
func delRangeArgs(
	key roachpb.Key, endKey roachpb.Key, useRangeTombstone bool,
) *kvpb.DeleteRangeRequest {
	return &kvpb.DeleteRangeRequest{
		RequestHeader:     kvpb.RequestHeader{Key: key, EndKey: endKey},
		UseRangeTombstone: useRangeTombstone,
	}
}

func truncateLogArgs(index kvpb.RaftIndex, rangeID roachpb.RangeID) *kvpb.TruncateLogRequest {
	return &kvpb.TruncateLogRequest{
		Index:   index,
		RangeID: rangeID,
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (*kvpb.HeartbeatTxnRequest, kvpb.Header) {
	return &kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	}, kvpb.Header{Txn: txn}
}

func endTxnArgs(txn *roachpb.Transaction, commit bool) (*kvpb.EndTxnRequest, kvpb.Header) {
	return &kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key, // not allowed when going through TxnCoordSender, but we're not
		},
		Commit: commit,
	}, kvpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType kvpb.PushTxnType,
) *kvpb.PushTxnRequest {
	return &kvpb.PushTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: pushee.Key,
		},
		PushTo:    pusher.WriteTimestamp.Next(),
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func migrateArgs(start, end roachpb.Key, version roachpb.Version) *kvpb.MigrateRequest {
	return &kvpb.MigrateRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
		Version: version,
	}
}

func adminTransferLeaseArgs(key roachpb.Key, target roachpb.StoreID) kvpb.Request {
	return &kvpb.AdminTransferLeaseRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Target: target,
	}
}

func assertRangeStats(
	t *testing.T, name string, r storage.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats,
) {
	t.Helper()

	ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), r)
	require.NoError(t, err)
	// When used with a real wall clock these will not be the same, since it
	// takes time to load stats.
	expMS.AgeTo(ms.LastUpdateNanos)
	// Clear system counts as these are expected to vary.
	ms.SysBytes, ms.SysCount, ms.AbortSpanBytes = 0, 0, 0
	require.Equal(t, expMS, ms, "%s: stats differ", name)
}

func assertRecomputedStats(
	t *testing.T,
	name string,
	r storage.Reader,
	desc *roachpb.RangeDescriptor,
	expMS enginepb.MVCCStats,
	nowNanos int64,
) {
	t.Helper()

	ms, err := rditer.ComputeStatsForRange(context.Background(), desc, r, nowNanos)
	require.NoError(t, err)

	// When used with a real wall clock these will not be the same, since it
	// takes time to load stats.
	expMS.AgeTo(ms.LastUpdateNanos)
	// Recomputing stats always has ContainsEstimates = 0, while on-disk stats may
	// have a non-zero value. ContainsEstimates should be asserted separately.
	ms.ContainsEstimates = expMS.ContainsEstimates
	require.Equal(t, expMS, ms, "%s: recomputed stats diverge", name)
}

func waitForTombstone(
	t *testing.T, reader storage.Reader, rangeID roachpb.RangeID,
) (tombstone kvserverpb.RangeTombstone) {
	testutils.SucceedsSoon(t, func() error {
		tombstoneKey := keys.RangeTombstoneKey(rangeID)
		ok, err := storage.MVCCGetProto(
			context.Background(), reader, tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
		)
		if err != nil {
			t.Fatalf("failed to read tombstone: %v", err)
		}
		if !ok {
			return fmt.Errorf("tombstone not found for range %d", rangeID)
		}
		return nil
	})
	return tombstone
}
