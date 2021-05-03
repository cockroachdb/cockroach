// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/* Package storage_test provides a means of testing store
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key roachpb.Key) *roachpb.GetRequest {
	return &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key roachpb.Key, value []byte) *roachpb.PutRequest {
	return &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

// cPutArgs returns a ConditionPutRequest to the default replica
// for the specified key and value, with the given expected value.
func cPutArgs(key roachpb.Key, value, expValue []byte) *roachpb.ConditionalPutRequest {
	var expBytes []byte
	if expValue != nil {
		expBytes = roachpb.MakeValueFromBytes(expValue).TagAndDataBytes()
	}

	return &roachpb.ConditionalPutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpBytes: expBytes,
	}
}

// incrementArgs returns an IncrementRequest addressed to the default replica
// for the specified key.
func incrementArgs(key roachpb.Key, inc int64) *roachpb.IncrementRequest {
	return &roachpb.IncrementRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

func truncateLogArgs(index uint64, rangeID roachpb.RangeID) *roachpb.TruncateLogRequest {
	return &roachpb.TruncateLogRequest{
		Index:   index,
		RangeID: rangeID,
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (*roachpb.HeartbeatTxnRequest, roachpb.Header) {
	return &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	}, roachpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType,
) *roachpb.PushTxnRequest {
	return &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: pushee.Key,
		},
		PushTo:    pusher.WriteTimestamp.Next(),
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func migrateArgs(start, end roachpb.Key, version roachpb.Version) *roachpb.MigrateRequest {
	return &roachpb.MigrateRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
		Version: version,
	}
}

func adminTransferLeaseArgs(key roachpb.Key, target roachpb.StoreID) roachpb.Request {
	return &roachpb.AdminTransferLeaseRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Target: target,
	}
}

func verifyRangeStats(
	reader storage.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats,
) error {
	ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), reader)
	if err != nil {
		return err
	}
	// When used with a real wall clock these will not be the same, since it
	// takes time to load stats.
	expMS.AgeTo(ms.LastUpdateNanos)
	// Clear system counts as these are expected to vary.
	ms.SysBytes, ms.SysCount, ms.AbortSpanBytes = 0, 0, 0
	if ms != expMS {
		return errors.Errorf("expected and actual stats differ:\n%s", pretty.Diff(expMS, ms))
	}
	return nil
}

func verifyRecomputedStats(
	reader storage.Reader, d *roachpb.RangeDescriptor, expMS enginepb.MVCCStats, nowNanos int64,
) error {
	ms, err := rditer.ComputeStatsForRange(d, reader, nowNanos)
	if err != nil {
		return err
	}
	// When used with a real wall clock these will not be the same, since it
	// takes time to load stats.
	expMS.AgeTo(ms.LastUpdateNanos)
	if expMS != ms {
		return fmt.Errorf("expected range's stats to agree with recomputation: got\n%+v\nrecomputed\n%+v", expMS, ms)
	}
	return nil
}

func waitForTombstone(
	t *testing.T, reader storage.Reader, rangeID roachpb.RangeID,
) (tombstone roachpb.RangeTombstone) {
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
