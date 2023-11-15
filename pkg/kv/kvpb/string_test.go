// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	// Hook up the pretty printer.
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestBatchRequestString(t *testing.T) {
	ba := kvpb.BatchRequest{}
	txn := roachpb.MakeTransaction(
		"test",
		nil, // baseKey
		isolation.Serializable,
		roachpb.NormalUserPriority,
		hlc.Timestamp{}, // now
		0,               // maxOffsetNs
		99,              // coordinatorNodeID
		0,
	)
	txn.ID = uuid.NamespaceDNS
	ba.Txn = &txn
	ba.WaitPolicy = lock.WaitPolicy_Error
	ba.AmbiguousReplayProtection = true
	ba.CanForwardReadTimestamp = true
	ba.BoundedStaleness = &kvpb.BoundedStalenessHeader{
		MinTimestampBound:       hlc.Timestamp{WallTime: 1},
		MinTimestampBoundStrict: true,
		MaxTimestampBound:       hlc.Timestamp{WallTime: 2},
	}
	for i := 0; i < 100; i++ {
		var ru kvpb.RequestUnion
		ru.MustSetInner(&kvpb.GetRequest{})
		ba.Requests = append(ba.Requests, ru)
	}
	var ru kvpb.RequestUnion
	ru.MustSetInner(&kvpb.EndTxnRequest{})
	ba.Requests = append(ba.Requests, ru)

	{
		exp := `Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min], Get [/Min],... 76 skipped ..., Get [/Min], Get [/Min], Get [/Min], Get [/Min], EndTxn(abort) [/Min], [txn: 6ba7b810], [wait-policy: Error], [protect-ambiguous-replay], [can-forward-ts], [bounded-staleness, min_ts_bound: 0.000000001,0, min_ts_bound_strict, max_ts_bound: 0.000000002,0]`
		act := ba.String()
		require.Equal(t, exp, act)
	}
}

func TestReplicaUnavailableError(t *testing.T) {
	ctx := context.Background()
	rDesc := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 2, ReplicaID: 3}
	var set roachpb.ReplicaSet
	set.AddReplica(rDesc)
	desc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax, set)

	errSlowProposal := errors.New("slow proposal")
	var err = kvpb.NewReplicaUnavailableError(errSlowProposal, desc, rDesc)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))
	// Sanity check that Unwrap() was implemented.
	require.True(t, errors.Is(err, errSlowProposal), "%+v", err)
	require.True(t, errors.HasType(err, (*kvpb.ReplicaUnavailableError)(nil)), "%+v", err)

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "replica_unavailable_error.txt"))
}

func TestAmbiguousResultError(t *testing.T) {
	ctx := context.Background()

	wrapped := errors.Errorf("boom with a %s", redact.Unsafe("secret"))
	var err error = kvpb.NewAmbiguousResultError(wrapped)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))
	require.True(t, errors.Is(err, wrapped), "%+v", err)

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "ambiguous_result_error.txt"))
}

// Unit test the requests that implemented SafeFormatterRequest interface.
func TestRequestSafeFormat(t *testing.T) {
	gr := &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("a"),
		},
		KeyLockingStrength:   lock.Shared,
		KeyLockingDurability: lock.Unreplicated,
	}
	require.EqualValues(t, "Get(Shared,Unreplicated)", redact.Sprint(gr))
	require.EqualValues(t, "Get(Shared,Unreplicated)", redact.Sprint(gr).Redact())

	sr := &kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("b"),
			EndKey: roachpb.Key("d"),
		},
		KeyLockingStrength:   lock.Shared,
		KeyLockingDurability: lock.Unreplicated,
	}
	require.EqualValues(t, "Scan(Shared,Unreplicated)", redact.Sprint(sr))
	require.EqualValues(t, "Scan(Shared,Unreplicated)", redact.Sprint(sr).Redact())

	rsr := &kvpb.ReverseScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key("c"),
			EndKey: roachpb.Key("d"),
		},
		KeyLockingStrength:   lock.Shared,
		KeyLockingDurability: lock.Unreplicated,
	}
	require.EqualValues(t, "ReverseScan(Shared,Unreplicated)", redact.Sprint(rsr))
	require.EqualValues(t, "ReverseScan(Shared,Unreplicated)", redact.Sprint(rsr).Redact())

	etr := &kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("ab"),
		},
		Commit: true,
	}
	require.EqualValues(t, "EndTxn(commit)", redact.Sprint(etr))
	require.EqualValues(t, "EndTxn(commit)", redact.Sprint(etr).Redact())

	txn := roachpb.MakeTransaction("txn1", []byte("abc"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 6, 0)
	fixed_uuid, _ := uuid.FromString("00fbff57-c1ee-48ce-966c-da568d50e425")
	txn.ID = fixed_uuid
	rtr := &kvpb.RecoverTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("abc"),
		},
		Txn:                 txn.TxnMeta,
		ImplicitlyCommitted: false,
	}
	require.EqualValues(t, "RecoverTxn(00fbff57, abort)", redact.Sprint(rtr))
	require.EqualValues(t, "RecoverTxn(00fbff57, abort)", redact.Sprint(rtr).Redact())

	pusherTxn := roachpb.MakeTransaction("txn2", []byte("123"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 1, 0)
	fixed_uuid2, _ := uuid.FromString("00fbff58-c1ee-48ce-966c-da568d50e425")
	pusherTxn.ID = fixed_uuid2
	pusheeTxn := roachpb.MakeTransaction("txn3", []byte("1234"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 1, 0)
	fixed_uuid3, _ := uuid.FromString("00fbff59-c1ee-48ce-966c-da568d50e425")
	pusheeTxn.ID = fixed_uuid3
	ptr := &kvpb.PushTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: roachpb.Key("123"),
		},
		PusherTxn: pusherTxn,
		PusheeTxn: pusheeTxn.TxnMeta,
	}
	require.EqualValues(t, "PushTxn(‹PUSH_TIMESTAMP›,00fbff58->00fbff59)", redact.Sprint(ptr))
	require.EqualValues(t, "PushTxn(‹×›,00fbff58->00fbff59)", redact.Sprint(ptr).Redact())
}
