// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
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
		false, // omitInRangefeeds
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

	wrapped := errors.Errorf("boom with a %s", encoding.Unsafe("secret"))
	var err error = kvpb.NewAmbiguousResultError(wrapped)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))
	require.True(t, errors.Is(err, wrapped), "%+v", err)

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "ambiguous_result_error.txt"))
}

// Unit test the requests that implemented SafeFormatterRequest interface.
func TestRequestSafeFormat(t *testing.T) {
	txn := roachpb.MakeTransaction("txn1", []byte("abc"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 6, 0, false)
	fixedUuid, _ := uuid.FromString("00fbff57-c1ee-48ce-966c-da568d50e425")
	txn.ID = fixedUuid
	pusherTxn := roachpb.MakeTransaction("txn2", []byte("123"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 1, 0, false)
	pusheeTxn := roachpb.MakeTransaction("txn3", []byte("1234"), 0, 0, hlc.Timestamp{WallTime: 10}, 0, 1, 0, false)
	fixedUuid2, _ := uuid.FromString("00fbff58-c1ee-48ce-966c-da568d50e425")
	fixedUuid3, _ := uuid.FromString("00fbff59-c1ee-48ce-966c-da568d50e425")
	pusherTxn.ID = fixedUuid2
	pusheeTxn.ID = fixedUuid3

	testCases := []struct {
		req        kvpb.Request
		redactable string
		redacted   string
	}{
		{
			req: &kvpb.GetRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: roachpb.Key("a"),
				},
				KeyLockingStrength:   lock.Shared,
				KeyLockingDurability: lock.Unreplicated,
			},
			redactable: "Get(Shared,Unreplicated)",
			redacted:   "Get(Shared,Unreplicated)",
		},
		{
			req: &kvpb.ScanRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    roachpb.Key("b"),
					EndKey: roachpb.Key("d"),
				},
				KeyLockingStrength:   lock.Shared,
				KeyLockingDurability: lock.Unreplicated,
			},
			redactable: "Scan(Shared,Unreplicated)",
			redacted:   "Scan(Shared,Unreplicated)",
		},
		{
			req: &kvpb.ReverseScanRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    roachpb.Key("c"),
					EndKey: roachpb.Key("d"),
				},
				KeyLockingStrength:   lock.Shared,
				KeyLockingDurability: lock.Unreplicated,
			},
			redactable: "ReverseScan(Shared,Unreplicated)",
			redacted:   "ReverseScan(Shared,Unreplicated)",
		},
		{
			req: &kvpb.EndTxnRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: roachpb.Key("ab"),
				},
				Commit: true,
			},
			redactable: "EndTxn(commit)",
			redacted:   "EndTxn(commit)",
		},
		{
			req: &kvpb.RecoverTxnRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: roachpb.Key("abc"),
				},
				Txn:                 txn.TxnMeta,
				ImplicitlyCommitted: false,
			},
			redactable: "RecoverTxn(00fbff57, abort)",
			redacted:   "RecoverTxn(00fbff57, abort)",
		},
		{
			req: &kvpb.PushTxnRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: roachpb.Key("123"),
				},
				PusherTxn: pusherTxn,
				PusheeTxn: pusheeTxn.TxnMeta,
			},
			redactable: "PushTxn(PUSH_TIMESTAMP,00fbff58->00fbff59)",
			redacted:   "PushTxn(PUSH_TIMESTAMP,00fbff58->00fbff59)",
		},
	}
	for _, c := range testCases {
		t.Run(c.req.Method().String(), func(t *testing.T) {
			require.EqualValues(t, c.redactable, redact.Sprint(c.req))
			require.EqualValues(t, c.redacted, redact.Sprint(c.req).Redact())
		})
	}
}
