// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	// Hook up the pretty printer.
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestTransactionString(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	txn := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            roachpb.Key("foo"),
			ID:             txnID,
			Epoch:          2,
			WriteTimestamp: hlc.Timestamp{WallTime: 20, Logical: 21},
			MinTimestamp:   hlc.Timestamp{WallTime: 10, Logical: 11},
			Priority:       957356782,
			Sequence:       15,
		},
		Name:                   "name",
		Status:                 roachpb.COMMITTED,
		LastHeartbeat:          hlc.Timestamp{WallTime: 10, Logical: 11},
		ReadTimestamp:          hlc.Timestamp{WallTime: 30, Logical: 31},
		GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 40, Logical: 41, Synthetic: true},
	}
	expStr := `"name" meta={id=d7aa0f5e key="foo" pri=44.58039917 epo=2 ts=0.000000020,21 min=0.000000010,11 seq=15}` +
		` lock=true stat=COMMITTED rts=0.000000030,31 wto=false gul=0.000000040,41?`

	if str := txn.String(); str != expStr {
		t.Errorf(
			"expected txn: %s\n"+
				"got:          %s",
			expStr, str)
	}
}

func TestBatchRequestString(t *testing.T) {
	ba := roachpb.BatchRequest{}
	txn := roachpb.MakeTransaction(
		"test",
		nil, // baseKey
		roachpb.NormalUserPriority,
		hlc.Timestamp{}, // now
		0,               // maxOffsetNs
		99,              // coordinatorNodeID
	)
	txn.ID = uuid.NamespaceDNS
	ba.Txn = &txn
	ba.WaitPolicy = lock.WaitPolicy_Error
	ba.CanForwardReadTimestamp = true
	ba.BoundedStaleness = &roachpb.BoundedStalenessHeader{
		MinTimestampBound:       hlc.Timestamp{WallTime: 1},
		MinTimestampBoundStrict: true,
		MaxTimestampBound:       hlc.Timestamp{WallTime: 2},
	}
	for i := 0; i < 100; i++ {
		var ru roachpb.RequestUnion
		ru.MustSetInner(&roachpb.GetRequest{})
		ba.Requests = append(ba.Requests, ru)
	}
	var ru roachpb.RequestUnion
	ru.MustSetInner(&roachpb.EndTxnRequest{})
	ba.Requests = append(ba.Requests, ru)

	{
		exp := `Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min),... 76 skipped ..., Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), EndTxn(abort) [/Min], [txn: 6ba7b810], [wait-policy: Error], [can-forward-ts], [bounded-staleness, min_ts_bound: 0.000000001,0, min_ts_bound_strict, max_ts_bound: 0.000000002,0]`
		act := ba.String()
		require.Equal(t, exp, act)
	}
}

func TestKeyString(t *testing.T) {
	require.Equal(t,
		`/Table/53/42/"=\xbc ⌘"`,
		roachpb.Key("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98").String())
}

func TestRangeDescriptorStringRedact(t *testing.T) {
	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("c"),
		EndKey:   roachpb.RKey("g"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}

	require.EqualValues(t,
		`r1:‹{c-g}› [(n1,s1):?, (n2,s2):?, (n3,s3):?, next=0, gen=0]`,
		redact.Sprint(desc),
	)
}

func TestSpansString(t *testing.T) {
	for _, tc := range []struct {
		spans    roachpb.Spans
		expected string
	}{
		{
			spans:    roachpb.Spans{},
			expected: "",
		},
		{
			spans:    roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
			expected: "{a-b}",
		},
		{
			spans:    roachpb.Spans{{Key: roachpb.Key("a")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			expected: "a, {c-d}",
		},
	} {
		require.Equal(t, tc.expected, tc.spans.String())
	}
}

func TestReplicaUnavailableError(t *testing.T) {
	ctx := context.Background()
	rDesc := roachpb.ReplicaDescriptor{NodeID: 1, StoreID: 2, ReplicaID: 3}
	var set roachpb.ReplicaSet
	set.AddReplica(rDesc)
	desc := roachpb.NewRangeDescriptor(123, roachpb.RKeyMin, roachpb.RKeyMax, set)

	errSlowProposal := errors.New("slow proposal")
	var err = roachpb.NewReplicaUnavailableError(errSlowProposal, desc, rDesc)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))
	// Sanity check that Unwrap() was implemented.
	require.True(t, errors.Is(err, errSlowProposal), "%+v", err)
	require.True(t, errors.HasType(err, (*roachpb.ReplicaUnavailableError)(nil)), "%+v", err)

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "replica_unavailable_error.txt"))
}

func TestAmbiguousResultError(t *testing.T) {
	ctx := context.Background()

	wrapped := errors.Errorf("boom with a %s", redact.Unsafe("secret"))
	var err error = roachpb.NewAmbiguousResultError(wrapped)
	err = errors.DecodeError(ctx, errors.EncodeError(ctx, err))
	require.True(t, errors.Is(err, wrapped), "%+v", err)

	s := fmt.Sprintf("%s\n%s", err, redact.Sprint(err))
	echotest.Require(t, s, filepath.Join("testdata", "ambiguous_result_error.txt"))
}
