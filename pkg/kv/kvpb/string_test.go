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
		roachpb.NormalUserPriority,
		hlc.Timestamp{}, // now
		0,               // maxOffsetNs
		99,              // coordinatorNodeID
	)
	txn.ID = uuid.NamespaceDNS
	ba.Txn = &txn
	ba.WaitPolicy = lock.WaitPolicy_Error
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
		exp := `Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min),... 76 skipped ..., Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), Get [/Min,/Min), EndTxn(abort) [/Min], [txn: 6ba7b810], [wait-policy: Error], [can-forward-ts], [bounded-staleness, min_ts_bound: 0.000000001,0, min_ts_bound_strict, max_ts_bound: 0.000000002,0]`
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
