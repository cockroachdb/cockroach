// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package followerreadsccl

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan/replicaoracle"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	defaultInterval                          = 3
	defaultFraction                          = .2
	defaultMultiple                          = 3
	expectedFollowerReadOffset time.Duration = 1e9 * /* 1 second */
		-defaultInterval * (1 + defaultFraction*defaultMultiple)
)

func TestEvalFollowerReadOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	if offset, err := evalFollowerReadOffset(uuid.MakeV4(), st); err != nil {
		t.Fatal(err)
	} else if offset != expectedFollowerReadOffset {
		t.Fatalf("expected %v, got %v", expectedFollowerReadOffset, offset)
	}
	disableEnterprise()
	_, err := evalFollowerReadOffset(uuid.MakeV4(), st)
	if !testutils.IsError(err, "requires an enterprise license") {
		t.Fatalf("failed to get error when evaluating follower read offset without " +
			"an enterprise license")
	}
}

func TestCanSendToFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	kvserver.FollowerReadsEnabled.Override(&st.SV, true)

	old := hlc.Timestamp{
		WallTime: timeutil.Now().Add(2 * expectedFollowerReadOffset).UnixNano(),
	}
	oldHeader := roachpb.Header{Txn: &roachpb.Transaction{
		ReadTimestamp: old,
	}}
	rw := roachpb.BatchRequest{Header: oldHeader}
	rw.Add(&roachpb.PutRequest{})
	if canSendToFollower(uuid.MakeV4(), st, rw) {
		t.Fatalf("should not be able to send a rw request to a follower")
	}
	roNonTxn := roachpb.BatchRequest{Header: oldHeader}
	roNonTxn.Add(&roachpb.QueryTxnRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roNonTxn) {
		t.Fatalf("should not be able to send a non-transactional ro request to a follower")
	}
	roNoTxn := roachpb.BatchRequest{}
	roNoTxn.Add(&roachpb.GetRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roNoTxn) {
		t.Fatalf("should not be able to send a batch with no txn to a follower")
	}
	roOld := roachpb.BatchRequest{Header: oldHeader}
	roOld.Add(&roachpb.GetRequest{})
	if !canSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should be able to send an old ro batch to a follower")
	}
	roRWTxnOld := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			TxnMeta:       enginepb.TxnMeta{Key: []byte("key")},
			ReadTimestamp: old,
		},
	}}
	roRWTxnOld.Add(&roachpb.GetRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roRWTxnOld) {
		t.Fatalf("should not be able to send a ro request from a rw txn to a follower")
	}
	kvserver.FollowerReadsEnabled.Override(&st.SV, false)
	if canSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should not be able to send an old ro batch to a follower when follower reads are disabled")
	}
	kvserver.FollowerReadsEnabled.Override(&st.SV, true)
	roNew := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			ReadTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		},
	}}
	if canSendToFollower(uuid.MakeV4(), st, roNew) {
		t.Fatalf("should not be able to send a new ro batch to a follower")
	}
	roOldWithNewMax := roachpb.BatchRequest{Header: roachpb.Header{
		Txn: &roachpb.Transaction{
			MaxTimestamp: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		},
	}}
	roOldWithNewMax.Add(&roachpb.GetRequest{})
	if canSendToFollower(uuid.MakeV4(), st, roNew) {
		t.Fatalf("should not be able to send a ro batch with new MaxTimestamp to a follower")
	}
	disableEnterprise()
	if canSendToFollower(uuid.MakeV4(), st, roOld) {
		t.Fatalf("should not be able to send an old ro batch to a follower without enterprise enabled")
	}
}

func TestFollowerReadMultipleValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic from setting followerReadMultiple to .1")
		}
	}()
	st := cluster.MakeTestingClusterSettings()
	followerReadMultiple.Override(&st.SV, .1)
}

// TestOracle tests the OracleFactory exposed by this package.
// This test ends up being rather indirect but works by checking if the type
// of the oracle returned from the factory differs between requests we'd
// expect to support follower reads and that which we'd expect not to.
func TestOracleFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableEnterprise := utilccl.TestingEnableEnterprise()
	defer disableEnterprise()
	st := cluster.MakeTestingClusterSettings()
	kvserver.FollowerReadsEnabled.Override(&st.SV, true)
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	c := kv.NewDB(log.AmbientContext{
		Tracer: tracing.NewTracer(),
	}, kv.MockTxnSenderFactory{},
		hlc.NewClock(hlc.UnixNano, time.Nanosecond))
	txn := kv.NewTxn(context.Background(), c, 0)
	of := replicaoracle.NewOracleFactory(followerReadAwareChoice, replicaoracle.Config{
		Settings:   st,
		RPCContext: rpcContext,
	})
	noFollowerReadOracle := of.Oracle(txn)
	old := hlc.Timestamp{
		WallTime: timeutil.Now().Add(2 * expectedFollowerReadOffset).UnixNano(),
	}
	txn.SetFixedTimestamp(context.Background(), old)
	followerReadOracle := of.Oracle(txn)
	if reflect.TypeOf(followerReadOracle) == reflect.TypeOf(noFollowerReadOracle) {
		t.Fatalf("expected types of %T and %T to differ", followerReadOracle,
			noFollowerReadOracle)
	}
	disableEnterprise()
	disabledFollowerReadOracle := of.Oracle(txn)
	if reflect.TypeOf(disabledFollowerReadOracle) != reflect.TypeOf(noFollowerReadOracle) {
		t.Fatalf("expected types of %T and %T not to differ", disabledFollowerReadOracle,
			noFollowerReadOracle)
	}
}
