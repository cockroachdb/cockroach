// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Test that EvalContext.GetClusterTimestamp() gets its timestamp from the
// transaction, and also that the conversion to decimal works properly.
func TestClusterTimestampConversion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testData := []struct {
		walltime int64
		logical  int32
		expected string
	}{
		{42, 0, "42.0000000000"},
		{-42, 0, "-42.0000000000"},
		{42, 69, "42.0000000069"},
		{42, 2147483647, "42.2147483647"},
		{9223372036854775807, 2147483647, "9223372036854775807.2147483647"},
	}

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	senderFactory := kv.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			panic("unused")
		})
	db := kv.NewDB(testutils.MakeAmbientCtx(), senderFactory, clock, stopper)

	for _, d := range testData {
		ts := hlc.ClockTimestamp{WallTime: d.walltime, Logical: d.logical}
		txnProto := roachpb.MakeTransaction(
			"test",
			nil, // baseKey
			roachpb.NormalUserPriority,
			ts.ToTimestamp(),
			0, /* maxOffsetNs */
		)

		ctx := tree.EvalContext{
			Txn: kv.NewTxnFromProto(
				context.Background(),
				db,
				1, /* gatewayNodeID */
				ts,
				kv.RootTxn,
				&txnProto,
			),
		}

		dec := ctx.GetClusterTimestamp()
		final := dec.Text('f')
		if final != d.expected {
			t.Errorf("expected %s, but found %s", d.expected, final)
		}
	}
}
