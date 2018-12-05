// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Test that EvalContext.GetClusterTimestamp() gets its timestamp from the
// transaction, and also that the conversion to decimal works properly.
func TestClusterTimestampConversion(t *testing.T) {
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

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	senderFactory := client.MakeMockTxnSenderFactory(
		func(context.Context, *roachpb.Transaction, roachpb.BatchRequest,
		) (*roachpb.BatchResponse, *roachpb.Error) {
			panic("unused")
		})
	db := client.NewDB(
		testutils.MakeAmbientCtx(),
		senderFactory,
		clock)

	for _, d := range testData {
		ts := hlc.Timestamp{WallTime: d.walltime, Logical: d.logical}
		ctx := tree.EvalContext{
			Txn: client.NewTxnWithProto(
				context.Background(),
				db,
				1, /* gatewayNodeID */
				client.RootTxn,
				roachpb.MakeTransaction(
					"test",
					nil, // baseKey
					roachpb.NormalUserPriority,
					ts,
					0, /* maxOffsetNs */
				),
			),
		}

		dec := ctx.GetClusterTimestamp()
		final := dec.Text('f')
		if final != d.expected {
			t.Errorf("expected %s, but found %s", d.expected, final)
		}
	}
}
