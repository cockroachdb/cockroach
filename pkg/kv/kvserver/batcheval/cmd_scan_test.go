// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestScanReverseScanTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Sanity checks for the TargetBytes scan option. We're not checking the specifics here, just
	// that the plumbing works. TargetBytes is tested in-depth via TestMVCCHistories.

	const (
		tbNone = 0      // no limit, i.e. should return all kv pairs
		tbOne  = 1      // one byte = return first key only
		tbLots = 100000 // de facto ditto tbNone
	)
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		for _, tb := range []int64{tbNone, tbOne, tbLots} {
			t.Run(fmt.Sprintf("targetBytes=%d", tb), func(t *testing.T) {
				for _, sf := range []roachpb.ScanFormat{roachpb.KEY_VALUES, roachpb.BATCH_RESPONSE} {
					t.Run(fmt.Sprintf("format=%s", sf), func(t *testing.T) {
						testScanReverseScanInner(t, tb, sf, reverse, tb != tbOne)
					})
				}
			})
		}
	})
}

func testScanReverseScanInner(
	t *testing.T, tb int64, sf roachpb.ScanFormat, reverse bool, expBoth bool,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k1, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts := hlc.Timestamp{WallTime: 1}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write to k1 and k2.
	for _, k := range []roachpb.Key{k1, k2} {
		err := storage.MVCCPut(ctx, eng, nil, k, ts, roachpb.MakeValueFromString("value-"+string(k)), nil)
		require.NoError(t, err)
	}

	var req roachpb.Request
	var resp roachpb.Response
	if !reverse {
		req = &roachpb.ScanRequest{ScanFormat: sf}
		resp = &roachpb.ScanResponse{}
	} else {
		req = &roachpb.ReverseScanRequest{ScanFormat: sf}
		resp = &roachpb.ReverseScanResponse{}
	}
	req.SetHeader(roachpb.RequestHeader{Key: k1, EndKey: roachpb.KeyMax})

	cArgs := CommandArgs{
		Args: req,
		Header: roachpb.Header{
			Timestamp:   ts,
			TargetBytes: tb,
		},
		EvalCtx: (&MockEvalCtx{ClusterSettings: cluster.MakeClusterSettings()}).EvalContext(),
	}

	if !reverse {
		_, err := Scan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	} else {
		_, err := ReverseScan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	}
	expN := 1
	if expBoth {
		expN = 2
	}

	require.EqualValues(t, expN, resp.Header().NumKeys)
	require.NotZero(t, resp.Header().NumBytes)

	var rows []roachpb.KeyValue
	if !reverse {
		rows = resp.(*roachpb.ScanResponse).Rows
	} else {
		rows = resp.(*roachpb.ReverseScanResponse).Rows
	}

	if rows != nil {
		require.Len(t, rows, expN)
	}
}
