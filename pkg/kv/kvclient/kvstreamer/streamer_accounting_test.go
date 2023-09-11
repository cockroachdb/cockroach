// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
)

// TestStreamerMemoryAccounting performs sanity checking on the memory
// accounting done by the streamer.
func TestStreamerMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	codec := s.Codec()

	// Create a table (for which we know the encoding of valid keys) with a
	// single row.
	_, err := db.Exec("CREATE TABLE t (pk PRIMARY KEY, k) AS VALUES (0, 0)")
	require.NoError(t, err)

	// Obtain the TableID.
	r := db.QueryRow("SELECT 't'::regclass::oid")
	var tableID int
	require.NoError(t, r.Scan(&tableID))

	makeGetRequest := func(key int) kvpb.RequestUnion {
		var res kvpb.RequestUnion
		var get kvpb.GetRequest
		var union kvpb.RequestUnion_Get
		makeKey := func(pk int) []byte {
			// These numbers essentially make a key like '/t/primary/key/0'.
			return append(codec.IndexPrefix(uint32(tableID), 1), []byte{byte(136 + pk), 136}...)
		}
		get.Key = makeKey(key)
		union.Get = &get
		res.Value = &union
		return res
	}

	monitor := mon.NewMonitor(
		"streamer", /* name */
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	monitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
	defer monitor.Stop(ctx)
	acc := monitor.MakeBoundAccount()
	defer acc.Close(ctx)

	getStreamer := func(singleRowLookup bool) *Streamer {
		require.Zero(t, acc.Used())
		rootTxn := kv.NewTxn(ctx, s.DB(), s.DistSQLPlanningNodeID())
		leafInputState, err := rootTxn.GetLeafTxnInputState(ctx)
		if err != nil {
			panic(err)
		}
		s := NewStreamer(
			s.DistSenderI().(*kvcoord.DistSender),
			s.AppStopper(),
			kv.NewLeafTxn(ctx, s.DB(), s.DistSQLPlanningNodeID(), leafInputState),
			cluster.MakeTestingClusterSettings(),
			lock.WaitPolicy(0),
			math.MaxInt64,
			&acc,
			nil, /* kvPairsRead */
			nil, /* batchRequestsIssued */
			lock.None,
		)
		s.Init(OutOfOrder, Hints{UniqueRequests: true, SingleRowLookup: singleRowLookup}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
		return s
	}

	t.Run("get", func(t *testing.T) {
		acc.Clear(ctx)
		// SingleRowLookup hint only influences the accounting when at least
		// one Scan request is present.
		streamer := getStreamer(false /* singleRowLookup */)
		defer streamer.Close(ctx)

		// Get the row with pk=0.
		reqs := make([]kvpb.RequestUnion, 1)
		reqs[0] = makeGetRequest(0)
		require.NoError(t, streamer.Enqueue(ctx, reqs))
		results, err := streamer.GetResults(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, len(results))
		// 7 is the number of bytes in GetResponse.Value.RawBytes.
		var expectedMemToken = getResponseOverhead + 7
		require.Equal(t, expectedMemToken, results[0].memoryTok.toRelease)
		var expectedUsed = expectedMemToken + resultSize
		require.Equal(t, expectedUsed, acc.Used())
	})

	for _, singleRowLookup := range []bool{false, true} {
		t.Run(fmt.Sprintf("scan/single_row_lookup=%t", singleRowLookup), func(t *testing.T) {
			acc.Clear(ctx)
			streamer := getStreamer(singleRowLookup)
			defer streamer.Close(ctx)

			// Scan the row with pk=0.
			reqs := make([]kvpb.RequestUnion, 1)
			reqs[0] = makeScanRequest(codec, uint32(tableID), 0, 1)
			require.NoError(t, streamer.Enqueue(ctx, reqs))
			results, err := streamer.GetResults(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, len(results))
			// 29 is usually the number of bytes in
			// ScanResponse.BatchResponse[0]. We choose to hard-code this number
			// rather than consult NumBytes field directly as an additional
			// sanity-check. We also adjust the estimate to account for possible
			// tenant prefix.
			expectedMemToken := scanResponseOverhead + 29 + int64(len(codec.TenantPrefix()))
			if results[0].ScanResp.NumBytes == 33+int64(len(codec.TenantPrefix())) {
				// For some reason, sometimes it's not 29, but 33, and we do
				// allow for this.
				expectedMemToken += 4
			}
			require.Equal(t, expectedMemToken, results[0].memoryTok.toRelease)
			expectedUsed := expectedMemToken + resultSize
			if !singleRowLookup {
				// This is streamer.numRangesPerScanRequestAccountedFor which is
				// only non-zero when SingleRowLookup hint is false.
				expectedUsed += 4
			}
			require.Equal(t, expectedUsed, acc.Used())
		})
	}
}

func makeScanRequest(codec keys.SQLCodec, tableID uint32, start, end int) kvpb.RequestUnion {
	var res kvpb.RequestUnion
	var scan kvpb.ScanRequest
	var union kvpb.RequestUnion_Scan
	makeKey := func(pk int) []byte {
		// These numbers essentially make a key like '/t/primary/pk'.
		return append(codec.IndexPrefix(tableID, 1), byte(136+pk))
	}
	scan.Key = makeKey(start)
	scan.EndKey = makeKey(end)
	scan.ScanFormat = kvpb.BATCH_RESPONSE
	union.Scan = &scan
	res.Value = &union
	return res
}
