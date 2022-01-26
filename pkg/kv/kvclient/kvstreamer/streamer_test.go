// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func getStreamer(
	ctx context.Context, s serverutils.TestServerInterface, limitBytes int64, acc *mon.BoundAccount,
) *Streamer {
	return NewStreamer(
		s.DistSenderI().(*kvcoord.DistSender),
		s.Stopper(),
		kv.NewTxn(ctx, s.DB(), s.NodeID()),
		cluster.MakeTestingClusterSettings(),
		lock.WaitPolicy(0),
		limitBytes,
		acc,
	)
}

// TestStreamerLimitations verifies that the streamer panics or encounters
// errors in currently unsupported or invalid scenarios.
func TestStreamerLimitations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	getStreamer := func() *Streamer {
		return getStreamer(ctx, s, math.MaxInt64, nil /* acc */)
	}

	t.Run("InOrder mode unsupported", func(t *testing.T) {
		require.Panics(t, func() {
			streamer := getStreamer()
			streamer.Init(InOrder, Hints{UniqueRequests: true})
		})
	})

	t.Run("non-unique requests unsupported", func(t *testing.T) {
		require.Panics(t, func() {
			streamer := getStreamer()
			streamer.Init(OutOfOrder, Hints{UniqueRequests: false})
		})
	})

	t.Run("invalid enqueueKeys", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close()
		streamer.Init(OutOfOrder, Hints{UniqueRequests: true})
		// Use a single request but two keys which is invalid.
		reqs := []roachpb.RequestUnion{{Value: &roachpb.RequestUnion_Get{}}}
		enqueueKeys := []int{0, 1}
		require.Error(t, streamer.Enqueue(ctx, reqs, enqueueKeys))
	})

	t.Run("pipelining unsupported", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close()
		streamer.Init(OutOfOrder, Hints{UniqueRequests: true})
		get := roachpb.NewGet(roachpb.Key("key"), false /* forUpdate */)
		reqs := []roachpb.RequestUnion{{
			Value: &roachpb.RequestUnion_Get{
				Get: get.(*roachpb.GetRequest),
			},
		}}
		require.NoError(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
		// It is invalid to enqueue more requests before the previous have been
		// responded to.
		require.Error(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
	})
}

// TestLargeKeys verifies that the Streamer successfully completes the queries
// when the keys to lookup are large (i.e. the enqueued requests themselves have
// large memory footprint).
func TestLargeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "the test inserts large blobs, and the machine can be overloaded when under stress")

	rng, _ := randutil.NewTestRand()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Lower the distsql_workmem limit so that we can operate with smaller
	// blobs. Note that the joinReader in the row-by-row engine will override
	// the limit if it is lower than 8MiB, so we cannot go lower than that here.
	_, err := db.Exec("SET distsql_workmem='8MiB'")
	require.NoError(t, err)
	// In both engines, the index joiner buffers input rows up to 4MiB in size,
	// so we have a couple of interesting options for the blob size:
	// - 3000000 is interesting because it doesn't exceed the buffer size, yet
	// two rows with such blobs do exceed it. The index joiners are expected to
	// to process each row on its own.
	// - 5000000 is interesting because a single row already exceeds the buffer
	// size.
	for _, blobSize := range []int{3000000, 5000000} {
		// onlyLarge determines whether only large blobs are inserted or a mix
		// of large and small blobs.
		for _, onlyLarge := range []bool{false, true} {
			_, err = db.Exec("DROP TABLE IF EXISTS foo")
			require.NoError(t, err)
			// We set up such a table that contains two large columns, one of them
			// being the primary key. The idea is that the query below will first
			// read from the secondary index which would include only the PK blob,
			// and that will be used to construct index join lookups (i.e. the PK
			// blobs will be the enqueued requests for the Streamer) whereas the
			// other blob will be part of the response.
			_, err = db.Exec("CREATE TABLE foo (pk_blob STRING PRIMARY KEY, attribute INT, blob TEXT, INDEX(attribute))")
			require.NoError(t, err)

			// Insert a handful of rows.
			numRows := rng.Intn(3) + 3
			for i := 0; i < numRows; i++ {
				letter := string(byte('a') + byte(i))
				valueSize := blobSize
				if !onlyLarge && rng.Float64() < 0.5 {
					// If we're using a mix of large and small values, with 50%
					// use a small value now.
					valueSize = rng.Intn(10) + 1
				}
				_, err = db.Exec("INSERT INTO foo SELECT repeat($1, $2), 1, repeat($1, $2)", letter, valueSize)
				require.NoError(t, err)
			}

			// Perform an index join so that the Streamer API is used.
			query := "SELECT * FROM foo@foo_attribute_idx WHERE attribute=1"
			testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
				vectorizeMode := "off"
				if vectorize {
					vectorizeMode = "on"
				}
				_, err = db.Exec("SET vectorize = " + vectorizeMode)
				require.NoError(t, err)
				_, err = db.Exec(query)
				require.NoError(t, err)
			})
		}
	}
}

// TestStreamerBudgetErrorInEnqueue verifies the behavior of the Streamer in
// Enqueue when its limit and/or root pool limit are exceeded. Additional tests
// around the memory limit errors (when the responses exceed the limit) can be
// found in TestMemoryLimit in pkg/sql.
func TestStreamerBudgetErrorInEnqueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Create a dummy table for which we know the encoding of valid keys.
	_, err := db.Exec("CREATE TABLE foo (pk_blob STRING PRIMARY KEY, attribute INT, blob TEXT, INDEX(attribute))")
	require.NoError(t, err)

	// makeGetRequest returns a valid GetRequest that wants to lookup a key with
	// value 'a' repeated keySize number of times in the primary index of table
	// foo.
	makeGetRequest := func(keySize int) roachpb.RequestUnion {
		var res roachpb.RequestUnion
		var get roachpb.GetRequest
		var union roachpb.RequestUnion_Get
		key := make([]byte, keySize+6)
		key[0] = 190
		key[1] = 137
		key[2] = 18
		for i := 0; i < keySize; i++ {
			key[i+3] = 97
		}
		key[keySize+3] = 0
		key[keySize+4] = 1
		key[keySize+5] = 136
		get.Key = key
		union.Get = &get
		res.Value = &union
		return res
	}

	// Imitate a root SQL memory monitor with 1MiB size.
	const rootPoolSize = 1 << 20 /* 1MiB */
	rootMemMonitor := mon.NewMonitor(
		"root", /* name */
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment */
		math.MaxInt64, /* noteworthy */
		cluster.MakeTestingClusterSettings(),
	)
	rootMemMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(rootPoolSize))
	defer rootMemMonitor.Stop(ctx)

	acc := rootMemMonitor.MakeBoundAccount()
	defer acc.Close(ctx)

	getStreamer := func(limitBytes int64) *Streamer {
		acc.Clear(ctx)
		s := getStreamer(ctx, s, limitBytes, &acc)
		s.Init(OutOfOrder, Hints{UniqueRequests: true})
		return s
	}

	t.Run("single key exceeds limit", func(t *testing.T) {
		const limitBytes = 10
		streamer := getStreamer(limitBytes)
		defer streamer.Close()

		// A single request that exceeds the limit should be allowed.
		reqs := make([]roachpb.RequestUnion, 1)
		reqs[0] = makeGetRequest(limitBytes + 1)
		require.NoError(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
	})

	t.Run("single key exceeds root pool size", func(t *testing.T) {
		const limitBytes = 10
		streamer := getStreamer(limitBytes)
		defer streamer.Close()

		// A single request that exceeds the limit as well as the root SQL pool
		// should be denied.
		reqs := make([]roachpb.RequestUnion, 1)
		reqs[0] = makeGetRequest(rootPoolSize + 1)
		require.Error(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
	})

	t.Run("multiple keys exceed limit", func(t *testing.T) {
		const limitBytes = 10
		streamer := getStreamer(limitBytes)
		defer streamer.Close()

		// Create two requests which exceed the limit when combined.
		reqs := make([]roachpb.RequestUnion, 2)
		reqs[0] = makeGetRequest(limitBytes/2 + 1)
		reqs[1] = makeGetRequest(limitBytes/2 + 1)
		require.Error(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
	})
}

// TestStreamerCorrectlyDiscardsResponses verifies that the Streamer behaves
// correctly in a scenario when partial results are returned, but the original
// memory reservation was under-provisioned and the budget cannot be reconciled,
// so the responses have to be discarded.
func TestStreamerCorrectlyDiscardsResponses(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a cluster with large --max-sql-memory parameter so that the
	// Streamer isn't hitting the root budget exceeded error.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 1 << 30, /* 1GiB */
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// The initial estimate for TargetBytes argument for each asynchronous
	// request by the Streamer will be numRowsPerRange x initialAvgResponseSize,
	// so we pick the blob size such that about half of rows are included in the
	// partial responses.
	const blobSize = 2 * initialAvgResponseSize
	const numRows = 20
	const numRowsPerRange = 4

	_, err := db.Exec("CREATE TABLE t (pk INT PRIMARY KEY, k INT, blob STRING, INDEX (k))")
	require.NoError(t, err)
	for i := 0; i < numRows; i++ {
		if i > 0 && i%numRowsPerRange == 0 {
			// Create a new range for the next numRowsPerRange rows.
			_, err = db.Exec(fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES(%d)", i))
			require.NoError(t, err)
		}
		_, err = db.Exec(fmt.Sprintf("INSERT INTO t SELECT %d, 1, repeat('a', %d)", i, blobSize))
		require.NoError(t, err)
	}

	// Populate the range cache.
	_, err = db.Exec("SELECT count(*) from t")
	require.NoError(t, err)

	// Perform an index join to read the blobs.
	query := "SELECT sum(length(blob)) FROM t@t_k_idx WHERE k = 1"
	// Use several different workmem limits to exercise somewhat different
	// scenarios.
	//
	// All of these values allow for all initial requests to be issued
	// asynchronously, but only for some of the responses to be "accepted" by
	// the budget. This includes 4/3 factor since the vectorized ColIndexJoin
	// gives 3/4 of the workmem limit to the Streamer.
	for _, workmem := range []int{
		3 * initialAvgResponseSize * numRows / 2,
		7 * initialAvgResponseSize * numRows / 4,
		2 * initialAvgResponseSize * numRows,
	} {
		t.Run(fmt.Sprintf("workmem=%s", humanize.Bytes(uint64(workmem))), func(t *testing.T) {
			_, err = db.Exec(fmt.Sprintf("SET distsql_workmem = '%dB'", workmem))
			require.NoError(t, err)
			row := db.QueryRow(query)
			var sum int
			require.NoError(t, row.Scan(&sum))
			require.Equal(t, numRows*blobSize, sum)
		})
	}
}
