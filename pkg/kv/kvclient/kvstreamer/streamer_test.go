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
	gosql "database/sql"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func getStreamer(
	ctx context.Context, s serverutils.TestServerInterface, limitBytes int64, acc *mon.BoundAccount,
) *Streamer {
	rootTxn := kv.NewTxn(ctx, s.DB(), s.NodeID())
	return NewStreamer(
		s.DistSenderI().(*kvcoord.DistSender),
		s.Stopper(),
		kv.NewLeafTxn(ctx, s.DB(), s.NodeID(), rootTxn.GetLeafTxnInputState(ctx)),
		cluster.MakeTestingClusterSettings(),
		lock.WaitPolicy(0),
		limitBytes,
		acc,
		nil, /* batchRequestsIssued */
		lock.None,
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

	t.Run("non-unique requests unsupported", func(t *testing.T) {
		require.Panics(t, func() {
			streamer := getStreamer()
			streamer.Init(OutOfOrder, Hints{UniqueRequests: false}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
		})
	})

	t.Run("pipelining unsupported", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)
		streamer.Init(OutOfOrder, Hints{UniqueRequests: true}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
		get := kvpb.NewGet(roachpb.Key("key"), false /* forUpdate */)
		reqs := []kvpb.RequestUnion{{
			Value: &kvpb.RequestUnion_Get{
				Get: get.(*kvpb.GetRequest),
			},
		}}
		require.NoError(t, streamer.Enqueue(ctx, reqs))
		// It is invalid to enqueue more requests before the previous have been
		// responded to.
		require.Error(t, streamer.Enqueue(ctx, reqs))
	})

	t.Run("unexpected RootTxn", func(t *testing.T) {
		require.Panics(t, func() {
			NewStreamer(
				s.DistSenderI().(*kvcoord.DistSender),
				s.Stopper(),
				kv.NewTxn(ctx, s.DB(), s.NodeID()),
				cluster.MakeTestingClusterSettings(),
				lock.WaitPolicy(0),
				math.MaxInt64, /* limitBytes */
				nil,           /* acc */
				nil,           /* batchRequestsIssued */
				lock.None,
			)
		})
	})
}

// assertTableID verifies that the table specified by 'tableName' has the
// provided value for TableID.
func assertTableID(t *testing.T, db *gosql.DB, tableName string, tableID int) {
	r := db.QueryRow(fmt.Sprintf("SELECT '%s'::regclass::oid", tableName))
	var actualTableID int
	require.NoError(t, r.Scan(&actualTableID))
	require.Equal(t, tableID, actualTableID)
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

	const tableID = 104
	// Sanity check that the table 'foo' has the expected TableID.
	assertTableID(t, db, "foo" /* tableName */, tableID)

	// makeGetRequest returns a valid GetRequest that wants to lookup a key with
	// value 'a' repeated keySize number of times in the primary index of table
	// foo.
	makeGetRequest := func(keySize int) kvpb.RequestUnion {
		var res kvpb.RequestUnion
		var get kvpb.GetRequest
		var union kvpb.RequestUnion_Get
		key := make([]byte, keySize+6)
		key[0] = tableID + 136
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
	rootMemMonitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(rootPoolSize))
	defer rootMemMonitor.Stop(ctx)

	acc := rootMemMonitor.MakeBoundAccount()
	defer acc.Close(ctx)

	const limitBytes = 30
	getStreamer := func() *Streamer {
		acc.Clear(ctx)
		s := getStreamer(ctx, s, limitBytes, &acc)
		s.Init(OutOfOrder, Hints{UniqueRequests: true}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
		return s
	}

	t.Run("single key exceeds limit", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)

		// A single request that exceeds the limit should be allowed.
		reqs := make([]kvpb.RequestUnion, 1)
		reqs[0] = makeGetRequest(limitBytes + 1)
		require.NoError(t, streamer.Enqueue(ctx, reqs))
	})

	t.Run("single key exceeds root pool size", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)

		// A single request that exceeds the limit as well as the root SQL pool
		// should be denied.
		reqs := make([]kvpb.RequestUnion, 1)
		reqs[0] = makeGetRequest(rootPoolSize + 1)
		require.Error(t, streamer.Enqueue(ctx, reqs))
	})

	t.Run("multiple keys exceed limit", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)

		// Create two requests which exceed the limit when combined.
		reqs := make([]kvpb.RequestUnion, 2)
		reqs[0] = makeGetRequest(limitBytes/2 + 1)
		reqs[1] = makeGetRequest(limitBytes/2 + 1)
		require.Error(t, streamer.Enqueue(ctx, reqs))
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

// TestStreamerColumnFamilies verifies that the Streamer works correctly with
// large rows and multiple column families. The goal is to make sure that KVs
// from different rows are not intertwined.
func TestStreamerWideRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a cluster with large --max-sql-memory parameter so that the
	// Streamer isn't hitting the root budget exceeded error.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: 1 << 30, /* 1GiB */
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	const blobSize = 10 * initialAvgResponseSize
	const numRows = 2

	_, err := db.Exec("CREATE TABLE t (pk INT PRIMARY KEY, k INT, blob1 STRING, blob2 STRING, INDEX (k), FAMILY (pk, k, blob1), FAMILY (blob2))")
	require.NoError(t, err)
	for i := 0; i < numRows; i++ {
		if i > 0 {
			// Split each row into a separate range.
			_, err = db.Exec(fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES(%d)", i))
			require.NoError(t, err)
		}
		_, err = db.Exec(fmt.Sprintf("INSERT INTO t SELECT %d, 1, repeat('a', %d), repeat('b', %d)", i, blobSize, blobSize))
		require.NoError(t, err)
	}

	// Populate the range cache.
	_, err = db.Exec("SELECT count(*) from t")
	require.NoError(t, err)

	// Perform an index join to read large blobs.
	query := "SELECT count(*), sum(length(blob1)), sum(length(blob2)) FROM t@t_k_idx WHERE k = 1"
	const concurrency = 3
	// Different values for the distsql_workmem setting allow us to exercise the
	// behavior in some degenerate cases (e.g. a small value results in a single
	// KV exceeding the limit).
	for _, workmem := range []int{
		3 * blobSize / 2,
		3 * blobSize,
		4 * blobSize,
	} {
		t.Run(fmt.Sprintf("workmem=%s", humanize.Bytes(uint64(workmem))), func(t *testing.T) {
			_, err = db.Exec(fmt.Sprintf("SET distsql_workmem = '%dB'", workmem))
			require.NoError(t, err)
			var wg sync.WaitGroup
			wg.Add(concurrency)
			errCh := make(chan error, concurrency)
			for i := 0; i < concurrency; i++ {
				go func() {
					defer wg.Done()
					row := db.QueryRow(query)
					var count, sum1, sum2 int
					if err := row.Scan(&count, &sum1, &sum2); err != nil {
						errCh <- err
						return
					}
					if count != numRows {
						errCh <- errors.Newf("expected %d rows, read %d", numRows, count)
						return
					}
					if sum1 != numRows*blobSize {
						errCh <- errors.Newf("expected total length %d of blob1, read %d", numRows*blobSize, sum1)
						return
					}
					if sum2 != numRows*blobSize {
						errCh <- errors.Newf("expected total length %d of blob2, read %d", numRows*blobSize, sum2)
						return
					}
				}()
			}
			wg.Wait()
			close(errCh)
			err, ok := <-errCh
			if ok {
				t.Fatal(err)
			}
		})
	}
}

func makeScanRequest(tableID byte, start, end int) kvpb.RequestUnion {
	var res kvpb.RequestUnion
	var scan kvpb.ScanRequest
	var union kvpb.RequestUnion_Scan
	makeKey := func(pk int) []byte {
		// These numbers essentially make a key like '/t/primary/pk'.
		return []byte{tableID + 136, 137, byte(136 + pk)}
	}
	scan.Key = makeKey(start)
	scan.EndKey = makeKey(end)
	scan.ScanFormat = kvpb.BATCH_RESPONSE
	union.Scan = &scan
	res.Value = &union
	return res
}

// TestStreamerEmptyScans verifies that the Streamer behaves correctly when
// Scan requests return empty responses.
func TestStreamerEmptyScans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a cluster with large --max-sql-memory parameter so that the
	// Streamer isn't hitting the root budget exceeded error.
	const rootPoolSize = 1 << 30 /* 1GiB */
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: rootPoolSize,
	})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Create a dummy table for which we know the encoding of valid keys.
	// Although not strictly necessary, we set up two column families since with
	// a single family in production a Get request would have been used.
	_, err := db.Exec("CREATE TABLE t (pk INT PRIMARY KEY, k INT, blob STRING, INDEX (k), FAMILY (pk, k), FAMILY (blob))")
	require.NoError(t, err)

	const tableID = 104
	// Sanity check that the table 't' has the expected TableID.
	assertTableID(t, db, "t" /* tableName */, tableID)

	// Split the table into 5 ranges and populate the range cache.
	for pk := 1; pk < 5; pk++ {
		_, err = db.Exec(fmt.Sprintf("ALTER TABLE t SPLIT AT VALUES(%d)", pk))
		require.NoError(t, err)
	}
	_, err = db.Exec("SELECT count(*) from t")
	require.NoError(t, err)

	getStreamer := func() *Streamer {
		s := getStreamer(ctx, s, math.MaxInt64, nil /* acc */)
		// There are two column families in the table.
		s.Init(OutOfOrder, Hints{UniqueRequests: true}, 2 /* maxKeysPerRow */, nil /* diskBuffer */)
		return s
	}

	t.Run("scan single range", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)

		// Scan the row with pk=0.
		reqs := make([]kvpb.RequestUnion, 1)
		reqs[0] = makeScanRequest(tableID, 0, 1)
		require.NoError(t, streamer.Enqueue(ctx, reqs))
		results, err := streamer.GetResults(ctx)
		require.NoError(t, err)
		// We expect a single empty Scan response.
		require.Equal(t, 1, len(results))
	})

	t.Run("scan multiple ranges", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)

		// Scan the rows with pk in range [1, 4).
		reqs := make([]kvpb.RequestUnion, 1)
		reqs[0] = makeScanRequest(tableID, 1, 4)
		require.NoError(t, streamer.Enqueue(ctx, reqs))
		// We expect an empty response for each range.
		var numResults int
		for {
			results, err := streamer.GetResults(ctx)
			require.NoError(t, err)
			numResults += len(results)
			if len(results) == 0 {
				break
			}
		}
		require.Equal(t, 3, numResults)
	})
}

// TestStreamerMultiRangeScan verifies that the Streamer correctly handles scan
// requests that span multiple ranges.
func TestStreamerMultiRangeScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	rng, _ := randutil.NewTestRand()
	numRows := rng.Intn(100) + 2

	// We set up two tables such that we'll use the value from the smaller one
	// to lookup into a secondary index in the larger table.
	_, err := db.Exec("CREATE TABLE small (n PRIMARY KEY) AS SELECT 1")
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE large (k INT PRIMARY KEY, n INT, s STRING, INDEX (n, k) STORING (s))")
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO large SELECT i, 1, repeat('a', i) FROM generate_series(1, %d) AS i", numRows))
	require.NoError(t, err)

	// Split the range for the secondary index into multiple.
	numRanges := 2
	if numRows > 2 {
		numRanges = rng.Intn(numRows-2) + 2
	}
	kValues := make([]int, numRows)
	for i := range kValues {
		kValues[i] = i + 1
	}
	rng.Shuffle(numRows, func(i, j int) {
		kValues[i], kValues[j] = kValues[j], kValues[i]
	})
	splitKValues := kValues[:numRanges]
	for _, kValue := range splitKValues {
		_, err = db.Exec(fmt.Sprintf("ALTER INDEX large_n_k_idx SPLIT AT VALUES (1, %d)", kValue))
		require.NoError(t, err)
	}

	// Populate the range cache.
	_, err = db.Exec("SELECT * FROM large@large_n_k_idx")
	require.NoError(t, err)

	// The crux of the test - run a query that performs a lookup join when
	// ordering needs to be maintained and then confirm that the results of the
	// parallel lookups are served in the right order.
	// TODO(yuzefovich): remove ORDER BY clause inside array_agg when the lookup
	// joins use the InOrder mode of the streamer when ordering needs to be
	// maintained.
	r := db.QueryRow("SELECT array_agg(s ORDER BY s) FROM small INNER LOOKUP JOIN large ON small.n = large.n GROUP BY small.n ORDER BY small.n")
	var result string
	err = r.Scan(&result)
	require.NoError(t, err)
	// The expected result is of the form: {a,aa,aaa,...}.
	expected := "{"
	for i := 1; i <= numRows; i++ {
		if i > 1 {
			expected += ","
		}
		for j := 0; j < i; j++ {
			expected += "a"
		}
	}
	expected += "}"
	require.Equal(t, expected, result)
}

// TestStreamerMemoryAccounting performs sanity checking on the memory
// accounting done by the streamer.
func TestStreamerMemoryAccounting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Create a table (for which we know the encoding of valid keys) with a
	// single row.
	_, err := db.Exec("CREATE TABLE t (pk PRIMARY KEY, k) AS VALUES (0, 0)")
	require.NoError(t, err)

	const tableID = 104
	// Sanity check that the table 't' has the expected TableID.
	assertTableID(t, db, "t" /* tableName */, tableID)

	makeGetRequest := func(key int) kvpb.RequestUnion {
		var res kvpb.RequestUnion
		var get kvpb.GetRequest
		var union kvpb.RequestUnion_Get
		makeKey := func(pk int) []byte {
			// These numbers essentially make a key like '/t/primary/key/0'.
			return []byte{tableID + 136, 137, byte(136 + pk), 136}
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
		s := getStreamer(ctx, s, math.MaxInt64, &acc)
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
			reqs[0] = makeScanRequest(tableID, 0, 1)
			require.NoError(t, streamer.Enqueue(ctx, reqs))
			results, err := streamer.GetResults(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, len(results))
			// 29 is usually the number of bytes in
			// ScanResponse.BatchResponse[0]. We choose to hard-code this number
			// rather than consult NumBytes field directly as an additional
			// sanity-check.
			expectedMemToken := scanResponseOverhead + 29
			if results[0].ScanResp.NumBytes == 33 {
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
