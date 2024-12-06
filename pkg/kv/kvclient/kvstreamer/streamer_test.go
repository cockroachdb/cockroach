// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func getStreamer(
	ctx context.Context,
	s serverutils.ApplicationLayerInterface,
	limitBytes int64,
	acc *mon.BoundAccount,
) *kvstreamer.Streamer {
	rootTxn := kv.NewTxn(ctx, s.DB(), s.DistSQLPlanningNodeID())
	leafInputState, err := rootTxn.GetLeafTxnInputState(ctx)
	if err != nil {
		panic(err)
	}
	leafTxn := kv.NewLeafTxn(ctx, s.DB(), s.DistSQLPlanningNodeID(), leafInputState)
	metrics := kvstreamer.MakeMetrics()
	return kvstreamer.NewStreamer(
		s.DistSenderI().(*kvcoord.DistSender),
		&metrics,
		s.AppStopper(),
		leafTxn,
		func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
			res, err := leafTxn.Send(ctx, ba)
			if err != nil {
				return nil, err.GoError()
			}
			return res, nil
		},
		cluster.MakeTestingClusterSettings(),
		nil, /* sd */
		lock.WaitPolicy(0),
		limitBytes,
		acc,
		nil, /* kvPairsRead */
		lock.None,
		lock.Unreplicated,
	)
}

// TestStreamerLimitations verifies that the streamer panics or encounters
// errors in currently unsupported or invalid scenarios.
func TestStreamerLimitations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	getStreamer := func() *kvstreamer.Streamer {
		return getStreamer(ctx, s, math.MaxInt64, mon.NewStandaloneUnlimitedAccount())
	}

	t.Run("non-unique requests unsupported", func(t *testing.T) {
		require.Panics(t, func() {
			streamer := getStreamer()
			streamer.Init(kvstreamer.OutOfOrder, kvstreamer.Hints{UniqueRequests: false}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
		})
	})

	t.Run("pipelining unsupported", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)
		streamer.Init(kvstreamer.OutOfOrder, kvstreamer.Hints{UniqueRequests: true}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
		k := append(s.Codec().TenantPrefix(), roachpb.Key("key")...)
		get := kvpb.NewGet(k)
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
		metrics := kvstreamer.MakeMetrics()
		require.Panics(t, func() {
			kvstreamer.NewStreamer(
				s.DistSenderI().(*kvcoord.DistSender),
				&metrics,
				s.AppStopper(),
				kv.NewTxn(ctx, s.DB(), s.DistSQLPlanningNodeID()),
				nil, /* sendFn */
				cluster.MakeTestingClusterSettings(),
				nil, /* sd */
				lock.WaitPolicy(0),
				math.MaxInt64, /* limitBytes */
				nil,           /* acc */
				nil,           /* kvPairsRead */
				lock.None,
				lock.Unreplicated,
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
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	codec := s.Codec()

	// Create a dummy table for which we know the encoding of valid keys.
	_, err := db.Exec("CREATE TABLE foo (pk_blob STRING PRIMARY KEY, attribute INT, blob TEXT, INDEX(attribute))")
	require.NoError(t, err)

	// Obtain the TableID.
	r := db.QueryRow("SELECT 'foo'::regclass::oid")
	var tableID int
	require.NoError(t, r.Scan(&tableID))

	// makeGetRequest returns a valid GetRequest that wants to lookup a key with
	// value 'a' repeated keySize number of times in the primary index of table
	// 'foo'.
	makeGetRequest := func(keySize int) kvpb.RequestUnion {
		var res kvpb.RequestUnion
		var get kvpb.GetRequest
		var union kvpb.RequestUnion_Get
		var key []byte
		key = append(key, codec.IndexPrefix(uint32(tableID), 1)...)
		key = append(key, 18)
		for i := 0; i < keySize; i++ {
			key = append(key, 97)
		}
		key = append(key, []byte{0, 1, 136}...)
		get.Key = key
		union.Get = &get
		res.Value = &union
		return res
	}

	// Imitate a root SQL memory monitor with 1MiB size.
	const rootPoolSize = 1 << 20 /* 1MiB */
	rootMemMonitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("root"),
		Settings: cluster.MakeTestingClusterSettings(),
	})
	rootMemMonitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(rootPoolSize))
	defer rootMemMonitor.Stop(ctx)

	acc := rootMemMonitor.MakeBoundAccount()
	defer acc.Close(ctx)

	const limitBytes = 30
	getStreamer := func() *kvstreamer.Streamer {
		acc.Clear(ctx)
		s := getStreamer(ctx, s, limitBytes, &acc)
		s.Init(kvstreamer.OutOfOrder, kvstreamer.Hints{UniqueRequests: true}, 1 /* maxKeysPerRow */, nil /* diskBuffer */)
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
	defer s.Stopper().Stop(context.Background())

	// The initial estimate for TargetBytes argument for each asynchronous
	// request by the Streamer will be numRowsPerRange x InitialAvgResponseSize,
	// so we pick the blob size such that about half of rows are included in the
	// partial responses.
	const blobSize = 2 * kvstreamer.InitialAvgResponseSize
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
		3 * kvstreamer.InitialAvgResponseSize * numRows / 2,
		7 * kvstreamer.InitialAvgResponseSize * numRows / 4,
		2 * kvstreamer.InitialAvgResponseSize * numRows,
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
	defer s.Stopper().Stop(context.Background())

	const blobSize = 10 * kvstreamer.InitialAvgResponseSize
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

// TestStreamerEmptyScans verifies that the Streamer behaves correctly when
// Scan requests return empty responses.
func TestStreamerEmptyScans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Start a cluster with large --max-sql-memory parameter so that the
	// Streamer isn't hitting the root budget exceeded error.
	const rootPoolSize = 1 << 30 /* 1GiB */
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		SQLMemoryPoolSize: rootPoolSize,
	})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)

	ts := srv.ApplicationLayer()
	codec := ts.Codec()

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

	getStreamer := func() *kvstreamer.Streamer {
		s := getStreamer(ctx, ts, math.MaxInt64, mon.NewStandaloneUnlimitedAccount())
		// There are two column families in the table.
		s.Init(kvstreamer.OutOfOrder, kvstreamer.Hints{UniqueRequests: true}, 2 /* maxKeysPerRow */, nil /* diskBuffer */)
		return s
	}

	t.Run("scan single range", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Close(ctx)

		// Scan the row with pk=0.
		reqs := make([]kvpb.RequestUnion, 1)
		reqs[0] = makeScanRequest(codec, tableID, 0, 1)
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
		reqs[0] = makeScanRequest(codec, tableID, 1, 4)
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
	defer s.Stopper().Stop(context.Background())

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

// TestStreamerVaryingResponseSizes verifies that the Streamer handles the
// responses of vastly variable sizes reasonably well. It is a regression test
// for #113729.
func TestStreamerVaryingResponseSizes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				// We disable the randomization of some batch sizes because with
				// some low values the test takes much longer.
				ForceProductionValues: true,
			},
		},
		// Disable tenant randomization since this test is quite heavy and could
		// result in a timeout under shared-process tenant.
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(context.Background())

	runner := sqlutils.MakeSQLRunner(db)
	// Create a table with 10 ranges, with 3k rows in each. Within each range,
	// first 1k rows are relatively small, then next 1k rows are medium, and
	// the last 1k rows are large.
	runner.Exec(t, `
CREATE TABLE t (
	k INT PRIMARY KEY,
	v STRING,
	blob STRING,
	INDEX t_v_idx (v ASC)
);

INSERT INTO t SELECT 3000 * (i // 1000) + i % 1000, '1', repeat('a', 600 + i % 200) FROM generate_series(1, 10000) AS g(i);
INSERT INTO t SELECT 3000 * (i // 1000) + i % 1000 + 1000, '1', repeat('a', 3000 + i % 1000) FROM generate_series(1, 10000) AS g(i);
INSERT INTO t SELECT 3000 * (i // 1000) + i % 1000 + 2000, '1', repeat('a', 20000 + i) FROM generate_series(1, 10000) AS g(i);

ALTER TABLE t SPLIT AT SELECT generate_series(1, 30000, 3000);
`)

	// The meat of the test - run the query that performs an index join to fetch
	// all rows via the streamer, both in the OutOfOrder and InOrder modes. Each
	// time assert that the number of BatchRequests issued is in double digits
	// (if not, then the streamer was extremely suboptimal).
	kvGRPCCallsRegex := regexp.MustCompile(`KV gRPC calls: ([\d,]+)`)
	for inOrder := range []bool{false, true} {
		runner.Exec(t, `SET streamer_always_maintain_ordering = $1;`, inOrder)
		for i := 0; i < 2; i++ {
			gRPCCalls := -1
			var err error
			rows := runner.QueryStr(t, `EXPLAIN ANALYZE SELECT length(blob) FROM t@t_v_idx WHERE v = '1';`)
			for _, row := range rows {
				if matches := kvGRPCCallsRegex.FindStringSubmatch(row[0]); len(matches) > 0 {
					gRPCCalls, err = strconv.Atoi(strings.ReplaceAll(matches[1], ",", ""))
					require.NoError(t, err)
					break
				}
			}
			require.Greater(t, gRPCCalls, 0, rows)
			require.Greater(t, 100, gRPCCalls, rows)
		}
	}
}

// TestStreamerRandomAccess verifies that the Streamer handles the requests that
// have random access pattern within ranges reasonably well. It is a regression
// test for #133043.
func TestStreamerRandomAccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				// We disable the randomization of some batch sizes because with
				// some low values the test takes much longer.
				ForceProductionValues: true,
			},
		},
		// Disable tenant randomization since this test is quite heavy and could
		// result in a timeout under shared-process tenant.
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(context.Background())

	rng, _ := randutil.NewTestRand()
	runner := sqlutils.MakeSQLRunner(db)
	// Create a table with 3 ranges, with 2k rows in each. Each row is about
	// 2.7KiB in size and has a random value in column 'v'.
	runner.Exec(t, `
CREATE TABLE t (
  k INT PRIMARY KEY,
  v INT,
  blob STRING,
  INDEX v_idx (v)
);

INSERT INTO t (k, v, blob) SELECT i, (random()*6000)::INT, repeat('a', 2700) FROM generate_series(1, 6000) AS g(i);

ALTER TABLE t SPLIT AT SELECT i*2000 FROM generate_series(0, 2) AS g(i);
`)

	// The meat of the test - run the query that performs an index join to fetch
	// all rows via the streamer, both in the OutOfOrder and InOrder modes, and
	// with different workmem limits. Each time assert that the number of
	// BatchRequests issued is relatively small (if not, then the streamer was
	// extremely suboptimal).
	kvGRPCCallsRegex := regexp.MustCompile(`KV gRPC calls: ([\d,]+)`)
	for i := 0; i < 10; i++ {
		// Pick random workmem limit in [2MiB; 16MiB] range.
		workmem := 2<<20 + rng.Intn(14<<20)
		runner.Exec(t, fmt.Sprintf("SET distsql_workmem = '%dB'", workmem))
		for inOrder := range []bool{false, true} {
			runner.Exec(t, `SET streamer_always_maintain_ordering = $1;`, inOrder)
			gRPCCalls := -1
			var err error
			rows := runner.QueryStr(t, `EXPLAIN ANALYZE SELECT * FROM t@v_idx WHERE v > 0`)
			for _, row := range rows {
				if matches := kvGRPCCallsRegex.FindStringSubmatch(row[0]); len(matches) > 0 {
					gRPCCalls, err = strconv.Atoi(strings.ReplaceAll(matches[1], ",", ""))
					require.NoError(t, err)
					break
				}
			}
			require.Greater(t, gRPCCalls, 0, rows)
			require.Greater(t, 150, gRPCCalls, rows)
		}
	}
}
