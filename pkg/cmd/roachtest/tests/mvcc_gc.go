// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func registerMVCCGC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "mvcc_gc",
		Owner:            registry.OwnerKV,
		Timeout:          30 * time.Minute,
		Cluster:          r.MakeClusterSpec(3),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Leases:           registry.MetamorphicLeases,
		Run:              runMVCCGC,
	})
}

/*
Test verifies that GC respects range tombstones and garbage collects range
tombstones and that mvcc stats are correctly updated.

Test sequence:
- create kv test data schema
- pre-split with 3 min expiration to allow merging to proceed in parallel after
  initial data is loaded
- load test data using KV workload
- set low GC ttl to make all data eligible for deletion after 2 min
- start background kv workload to write data into cockroach
- repeat 3 times (while ranges are being gradually merged back)
  - delete some data in kv table using range tombstones
    (deletion is done using cli commands that sends kv requests to cluster)
  - wait for all range tombstones to be garbage collected within 5 min
    - keep enqueueing ranges to GC queue every 2 minutes to force deletion
- stop workload
- remove all data in kv table using overlapping range tombstones
- verify that all data was removed and that consistency check passes
*/

func runMVCCGC(ctx context.Context, t test.Test, c cluster.Cluster) {
	rng, _ := randutil.NewTestRand()

	// How many times test repeats generate/cleanup cycles after initial one.
	const cleanupRuns = 3
	// How long to wait for data to be GCd during assert loop.
	const gcRetryTimeout = 7 * time.Minute

	var randomSeed int64
	if roachtestutil.UsingRuntimeAssertions(t) {
		// Do not use `0` as that is reserved to mean that we are running
		// without runtime assertions.
		for randomSeed == 0 {
			randomSeed = rand.Int63()
		}
		c.SetRandomSeed(randomSeed)
	}
	s := install.MakeClusterSettings()
	s.Env = append(s.Env, "COCKROACH_SCAN_INTERVAL=30s")
	// Disable an automatic scheduled backup as it would mess with the gc ttl this test relies on.
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), s)

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()

	execSQLOrFail := func(statement string, args ...interface{}) {
		if _, err := conn.ExecContext(ctx, statement, args...); err != nil {
			t.Fatal(err)
		}
	}

	setClusterSetting := func(name string, value interface{}) {
		execSQLOrFail(fmt.Sprintf(`SET CLUSTER SETTING %s = $1`, name), value)
	}

	// Protected timestamps prevent GC from collecting data, even with low ttl
	// we need to wait for protected ts to be moved. By reducing this interval
	// we ensure that data will always be collectable after ttl + 5s.
	setClusterSetting("kv.protectedts.poll_interval", "5s")
	// Disable mvcc_gc queue throttling, we always manually enqueue replicas as
	// fast as possible.
	setClusterSetting("kv.mvcc_gc.queue_interval", "0s")
	// Load based lease balancing could move leaseholders from under the GC. If
	// that happens gc will end up running on non lease-holder store and its
	// requests would be rejected. This is causing test to flake. Disabling
	// rebalancing is better than increasing wait time further.
	setClusterSetting("kv.allocator.load_based_lease_rebalancing.enabled", false)

	if err := roachtestutil.WaitFor3XReplication(ctx, t.L(), conn); err != nil {
		t.Fatalf("failed to up-replicate cluster: %s", err)
	}

	m := c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		cmd := roachtestutil.NewCommand("./cockroach workload init kv").
			Flag("cycle-length", 20000).
			Arg("{pgurl:1}").
			String()
		c.Run(ctx, option.WithNodes(c.Node(1)), cmd)

		execSQLOrFail("alter database kv configure zone using gc.ttlseconds = $1", 120)

		t.L().Printf("finished init, doing force split ranges")
		splitKvIntoRanges(t, conn, 50, 180*time.Second, "kv", "kv")

		wlCtx, wlCancel := context.WithCancel(ctx)
		defer wlCancel()
		wlFailure := make(chan error, 1)
		t.Go(func(context.Context, *logger.Logger) error {
			defer close(wlFailure)
			cmd = roachtestutil.NewCommand("./cockroach workload run kv").
				Flag("cycle-length", 20000).
				Flag("max-block-bytes", 2048).
				Flag("min-block-bytes", 2048).
				Flag("read-percent", 0).
				Flag("max-rate", 1800).
				Arg("{pgurl%s}", c.Node(1)).
				String()
			err := c.RunE(wlCtx, option.WithNodes(c.Node(1)), cmd)
			wlFailure <- err
			return nil
		}, task.Name("workload"))

		m := queryTableMetaOrFatal(t, conn, "kv", "kv")

		for i := 0; i < cleanupRuns; i++ {
			t.L().Printf("performing clean-assert cycle #%d", i)

			if err := retry.WithMaxAttempts(ctx, retry.Options{}, 3, func() error {
				return deleteSomeTableDataWithOverlappingTombstones(ctx, t, c, conn, rng, m, 5, randomSeed)
			}); err != nil {
				t.Fatal(err)
			}

			t.L().Printf("partially deleted some data using tombstones")

			assertRangesWithGCRetry(ctx, t, c, gcRetryTimeout, m, func() error {
				return assertTableMVCCStatsOrFatal(t, conn, m, checkRangesHaveNoRangeTombstones)
			})

			t.L().Printf("all range tombstones were garbage collected")
		}

		select {
		case wlErr := <-wlFailure:
			if wlErr != nil {
				t.Fatalf("workload failed: %s", wlErr)
			}
		default:
		}
		wlCancel()

		if err := retry.WithMaxAttempts(ctx, retry.Options{}, 3, func() error {
			err := deleteAllTableDataWithOverlappingTombstones(ctx, t, c, conn, rng, m, 5, randomSeed)
			if err != nil {
				return err
			}
			// We must check if table is empty because workload termination is only
			// cancelling context and termination will happen in the background.
			return checkTableIsEmpty(t, conn, m)
		}); err != nil {
			t.Fatal(err)
		}

		t.L().Printf("deleted all table data using tombstones")

		assertRangesWithGCRetry(ctx, t, c, gcRetryTimeout, m, func() error {
			return assertStatsAndConsistencyOrFatal(t, conn, m, checkRangesConsistentAndHaveNoData)
		})

		return nil
	})
	m.Wait()
}

func checkTableIsEmpty(t test.Test, conn *gosql.DB, m tableMetadata) error {
	t.Helper()
	var rows int
	queryRowOrFatal(t, conn, fmt.Sprintf("select count(*) from %s.%s", m.databaseName, m.tableName), nil, []any{&rows})
	if rows > 0 {
		return errors.Newf("table still has %d rows", rows)
	}
	return nil
}

func assertRangesWithGCRetry(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	timeout time.Duration,
	m tableMetadata,
	assertion func() error,
) {
	t.Helper()
	deadline := timeutil.Now().Add(timeout)
	gcRetryTime := timeutil.Now()
	// Track number of ranges in the table to handle cases when merges run
	// concurrently with GC and doesn't allow it to collect all data.
	var rangeCount int
	lastMergeDeadline := timeutil.Now()
	for {
		if timeutil.Now().After(gcRetryTime) {
			rc := enqueueAllTableRangesForGC(ctx, t, c, m)
			t.L().Printf("enqueued %d ranges for GC", rc)
			gcRetryTime = timeutil.Now().Add(2 * time.Minute)
			if rc != rangeCount {
				// Wait at least one more GC run if number of ranges changed.
				lastMergeDeadline = gcRetryTime.Add(time.Minute)
				rangeCount = rc
			}
		}
		err := assertion()
		if err == nil {
			return
		}
		if now := timeutil.Now(); now.After(deadline) && now.After(lastMergeDeadline) {
			t.Fatalf("assertion still failing after %s: %s", timeout, err)
		}
		select {
		case <-time.After(10 * time.Second):
		case <-ctx.Done():
			// Test is cancelled.
			return
		}
	}
}

func checkRangesHaveNoRangeTombstones(rangeStats enginepb.MVCCStats) error {
	if rangeStats.RangeKeyCount > 0 || rangeStats.RangeKeyBytes > 0 {
		return errors.Errorf("range contain range tombstones %s", rangeStats.String())
	}
	return nil
}

func splitKvIntoRanges(
	t test.Test, conn *gosql.DB, ranges int, expiration time.Duration, tableName, databaseName string,
) {
	if ranges < 2 {
		return
	}
	var values = make([]string, ranges-1)
	for i := 0; i < ranges-1; i++ {
		values[i] = fmt.Sprintf("(%d)", getSplitKey(i+1, ranges))
	}
	stmt := fmt.Sprintf(`alter table %s.%s split at values %s with expiration now() + INTERVAL '%d'`,
		databaseName, tableName, strings.Join(values, ", "), int(expiration.Seconds()))
	if _, err := conn.Exec(stmt); err != nil {
		t.Fatalf("failed to split kv table into ranges: %s", err)
	}
}

// Get split point splits uint64 key space into 'fragments' chunks and returns
// 'index' chunk start.
func getSplitPoint(index, fragments int) uint64 {
	return math.MaxUint64 / uint64(fragments) * uint64(index)
}

// Get split keys gives back int64 split points for keys used by kv workload.
func getSplitKey(index, fragments int) int64 {
	return math.MinInt64 + int64(getSplitPoint(index, fragments))
}

func checkRangesConsistentAndHaveNoData(totals enginepb.MVCCStats, details rangeDetails) error {
	if totals.RangeKeyCount > 0 || totals.RangeKeyBytes > 0 {
		return errors.Errorf("table ranges contain range tombstones %s", totals.String())
	}
	if totals.GCBytesAge != 0 || totals.GCBytes() > 0 {
		return errors.Errorf("table ranges contain garbage %s", totals.String())
	}
	if totals.LiveBytes > 0 || totals.LiveCount > 0 ||
		totals.IntentBytes > 0 || totals.IntentCount > 0 ||
		totals.LockBytes > 0 || totals.LockCount > 0 {
		return errors.Errorf("table ranges contain live data %s", totals.String())
	}
	if details.status != kvpb.CheckConsistencyResponse_RANGE_CONSISTENT.String() {
		return errors.Errorf("consistency check failed %s detail: %s", details.status,
			details.detail)
	}
	return nil
}

func assertTableMVCCStatsOrFatal(
	t test.Test, conn *gosql.DB, m tableMetadata, assert func(enginepb.MVCCStats) error,
) error {
	t.Helper()
	rows, err := conn.Query(fmt.Sprintf(`
SELECT range_id, raw_start_key, raw_end_key, crdb_internal.range_stats(raw_start_key)
FROM [SHOW RANGES FROM TABLE %s.%s WITH KEYS]
ORDER BY start_key`,
		tree.NameString(m.databaseName), tree.NameString(m.tableName)))
	if err != nil {
		t.Fatalf("failed to run consistency check query on table: %s", err)
	}
	defer rows.Close()

	jsonpb := protoutil.JSONPb{}
	var rangeCount int
	for rows.Next() {
		var (
			rangeID       int
			rangeStartKey roachpb.Key
			rangeEndKey   roachpb.Key
			jsonb         []byte
			rangeStats    enginepb.MVCCStats
		)
		if err := rows.Scan(&rangeID, &rangeStartKey, &rangeEndKey, &jsonb); err != nil {
			t.Fatalf("failed to run consistency check query on table: %s", err)
		}
		if err := jsonpb.Unmarshal(jsonb, &rangeStats); err != nil {
			t.Fatalf("failed to unmarshal json %s stats: %s", string(jsonb), err)
		}
		if err := assert(rangeStats); err != nil {
			return errors.Wrapf(err, "assertion failed on range r%d %s", rangeID,
				roachpb.Span{Key: rangeStartKey, EndKey: rangeEndKey})
		}
		rangeCount++
	}
	if rangeCount == 0 {
		return errors.New("failed to find any ranges for table")
	}
	return nil
}

// rangeDetails contains results of check_consistency for a single range.
type rangeDetails struct {
	stats  enginepb.MVCCStats
	status string
	detail string
}

func assertStatsAndConsistencyOrFatal(
	t test.Test, conn *gosql.DB, m tableMetadata, assert func(enginepb.MVCCStats, rangeDetails) error,
) error {
	t.Helper()
	var startKey, endKey roachpb.Key
	queryRowOrFatal(t, conn,
		fmt.Sprintf(`SELECT min(raw_start_key), max(raw_end_key)
FROM [SHOW RANGES FROM TABLE %s.%s WITH KEYS]`,
			tree.NameString(m.tableName), tree.NameString(m.databaseName)), nil, []interface{}{&startKey, &endKey})

	rows, err := conn.Query(`SELECT range_id, start_key, status, detail, crdb_internal.range_stats(start_key) FROM crdb_internal.check_consistency(false, $1, $2) ORDER BY start_key`,
		startKey, endKey)
	if err != nil {
		t.Fatalf("failed to run consistency check query on table: %s", err)
	}
	defer rows.Close()

	jsonpb := protoutil.JSONPb{}
	var tableRanges int
	for rows.Next() {
		var (
			rangeID       int
			rangeStartKey roachpb.Key
			status        string
			detail        string
			jsonb         []byte
			rangeStats    enginepb.MVCCStats
		)
		if err := rows.Scan(&rangeID, &rangeStartKey, &status, &detail, &jsonb); err != nil {
			t.Fatalf("failed to run consistency check query on table: %s", err)
		}
		if err := jsonpb.Unmarshal(jsonb, &rangeStats); err != nil {
			t.Fatalf("failed to unmarshal json %s stats: %s", string(jsonb), err)
		}
		if err := assert(rangeStats, rangeDetails{
			stats:  rangeStats,
			status: status,
			detail: detail,
		}); err != nil {
			return errors.Wrapf(err, "assertion failed on range r%d %s", rangeID, rangeStartKey)
		}
		tableRanges++
	}
	if tableRanges == 0 {
		return errors.New("failed to find any ranges for table")
	}
	return nil
}

type tableMetadata struct {
	tableID      int
	tableName    string
	databaseName string
}

func (m tableMetadata) String() string {
	return fmt.Sprintf("TableMetadata{tableID=%d, tableName='%s', databaseName='%s'}",
		m.tableID, m.tableName, m.databaseName)
}

func queryTableMetaOrFatal(
	t test.Test, conn *gosql.DB, tableName, databaseName string,
) tableMetadata {
	t.Helper()
	var tableID int
	queryRowOrFatal(t, conn,
		`select table_id from crdb_internal.tables where database_name = $1 and name = $2`,
		[]interface{}{databaseName, tableName}, []interface{}{&tableID})

	return tableMetadata{
		tableID:      tableID,
		tableName:    tableName,
		databaseName: databaseName,
	}
}

func enqueueAllTableRangesForGC(
	ctx context.Context, t test.Test, c cluster.Cluster, m tableMetadata,
) int {
	t.Helper()
	var conns []*gosql.DB
	for _, node := range c.All() {
		conns = append(conns, c.Conn(ctx, t.L(), node))
	}
	var tableRanges int
	if err := visitTableRanges(ctx, t, conns[0], m, func(info rangeInfo) error {
		t.L().Printf("enqueuing range r%d [%s,%s) for GC", info.id, info.startKey, info.endKey)
		for _, c := range conns {
			_, _ = c.ExecContext(ctx, `select crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, info.id)
		}
		tableRanges++
		return nil
	}); err != nil {
		t.Fatalf("failed to enqueue table ranges for GC: %s", err)
	}
	return tableRanges
}

type rangeInfo struct {
	id               int
	startKey, endKey string
}

func visitTableRanges(
	ctx context.Context, t test.Test, conn *gosql.DB, m tableMetadata, f func(info rangeInfo) error,
) error {
	t.Helper()
	rows, err := conn.QueryContext(
		ctx, fmt.Sprintf(`SELECT range_id, start_key, end_key FROM [ SHOW RANGES FROM TABLE %s.%s ]`, m.databaseName, m.tableName),
	)
	if err != nil {
		t.Fatalf("failed to query ranges for table: %s", err)
	}
	defer rows.Close()

	var ranges []rangeInfo
	for rows.Next() {
		var info rangeInfo
		if err := rows.Scan(&info.id, &info.startKey, &info.endKey); err != nil {
			t.Fatalf("failed to query ranges for table: %s", err)
		}
		ranges = append(ranges, info)
	}
	rows.Close()

	for _, id := range ranges {
		if err := f(id); err != nil {
			return err
		}
	}
	return nil
}

func deleteAllTableDataWithOverlappingTombstones(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	conn *gosql.DB,
	rng *rand.Rand,
	tm tableMetadata,
	fragments int,
	randomSeed int64,
) error {
	t.Helper()
	encodeKey := func(index int64) roachpb.Key {
		var key roachpb.Key
		queryRowOrFatal(t, conn, `select * from crdb_internal.encode_key($1, 1, (CAST($2 as INT8),))`, []interface{}{tm.tableID, index}, []interface{}{&key})
		return key
	}

	leftBound := func(index int) roachpb.Key {
		if index == 0 {
			return encodeKey(math.MinInt64)
		}
		max := getSplitPoint(index, fragments)
		left := math.MinInt64 + int64(randutil.RandUint64n(rng, max))
		return encodeKey(left)
	}

	rightBound := func(index int) roachpb.Key {
		if index >= fragments-1 {
			return encodeKey(math.MaxInt64).Next()
		}
		min := getSplitPoint(index+1, fragments)
		right := math.MinInt64 + int64(min+randutil.RandUint64n(rng, math.MaxUint64-min))
		return encodeKey(right)
	}

	var ba kvpb.BatchRequest
	for i := 0; i < fragments; i++ {
		startKey := leftBound(i)
		endKey := rightBound(i)
		t.L().Printf("adding range tombstone [%s, %s)", startKey, endKey)
		addDeleteRangeUsingTombstone(&ba, startKey, endKey)
	}
	br, err := sendBatchRequest(ctx, t, c, 1, ba, randomSeed)
	if err != nil {
		return err
	}
	if br.Error != nil {
		t.L().Printf("batch request failed to put range tombstones %s", br.Error.String())
		return br.Error.GoError()
	}
	return nil
}

func deleteSomeTableDataWithOverlappingTombstones(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	conn *gosql.DB,
	rng *rand.Rand,
	tm tableMetadata,
	rangeKeys int,
	randomSeed int64,
) error {
	t.Helper()
	encodeKey := func(index int64) roachpb.Key {
		var key roachpb.Key
		queryRowOrFatal(t, conn, `select * from crdb_internal.encode_key($1, 1, (CAST($2 as INT8),))`, []interface{}{tm.tableID, index}, []interface{}{&key})
		return key
	}

	var ba kvpb.BatchRequest
	for i := 0; i < rangeKeys; i++ {
		startPK := math.MinInt64 + int64(rng.Uint64())
		endPK := math.MinInt64 + int64(rng.Uint64())
		if startPK > endPK {
			endPK, startPK = startPK, endPK
		} else if startPK == endPK {
			if endPK < math.MaxInt64 {
				endPK++
			} else {
				startPK--
			}
		}
		startKey := encodeKey(startPK)
		endKey := encodeKey(endPK)
		t.L().Printf("adding range tombstone [%s, %s)", startKey, endKey)
		addDeleteRangeUsingTombstone(&ba, startKey, endKey)
	}
	br, err := sendBatchRequest(ctx, t, c, 1, ba, randomSeed)
	if err != nil {
		return err
	}
	if br.Error != nil {
		t.L().Printf("batch request failed to put range tombstones %s", br.Error.String())
		return br.Error.GoError()
	}
	return nil
}

func queryRowOrFatal(
	t test.Test, conn *gosql.DB, query string, args []interface{}, to []interface{},
) {
	t.Helper()
	row := conn.QueryRow(query, args...)
	if row.Err() != nil {
		t.Fatalf("failed to query %s: %s", query, row.Err())
	}
	if err := row.Scan(to...); err != nil {
		t.Fatalf("failed to scan query result for %s: %s", query, err)
	}
}

func addDeleteRangeUsingTombstone(ba *kvpb.BatchRequest, startKey, endKey roachpb.Key) {
	r := kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		UseRangeTombstone: true,
	}
	ba.Add(&r)
}

func sendBatchRequest(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	node int,
	ba kvpb.BatchRequest,
	randomSeed int64,
) (kvpb.BatchResponse, error) {
	reqArg, err := batchToJSONOrFatal(ba)
	if err != nil {
		return kvpb.BatchResponse{}, err
	}
	requestFileName := "request-" + uuid.MakeV4().String() + ".json"
	if err := c.PutString(ctx, reqArg, requestFileName, 0755, c.Node(node)); err != nil {
		return kvpb.BatchResponse{}, err
	}
	var debugEnv string
	if randomSeed != 0 {
		debugEnv = fmt.Sprintf("COCKROACH_RANDOM_SEED=%d ", randomSeed)
	}
	cmd := roachtestutil.NewCommand("./cockroach debug send-kv-batch").
		Arg("%s", requestFileName).
		Flag("certs-dir", install.CockroachNodeCertsDir).
		Flag("host", fmt.Sprintf("localhost:{pgport:%d}", node)).
		String()
	res, err := c.RunWithDetailsSingleNode(
		ctx, t.L(), option.WithNodes(c.Node(node)), debugEnv+cmd)
	if err != nil {
		return kvpb.BatchResponse{}, err
	}
	return jsonToResponseOrFatal(res.Stdout)
}

func batchToJSONOrFatal(ba kvpb.BatchRequest) (string, error) {
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	return string(jsonProto), err
}

func jsonToResponseOrFatal(json string) (br kvpb.BatchResponse, err error) {
	jsonpb := protoutil.JSONPb{}
	err = jsonpb.Unmarshal([]byte(json), &br)
	return br, err
}
