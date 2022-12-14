// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
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
		Name:    "mvcc_gc",
		Owner:   registry.OwnerKV,
		Timeout: 30 * time.Minute,
		Cluster: r.MakeClusterSpec(3),
		Run:     runMVCCGC,
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

	c.Put(ctx, t.Cockroach(), "./cockroach")
	s := install.MakeClusterSettings()
	s.Env = append(s.Env, "COCKROACH_SCAN_INTERVAL=30s")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), s)

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

	setClusterSetting("storage.mvcc.range_tombstones.enabled", true)
	setClusterSetting("kv.protectedts.poll_interval", "5s")
	setClusterSetting("kv.mvcc_gc.queue_interval", "0s")

	if err := WaitFor3XReplication(ctx, t, conn); err != nil {
		t.Fatalf("failed to up-replicate cluster: %s", err)
	}

	m := c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(1), "./cockroach", "workload", "init", "kv", "--cycle-length", "20000")

		execSQLOrFail("alter database kv configure zone using gc.ttlseconds = $1", 120)

		t.L().Printf("finished init, doing force split ranges")
		splitKvIntoRanges(t, conn, 50, 180*time.Second, "kv", "kv")

		wlCtx, wlCancel := context.WithCancel(ctx)
		defer wlCancel()
		wlFailure := make(chan error)
		go func() {
			defer close(wlFailure)
			err := c.RunE(wlCtx, c.Node(1), "./cockroach", "workload", "run", "kv", "--cycle-length", "20000",
				"--max-block-bytes", "2048", "--min-block-bytes", "2048", "--read-percent", "0", "--max-rate", "1800")
			wlFailure <- err
		}()

		m := queryTableMetaOrFatal(t, conn, "kv", "kv")

		for i := 0; i < cleanupRuns; i++ {
			t.L().Printf("performing clean-assert cycle #%d", i)

			if err := retry.WithMaxAttempts(ctx, retry.Options{}, 3, func() error {
				return deleteSomeTableDataWithOverlappingTombstones(ctx, t, c, conn, rng, m, 5)
			}); err != nil {
				t.Fatal(err)
			}

			t.L().Printf("partially deleted some data using tombstones")

			assertRangesWithGCRetry(ctx, t, c, gcRetryTimeout, m, func() error {
				totals, rangeCount := collectTableMVCCStatsOrFatal(t, conn, m)
				return checkRangesHaveNoRangeTombstones(totals, rangeCount)
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
			return deleteAllTableDataWithOverlappingTombstones(ctx, t, c, conn, rng, m, 5)
		}); err != nil {
			t.Fatal(err)
		}

		assertRangesWithGCRetry(ctx, t, c, gcRetryTimeout, m, func() error {
			totals, details := collectStatsAndConsistencyOrFail(t, conn, m)
			return checkRangesConsistentAndHaveNoData(totals, details)
		})

		return nil
	})
	m.Wait()
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
	for {
		if timeutil.Now().After(gcRetryTime) {
			enqueueAllTableRangesForGC(ctx, t, c, m)
			t.L().Printf("enqueued ranges for GC")
			gcRetryTime = timeutil.Now().Add(2 * time.Minute)
		}
		err := assertion()
		if err == nil {
			return
		}
		if timeutil.Now().After(deadline) {
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

func checkRangesHaveNoRangeTombstones(totals enginepb.MVCCStats, c int) error {
	if c == 0 {
		return errors.New("failed to find any ranges for table")
	}
	if totals.RangeKeyCount > 0 || totals.RangeKeyBytes > 0 {
		return errors.Errorf("table ranges contain range tombstones %s", totals.String())
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

func checkRangesConsistentAndHaveNoData(
	totals enginepb.MVCCStats, details map[int]rangeDetails,
) error {
	if len(details) == 0 {
		return errors.New("failed to find any ranges for table")
	}
	if totals.RangeKeyCount > 0 || totals.RangeKeyBytes > 0 {
		return errors.Errorf("table ranges contain range tombstones %s", totals.String())
	}
	if totals.GCBytesAge != 0 || totals.GCBytes() > 0 {
		return errors.Errorf("table ranges contain garbage %s", totals.String())
	}
	if totals.LiveBytes > 0 || totals.LiveCount > 0 ||
		totals.IntentBytes > 0 || totals.IntentCount > 0 || totals.SeparatedIntentCount > 0 {
		return errors.Errorf("table ranges contain live data %s", totals.String())
	}
	for id, d := range details {
		if d.status != roachpb.CheckConsistencyResponse_RANGE_CONSISTENT.String() {
			return errors.Errorf("consistency check failed for r%d: %s detail: %s", id, d.status,
				d.detail)
		}
	}
	return nil
}

func collectTableMVCCStatsOrFatal(
	t test.Test, conn *gosql.DB, m tableMetadata,
) (enginepb.MVCCStats, int) {
	t.Helper()
	rows, err := conn.Query(fmt.Sprintf(`
SELECT range_id, raw_start_key, crdb_internal.range_stats(raw_start_key)
FROM [SHOW RANGES FROM %s.%s WITH KEYS]
ORDER BY r.start_key`,
		tree.NameString(m.databaseName), tree.NameString(m.tableName)))
	if err != nil {
		t.Fatalf("failed to run consistency check query on table: %s", err)
	}
	defer rows.Close()

	jsonpb := protoutil.JSONPb{}
	var tableStats enginepb.MVCCStats
	var rangeCount int
	for rows.Next() {
		var (
			rangeID       int
			rangeStartKey roachpb.Key
			jsonb         []byte
			rangeStats    enginepb.MVCCStats
		)
		if err := rows.Scan(&rangeID, &rangeStartKey, &jsonb); err != nil {
			t.Fatalf("failed to run consistency check query on table: %s", err)
		}
		if err := jsonpb.Unmarshal(jsonb, &rangeStats); err != nil {
			t.Fatalf("failed to unmarshal json %s stats: %s", string(jsonb), err)
		}
		tableStats.Add(rangeStats)
		rangeCount++
	}
	return tableStats, rangeCount
}

// rangeDetails contains results of check_consistency for a single range.
type rangeDetails struct {
	stats  enginepb.MVCCStats
	status string
	detail string
}

func collectStatsAndConsistencyOrFail(
	t test.Test, conn *gosql.DB, m tableMetadata,
) (enginepb.MVCCStats, map[int]rangeDetails) {
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

	var details = make(map[int]rangeDetails)
	jsonpb := protoutil.JSONPb{}
	var tableStats enginepb.MVCCStats
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
		tableStats.Add(rangeStats)
		details[rangeID] = rangeDetails{
			stats:  rangeStats,
			status: status,
			detail: detail,
		}
	}
	return tableStats, details
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
) {
	t.Helper()
	var conns []*gosql.DB
	for _, node := range c.All() {
		conns = append(conns, c.Conn(ctx, t.L(), node))
	}
	if err := visitTableRanges(ctx, t, conns[0], m, func(rangeID int) error {
		for _, c := range conns {
			_, _ = c.ExecContext(ctx, `select crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, rangeID)
		}
		return nil
	}); err != nil {
		t.Fatalf("failed to enqueue all ranges for GC: %s", err)
	}
}

func visitTableRanges(
	ctx context.Context, t test.Test, conn *gosql.DB, m tableMetadata, f func(rangeID int) error,
) error {
	t.Helper()
	rows, err := conn.QueryContext(ctx, `select range_id from crdb_internal.ranges_no_leases where database_name = $1 and table_name = $2`,
		m.databaseName, m.tableName)
	if err != nil {
		t.Fatalf("failed to run consistency check query on table: %s", err)
	}
	defer rows.Close()
	var rangeIDs []int
	for rows.Next() {
		var rangeID int
		if err := rows.Scan(&rangeID); err != nil {
			t.Fatalf("failed to run consistency check query on table: %s", err)
		}
		rangeIDs = append(rangeIDs, rangeID)
	}
	rows.Close()

	for _, id := range rangeIDs {
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

	var ba roachpb.BatchRequest
	for i := 0; i < fragments; i++ {
		startKey := leftBound(i)
		endKey := rightBound(i)
		t.L().Printf("adding range tombstone [%s, %s)", startKey, endKey)
		addDeleteRangeUsingTombstone(&ba, startKey, endKey)
	}
	br, err := sendBatchRequest(ctx, t, c, 1, ba)
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
) error {
	t.Helper()
	encodeKey := func(index int64) roachpb.Key {
		var key roachpb.Key
		queryRowOrFatal(t, conn, `select * from crdb_internal.encode_key($1, 1, (CAST($2 as INT8),))`, []interface{}{tm.tableID, index}, []interface{}{&key})
		return key
	}

	var ba roachpb.BatchRequest
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
	br, err := sendBatchRequest(ctx, t, c, 1, ba)
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

func addDeleteRangeUsingTombstone(ba *roachpb.BatchRequest, startKey, endKey roachpb.Key) {
	r := roachpb.DeleteRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		UseRangeTombstone: true,
	}
	ba.Add(&r)
}

func sendBatchRequest(
	ctx context.Context, t test.Test, c cluster.Cluster, node int, ba roachpb.BatchRequest,
) (roachpb.BatchResponse, error) {
	reqArg, err := batchToJSONOrFatal(ba)
	if err != nil {
		return roachpb.BatchResponse{}, err
	}
	requestFileName := "request-" + uuid.FastMakeV4().String() + ".json"
	if err := c.PutString(ctx, reqArg, requestFileName, 0755, c.Node(node)); err != nil {
		return roachpb.BatchResponse{}, err
	}
	res, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), "./cockroach", "debug",
		"send-kv-batch", "--insecure", requestFileName)
	if err != nil {
		return roachpb.BatchResponse{}, err
	}
	return jsonToResponseOrFatal(res.Stdout)
}

func batchToJSONOrFatal(ba roachpb.BatchRequest) (string, error) {
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	return string(jsonProto), err
}

func jsonToResponseOrFatal(json string) (br roachpb.BatchResponse, err error) {
	jsonpb := protoutil.JSONPb{}
	err = jsonpb.Unmarshal([]byte(json), &br)
	return br, err
}
