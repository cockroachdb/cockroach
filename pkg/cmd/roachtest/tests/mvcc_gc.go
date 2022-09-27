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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerMVCCGC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "mvcc_gc",
		Owner:   registry.OwnerKV,
		Timeout: 15 * time.Minute,
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
- delete all data in kv table using range tombstones
  (deletion is done using cli commands that sends kv requests to cluster)
- sets low GC ttl to make all data eligible for deletion after 2 min
- write more data over the existing one
- verify that all garbage and range keys were removed by GC within 5 min
*/

func runMVCCGC(ctx context.Context, t test.Test, c cluster.Cluster) {
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

	m := c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		c.Run(ctx, c.Node(1), "./cockroach", "workload", "init", "kv", "--cycle-length", "10000")

		splitKvIntoRanges(t, conn, 100, 180*time.Second, "kv", "kv")

		c.Run(ctx, c.Node(1), "./cockroach", "workload", "run", "kv", "--cycle-length", "10000",
			"--max-block-bytes", "2048", "--min-block-bytes", "2048", "--duration", "2m",
			"--read-percent", "0")

		t.L().Printf("Finished prepping GC data")

		if err := retry.WithMaxAttempts(ctx, retry.Options{}, 3, func() error {
			return deleteAllTableDataWithTombstones(ctx, t, c, conn, "kv", "kv")
		}); err != nil {
			t.Fatal(err)
		}

		t.L().Printf("Deleted all data using tombstones")

		if err := assertTableHasNoLiveData(t, conn, "kv", "kv"); err != nil {
			t.Fatal(err)
		}

		execSQLOrFail("alter database kv configure zone using gc.ttlseconds = $1", 120)

		t.L().Printf("Configured GC ttl to 2 minutes")

		c.Run(ctx, c.Node(1), "./cockroach", "workload", "run", "kv", "--duration", "2m", "--read-percent", "0")

		t.L().Printf("Run second batch of workload on top of deleted data")

		deadline := timeutil.Now().Add(5 * time.Minute)
		var err error
		for timeutil.Now().Before(deadline) {
			err = assertTableHasNoRangeTombstones(t, conn, "kv", "kv")
			if err == nil {
				break
			}
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				// Test is cancelled.
				return nil
			}
		}
		if err != nil {
			t.Fatal("tombstones were not GCd: ", err)
		}

		t.L().Printf("All tombstones were garbage collected")
		return nil
	})
	m.Wait()
}

func splitKvIntoRanges(
	t test.Test, conn *gosql.DB, ranges int, expiration time.Duration, tableName, databaseName string,
) {
	if ranges < 2 {
		return
	}
	var values = make([]string, ranges-1)
	stride := int64(math.MaxUint64 / uint64(ranges))
	split := math.MinInt64 + stride
	for i := 0; i < ranges-1; i++ {
		values[i] = fmt.Sprintf("(%d)", split)
		split += stride
	}
	stmt := fmt.Sprintf(`alter table %s.%s split at values %s with expiration now() + INTERVAL '%d'`,
		databaseName, tableName, strings.Join(values, ", "), int(expiration.Seconds()))
	if _, err := conn.Exec(stmt); err != nil {
		t.Fatal("failed to split kv table into ranges", err)
	}
}

func assertTableHasNoRangeTombstones(
	t test.Test, conn *gosql.DB, tableName, databaseName string,
) error {
	totals := collectTableMVCCStats(t, conn, tableName, databaseName)
	t.L().Printf("Assert range tombstones in stats: %s", totals)
	if totals["RangeKeyCount"] > 0 || totals["RangeKeyBytes"] > 0 || totals["Ranges"] == 0 {
		return errors.Errorf("table ranges contain range tombstones %s", totals.String())
	}
	return nil
}

func assertTableHasNoLiveData(t test.Test, conn *gosql.DB, tableName, databaseName string) error {
	totals := collectTableMVCCStats(t, conn, tableName, databaseName)
	t.L().Printf("Assert live data in stats: %s", totals)
	if totals["LiveBytes"] > 0 || totals["LiveCount"] > 0 ||
		totals["IntentBytes"] > 0 || totals["IntentCount"] > 0 || totals["SeparatedIntentCount"] > 0 ||
		totals["Ranges"] == 0 {
		return errors.Errorf("table ranges contain live data %s", totals.String())
	}
	return nil
}

func collectTableMVCCStats(
	t test.Test, conn *gosql.DB, tableName, databaseName string,
) testRangeStats {
	t.L().Printf("Rescanning table ranges for garbage")
	row := conn.QueryRow(`select min(start_key), max(end_key) from crdb_internal.ranges_no_leases where database_name = $1 and table_name = $2`,
		tableName, databaseName)
	if row.Err() != nil {
		t.Fatal("failed to query key boundaries for table", row.Err())
	}
	var startKey, endKey roachpb.Key
	if err := row.Scan(&startKey, &endKey); err != nil {
		t.Fatal("failed to query key boundaries for table", row)
	}
	rows, err := conn.Query(`select range_id, status, detail from crdb_internal.check_consistency(true, $1, $2) order by start_key`, startKey, endKey)
	if err != nil {
		t.Fatal("failed to run consistency check query on table", err)
	}
	defer rows.Close()

	var result = make(testRangeStats)
	for rows.Next() {
		var (
			rangeID int
			status  string
			detail  string
		)
		if err := rows.Scan(&rangeID, &status, &detail); err != nil {
			t.Fatal("failed to run consistency check query on table", err)
		}
		stats := getRangeStatsFromDetail(detail)
		result.merge(stats)
	}
	return result
}

type testRangeStats map[string]int

func (s testRangeStats) String() string {
	keys := make([]string, len(s))
	i := 0
	for k := range s {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	stats := make([]string, len(keys))
	for i, k := range keys {
		stats[i] = fmt.Sprintf("%s: %d", k, s[k])
	}
	return fmt.Sprintf("{%s}", strings.Join(stats, " "))
}

func (s testRangeStats) merge(other testRangeStats) {
	for k, v := range other {
		s[k] += v
	}
}

var statsRE = regexp.MustCompile(`.*stats: {([^}]*)}.*`)
var valuesRE = regexp.MustCompile(`(\w+):(\d+)`)

func getRangeStatsFromDetail(detail string) testRangeStats {
	stats := statsRE.FindStringSubmatch(detail)
	if stats == nil {
		return nil
	}
	var res = make(testRangeStats)
	for _, field := range valuesRE.FindAllStringSubmatch(stats[1], -1) {
		val, err := strconv.Atoi(field[2])
		if err != nil {
			continue
		}
		res[field[1]] = val
	}
	res["Ranges"] = 1
	return res
}

// Find all ranges that belong to the table via ranges query and then execute KV
// request using cli debug command remotely.
func deleteAllTableDataWithTombstones(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	conn *gosql.DB,
	tableName, databaseName string,
) error {
	rows, err := conn.Query(`select range_id, start_key, end_key, lease_holder  from crdb_internal.ranges where database_name = $1 and table_name = $2`,
		databaseName, tableName)
	if err != nil {
		t.Fatal("failed to query ranges", err)
	}
	defer rows.Close()
	var ba roachpb.BatchRequest
	for rows.Next() {
		var (
			rangeID     int
			startKey    roachpb.Key
			endKey      roachpb.Key
			leaseHolder int
		)
		if err = rows.Scan(&rangeID, &startKey, &endKey, &leaseHolder); err != nil {
			t.Fatal("failed to scan range query results", err)
		}
		addDeleteRangeUsingTombstone(&ba, startKey, endKey)
	}
	br := sendBatchRequestOrFatal(ctx, t, c, 1, ba)
	return br.Error.GoError()
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

func sendBatchRequestOrFatal(
	ctx context.Context, t test.Test, c cluster.Cluster, node int, ba roachpb.BatchRequest,
) roachpb.BatchResponse {
	reqArg := batchToJSONOrFatal(t, ba)
	if err := c.PutString(ctx, reqArg, "request.json", 0755, c.Node(node)); err != nil {
		t.Fatal("failed to put kv request json on the node")
	}
	res, err := c.RunWithDetailsSingleNode(ctx, t.L(), c.Node(node), "./cockroach", "debug",
		"send-kv-batch", "--insecure", "request.json")
	if err != nil {
		t.Fatal("failed to execute remote request over cli")
	}
	return jsonToResponseOrFatal(t, res.Stdout)
}

func batchToJSONOrFatal(t test.Test, ba roachpb.BatchRequest) string {
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	if err != nil {
		t.Fatal("failed to serialize batch request to json", err)
	}
	return string(jsonProto)
}

func jsonToResponseOrFatal(t test.Test, json string) (br roachpb.BatchResponse) {
	jsonpb := protoutil.JSONPb{}
	if err := jsonpb.Unmarshal([]byte(json), &br); err != nil {
		t.Fatal("failed to unmarshal json response", json)
	}
	return br
}
