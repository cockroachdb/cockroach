// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package structlogging_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/plan"
	"github.com/cockroachdb/cockroach/pkg/server/structlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

type hotRangesLogSpy struct {
	t  *testing.T
	mu struct {
		syncutil.RWMutex
		logs []eventpb.HotRangesStats
	}
}

func (spy *hotRangesLogSpy) Intercept(e []byte) {
	var entry logpb.Entry
	if err := json.Unmarshal(e, &entry); err != nil {
		spy.t.Fatal(err)
	}

	re := regexp.MustCompile(`"EventType":"hot_ranges_stats"`)
	if entry.Channel != logpb.Channel_TELEMETRY || !re.MatchString(entry.Message) {
		return
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	var rangesLog eventpb.HotRangesStats
	if err := json.Unmarshal([]byte(entry.Message[entry.StructuredStart:entry.StructuredEnd]), &rangesLog); err != nil {
		spy.t.Fatal(err)
	}

	spy.mu.logs = append(spy.mu.logs, rangesLog)
}

func (spy *hotRangesLogSpy) Logs() []eventpb.HotRangesStats {
	spy.mu.RLock()
	defer spy.mu.RUnlock()
	logs := make([]eventpb.HotRangesStats, len(spy.mu.logs))
	copy(logs, spy.mu.logs)
	return logs
}

func (spy *hotRangesLogSpy) Reset() {
	spy.mu.Lock()
	defer spy.mu.Unlock()
	spy.mu.logs = nil
}

// TestHotRangesStatsTenants tests that hot ranges stats are logged per node.
// The test will ensure each node contains 5 distinct range replicas for hot
// ranges logging. Each node should thus log 5 distinct range ids.
func TestHotRangesStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	skip.UnderRace(t)

	ctx := context.Background()
	spy := hotRangesLogSpy{t: t}
	defer log.InterceptWith(ctx, &spy)()

	tc := serverutils.StartCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					ReplicaPlannerKnobs: plan.ReplicaPlannerTestingKnobs{
						DisableReplicaRebalancing: true,
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	sqlutils.CreateTable(
		t, db, "foo",
		"k INT PRIMARY KEY, v INT",
		300,
		sqlutils.ToRowFn(sqlutils.RowIdxFn, sqlutils.RowModuloFn(2)),
	)

	// Ensure both of node 1 and 2 have 5 distinct replicas from the table.
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(
		tc.Server(0).DB(), keys.SystemSQLCodec, "test", "foo")
	tc.SplitTable(t, tableDesc, []serverutils.SplitPoint{
		{TargetNodeIdx: 1, Vals: []interface{}{100}},
		{TargetNodeIdx: 1, Vals: []interface{}{120}},
		{TargetNodeIdx: 1, Vals: []interface{}{140}},
		{TargetNodeIdx: 1, Vals: []interface{}{160}},
		{TargetNodeIdx: 1, Vals: []interface{}{180}},
		{TargetNodeIdx: 2, Vals: []interface{}{200}},
		{TargetNodeIdx: 2, Vals: []interface{}{220}},
		{TargetNodeIdx: 2, Vals: []interface{}{240}},
		{TargetNodeIdx: 2, Vals: []interface{}{260}},
		{TargetNodeIdx: 2, Vals: []interface{}{280}},
	})

	// query table
	for i := 0; i < 300; i++ {
		db := tc.ServerConn(0)
		sqlutils.MakeSQLRunner(db).Query(t, `SELECT * FROM test.foo`)
	}

	// Skip node 1 since it will contain many more replicas.
	// We only need to check nodes 2 and 3 to see that the nodes are logging their local hot ranges.
	rangeIDs := make(map[int64]struct{})
	for _, i := range []int{1, 2} {
		spy.Reset()
		ts := tc.ApplicationLayer(i)
		structlogging.TelemetryHotRangesStatsEnabled.Override(ctx, &ts.ClusterSettings().SV, true)
		structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &ts.ClusterSettings().SV, time.Second)
		structlogging.TelemetryHotRangesStatsLoggingDelay.Override(ctx, &ts.ClusterSettings().SV, 0*time.Millisecond)

		testutils.SucceedsSoon(t, func() error {
			logs := spy.Logs()
			if len(logs) < 5 {
				return errors.New("waiting for hot ranges to be logged")
			}

			return nil
		})
		structlogging.TelemetryHotRangesStatsInterval.Override(ctx, &ts.ClusterSettings().SV, 1*time.Hour)

		// Get first 5 logs since the logging loop may have fired multiple times.
		// We should have gotten 5 distinct range ids, one for each split point above.
		logs := spy.Logs()[:5]
		for _, l := range logs {
			assert.Equal(t, l.Databases, []string{"‹test›"})
			assert.Equal(t, l.Tables, []string{"‹foo›"})
			assert.Equal(t, l.Indexes, []string{"‹foo_pkey›"})
			_, ok := rangeIDs[l.RangeID]
			if ok {
				t.Fatalf(`Logged ranges should be unique per node for this test.
found range on node %d and node %d: %s %s %s %s %d`, i, l.LeaseholderNodeID, l.Databases, l.SchemaName, l.Tables, l.Indexes, l.RangeID)
			}
			rangeIDs[l.RangeID] = struct{}{}
		}

	}
}
