// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reports

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestRangeReport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// This test uses the cluster as a recipient for a report saved from outside
	// the cluster. We disable the cluster's own production of reports so that it
	// doesn't interfere with the test.
	ReporterInterval.Override(&st.SV, 0)
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	con := s.InternalExecutor().(sqlutil.InternalExecutor)
	defer s.Stopper().Stop(ctx)

	// Verify that tables are empty.
	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{})

	// Add several localities and verify the result
	r := makeReplicationStatsReportSaver()
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), true, true, true)
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), false, true, true)
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), false, false, true)
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), true, true, false)
	r.AddZoneRangeStatus(MakeZoneKey(2, 3), false, false, false)
	r.AddZoneRangeStatus(MakeZoneKey(2, 4), false, true, false)

	time1 := time.Date(2001, 1, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time1, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{
		{"1", "3", "3", "4", "2", "3", "3"},
		{"2", "3", "3", "1", "0", "0", "0"},
		{"2", "4", "3", "1", "0", "1", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"3", "'2001-01-01 10:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	// Add new set of localities and verify the old ones are deleted
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), false, true, true)
	r.AddZoneRangeStatus(MakeZoneKey(2, 3), false, false, false)
	r.AddZoneRangeStatus(MakeZoneKey(4, 4), false, true, true)

	time2 := time.Date(2001, 1, 1, 11, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time2, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{
		{"1", "3", "3", "1", "0", "1", "1"},
		{"2", "3", "3", "1", "0", "0", "0"},
		{"4", "4", "3", "1", "0", "1", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"3", "'2001-01-01 11:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	time3 := time.Date(2001, 1, 1, 11, 30, 0, 0, time.UTC)
	// If some other server takes over and does an update.
	rows, err := con.Exec(ctx, "another-updater", nil, "update system.reports_meta set generated=$1 where id=3", time3)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "update system.replication_stats "+
		"set total_ranges=3 where zone_id=1 and subzone_id=3")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "delete from system.replication_stats "+
		"where zone_id=2 and subzone_id=3")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "insert into system.replication_stats("+
		"zone_id, subzone_id, report_id, total_ranges, unavailable_ranges, under_replicated_ranges, "+
		"over_replicated_ranges) values(16, 16, 3, 6, 0, 1, 2)")
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	// Add new set of localities and verify the old ones are deleted
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), false, true, true)
	r.AddZoneRangeStatus(MakeZoneKey(2, 3), false, false, false)
	r.AddZoneRangeStatus(MakeZoneKey(4, 4), false, true, true)

	time4 := time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time4, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{
		{"1", "3", "3", "1", "0", "1", "1"},
		{"2", "3", "3", "1", "0", "0", "0"},
		{"4", "4", "3", "1", "0", "1", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"3", "'2001-01-01 12:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	// A brand new report (after restart for example) - still works.
	r = makeReplicationStatsReportSaver()
	r.AddZoneRangeStatus(MakeZoneKey(1, 3), false, true, true)

	time5 := time.Date(2001, 1, 1, 12, 30, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time5, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{
		{"1", "3", "3", "1", "0", "1", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"3", "'2001-01-01 12:30:00+00:00'"},
	})
	require.Equal(t, 2, r.LastUpdatedRowCount())
}
