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

func TestConstraintReport(t *testing.T) {
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
	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{})

	// Add several replication constraint statuses.
	r := makeReplicationConstraintStatusReportSaver()
	r.EnsureEntry(MakeZoneKey(1, 3), "constraint", "+country=CH")
	r.AddViolation(MakeZoneKey(2, 3), "constraint", "+country=CH")
	r.EnsureEntry(MakeZoneKey(2, 3), "constraint", "+country=CH")
	r.AddViolation(MakeZoneKey(5, 6), "constraint", "+ssd")
	r.AddViolation(MakeZoneKey(5, 6), "constraint", "+ssd")
	r.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	r.EnsureEntry(MakeZoneKey(7, 8), "constraint", "+dc=east")
	r.EnsureEntry(MakeZoneKey(7, 8), "constraint", "+dc=east")
	r.AddViolation(MakeZoneKey(8, 9), "constraint", "+dc=west")
	r.EnsureEntry(MakeZoneKey(8, 9), "constraint", "+dc=east")

	time1 := time.Date(2001, 1, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time1, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		{"1", "3", "'constraint'", "'+country=CH'", "1", "NULL", "0"},
		{"2", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"5", "6", "'constraint'", "'+ssd'", "1", "'2001-01-01 10:00:00+00:00'", "2"},
		{"7", "8", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"7", "8", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
		{"8", "9", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"8", "9", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 10:00:00+00:00'"},
	})
	require.Equal(t, 7, r.LastUpdatedRowCount())

	// Add new set of replication constraint statuses to the existing report and verify the old ones are deleted.
	r.AddViolation(MakeZoneKey(1, 3), "constraint", "+country=CH")
	r.EnsureEntry(MakeZoneKey(5, 6), "constraint", "+ssd")
	r.AddViolation(MakeZoneKey(6, 8), "constraint", "+dc=east")
	r.EnsureEntry(MakeZoneKey(6, 8), "constraint", "+dc=west")
	r.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	r.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	r.AddViolation(MakeZoneKey(8, 9), "constraint", "+dc=west")
	r.EnsureEntry(MakeZoneKey(8, 9), "constraint", "+dc=east")

	time2 := time.Date(2001, 1, 1, 11, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time2, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		// Wasn't violated before - is violated now.
		{"1", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 11:00:00+00:00'", "1"},
		// Was violated before - isn't violated now.
		{"5", "6", "'constraint'", "'+ssd'", "1", "NULL", "0"},
		// Didn't exist before - new for this run and violated.
		{"6", "8", "'constraint'", "'+dc=east'", "1", "'2001-01-01 11:00:00+00:00'", "1"},
		// Didn't exist before - new for this run and not violated.
		{"6", "8", "'constraint'", "'+dc=west'", "1", "NULL", "0"},
		// Was violated before - and it still is but the range count changed.
		{"7", "8", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "2"},
		// Was violated before - and it still is but the range count didn't change.
		{"8", "9", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		// Wasn't violated before - and is still not violated.
		{"8", "9", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 11:00:00+00:00'"},
	})
	require.Equal(t, 7, r.LastUpdatedRowCount())

	time3 := time.Date(2001, 1, 1, 11, 30, 0, 0, time.UTC)
	// If some other server takes over and does an update.
	rows, err := con.Exec(ctx, "another-updater", nil, "update system.reports_meta set generated=$1 where id=1", time3)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "update system.replication_constraint_stats "+
		"set violation_start=null, violating_ranges=0 where zone_id=1 and subzone_id=3")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "update system.replication_constraint_stats "+
		"set violation_start=$1, violating_ranges=5 where zone_id=5 and subzone_id=6", time3)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "delete from system.replication_constraint_stats "+
		"where zone_id=7 and subzone_id=8")
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	// Add new set of replication constraint statuses to the existing report and verify the everything is good.
	r.AddViolation(MakeZoneKey(1, 3), "constraint", "+country=CH")
	r.EnsureEntry(MakeZoneKey(5, 6), "constraint", "+ssd")
	r.AddViolation(MakeZoneKey(6, 8), "constraint", "+dc=east")
	r.EnsureEntry(MakeZoneKey(6, 8), "constraint", "+dc=west")
	r.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	r.AddViolation(MakeZoneKey(7, 8), "constraint", "+dc=west")
	r.AddViolation(MakeZoneKey(8, 9), "constraint", "+dc=west")
	r.EnsureEntry(MakeZoneKey(8, 9), "constraint", "+dc=east")

	time4 := time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time4, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		{"1", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 12:00:00+00:00'", "1"},
		{"5", "6", "'constraint'", "'+ssd'", "1", "NULL", "0"},
		{"6", "8", "'constraint'", "'+dc=east'", "1", "'2001-01-01 11:00:00+00:00'", "1"},
		{"6", "8", "'constraint'", "'+dc=west'", "1", "NULL", "0"},
		{"7", "8", "'constraint'", "'+dc=west'", "1", "'2001-01-01 12:00:00+00:00'", "2"},
		{"8", "9", "'constraint'", "'+dc=west'", "1", "'2001-01-01 10:00:00+00:00'", "1"},
		{"8", "9", "'constraint'", "'+dc=east'", "1", "NULL", "0"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 12:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	// A brand new report (after restart for example) - still works.
	// Add several replication constraint statuses.
	r = makeReplicationConstraintStatusReportSaver()
	r.AddViolation(MakeZoneKey(1, 3), "constraint", "+country=CH")

	time5 := time.Date(2001, 1, 1, 12, 30, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time5, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{
		{"1", "3", "'constraint'", "'+country=CH'", "1", "'2001-01-01 12:00:00+00:00'", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"1", "'2001-01-01 12:30:00+00:00'"},
	})
	require.Equal(t, 6, r.LastUpdatedRowCount())
}
