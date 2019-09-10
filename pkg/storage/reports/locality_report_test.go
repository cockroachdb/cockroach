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

func TestLocalityReport(t *testing.T) {
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

	// Add several localities and verify the result
	r := makeReplicationCriticalLocalitiesReportSaver()
	r.AddCriticalLocality(MakeZoneKey(1, 3), "region=US")
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")
	r.AddCriticalLocality(MakeZoneKey(7, 8), "dc=B")
	r.AddCriticalLocality(MakeZoneKey(7, 8), "dc=B")

	time1 := time.Date(2001, 1, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time1, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"1", "3", "'region=US'", "2", "1"},
		{"5", "6", "'dc=A'", "2", "1"},
		{"7", "8", "'dc=B'", "2", "2"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 10:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	// Add new set of localities and verify the old ones are deleted
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")
	r.AddCriticalLocality(MakeZoneKey(7, 8), "dc=B")
	r.AddCriticalLocality(MakeZoneKey(7, 8), "dc=B")
	r.AddCriticalLocality(MakeZoneKey(15, 6), "dc=A")

	time2 := time.Date(2001, 1, 1, 11, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time2, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"5", "6", "'dc=A'", "2", "2"},
		{"7", "8", "'dc=B'", "2", "2"},
		{"15", "6", "'dc=A'", "2", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 11:00:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())

	time3 := time.Date(2001, 1, 1, 11, 30, 0, 0, time.UTC)
	// If some other server takes over and does an update.
	rows, err := con.Exec(ctx, "another-updater", nil, "update system.reports_meta set generated=$1 where id=2", time3)
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "update system.replication_critical_localities "+
		"set at_risk_ranges=3 where zone_id=5 and subzone_id=6 and locality='dc=A'")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "delete from system.replication_critical_localities "+
		"where zone_id=7 and subzone_id=8")
	require.NoError(t, err)
	require.Equal(t, 1, rows)
	rows, err = con.Exec(ctx, "another-updater", nil, "insert into system.replication_critical_localities("+
		"zone_id, subzone_id, locality, report_id, at_risk_ranges) values(16,16,'region=EU',2,6)")
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	// Add new set of localities and verify the old ones are deleted
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")
	r.AddCriticalLocality(MakeZoneKey(7, 8), "dc=B")
	r.AddCriticalLocality(MakeZoneKey(7, 8), "dc=B")
	r.AddCriticalLocality(MakeZoneKey(15, 6), "dc=A")

	time4 := time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time4, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"5", "6", "'dc=A'", "2", "3"},
		{"7", "8", "'dc=B'", "2", "2"},
		{"15", "6", "'dc=A'", "2", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 12:00:00+00:00'"},
	})
	require.Equal(t, 2, r.LastUpdatedRowCount())

	// A brand new report (after restart for example) - still works.
	r = makeReplicationCriticalLocalitiesReportSaver()
	r.AddCriticalLocality(MakeZoneKey(5, 6), "dc=A")

	time5 := time.Date(2001, 1, 1, 12, 30, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, time5, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"5", "6", "'dc=A'", "2", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 12:30:00+00:00'"},
	})
	require.Equal(t, 3, r.LastUpdatedRowCount())
}

func TableData(
	ctx context.Context, tableName string, executor sqlutil.InternalExecutor,
) [][]string {
	if rows, err := executor.Query(
		ctx, "test-select-"+tableName, nil /* txn */, "select * from "+tableName); err == nil {
		result := make([][]string, 0, len(rows))
		for _, row := range rows {
			stringRow := make([]string, 0, row.Len())
			for _, item := range row {
				stringRow = append(stringRow, item.String())
			}
			result = append(result, stringRow)
		}
		return result
	}
	return nil
}
