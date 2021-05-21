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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type criticalLocality struct {
	object       string
	locality     LocalityRepr
	atRiskRanges int32
}

func (c criticalLocality) less(o criticalLocality) bool {
	if comp := strings.Compare(c.object, o.object); comp != 0 {
		return comp == -1
	}
	if comp := strings.Compare(string(c.locality), string(o.locality)); comp != 0 {
		return comp == -1
	}
	return false
}

type criticalLocalitiesTestCase struct {
	baseReportTestCase
	name string
	exp  []criticalLocality
}

func TestCriticalLocalitiesReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tests := []criticalLocalitiesTestCase{
		{
			name: "simple",
			baseReportTestCase: baseReportTestCase{
				defaultZone: zone{replicas: 3},
				schema: []database{
					{
						name: "db1",
						zone: &zone{replicas: 3},
						tables: []table{
							{name: "t1", zone: &zone{replicas: 3}},
							// Critical localities for t2 are counted towards db1's zone,
							// since t2 doesn't define a zone.
							{name: "t2"},
							// Critical localities for t3 are counted towards db1's zone since
							// t3's zone, although it exists, doesn't set any of the
							// "replication attributes".
							{name: "t3", zone: &zone{}},
						},
					},
					{
						name: "db2",
						tables: []table{
							// Critical localities for t4 and t5 are counted towards the
							// default zone, since neither the table nor the db define a
							// qualifying zone.
							{name: "t4"},
							{name: "t5"},
							{name: "t6", zone: &zone{replicas: 3}},
						},
					},
				},
				splits: []split{
					{key: "/Table/t1", stores: "1 2 3"},
					{key: "/Table/t1/pk", stores: "1 2 3"},
					{key: "/Table/t1/pk/1", stores: "1 2 3"},
					{key: "/Table/t1/pk/2", stores: "1 2 3"},
					{key: "/Table/t1/pk/3", stores: "1 2 3"},
					{key: "/Table/t1/pk/100", stores: "1 2 3"},
					{key: "/Table/t1/pk/150", stores: "1 2 3"},
					{key: "/Table/t1/pk/200", stores: "1 2 3"},
					{key: "/Table/t2", stores: "1 2 3"},
					{key: "/Table/t2/pk", stores: "1 2 3"},
					// This range causes az1 and az2 to both become critical.
					{key: "/Table/t3", stores: "1 2 4"},
					{key: "/Table/t4", stores: "1 2 3"},
					// All the learners are dead, but learners don't matter. So only reg1
					// is critical for this range.
					{key: "/Table/t5", stores: "1 2 3 4l 5l 6l 7l"},
					// Joint-consensus case. Here 1,2,3 are part of the outgoing quorum and
					// 1,4,8 are part of the incoming quorum. 4 and 5 are dead, which
					// makes all the other nodes critical. So localities "reg1",
					// "reg1,az1", "reg1,az=2" and "reg8" are critical for this range.
					{key: "/Table/t6", stores: "1 2o 4o 5i 8i"},
				},
				nodes: []node{
					{id: 1, stores: []store{{id: 1}}, locality: "region=reg1,az=az1"},
					{id: 2, stores: []store{{id: 2}}, locality: "region=reg1,az=az2"},
					{id: 3, stores: []store{{id: 3}}},
					{id: 4, stores: []store{{id: 4}}, dead: true},
					{id: 5, stores: []store{{id: 5}}, dead: true},
					{id: 6, stores: []store{{id: 6}}, dead: true},
					{id: 7, stores: []store{{id: 7}}, dead: true},
					{id: 8, stores: []store{{id: 8}}, locality: "region=reg8"},
				},
			},
			exp: []criticalLocality{
				{object: "db1", locality: "region=reg1", atRiskRanges: 3},
				{object: "t1", locality: "region=reg1", atRiskRanges: 8},
				{object: "db1", locality: "region=reg1,az=az1", atRiskRanges: 1},
				{object: "db1", locality: "region=reg1,az=az2", atRiskRanges: 1},
				{object: "default", locality: "region=reg1", atRiskRanges: 2},
				{object: "t6", locality: "region=reg1", atRiskRanges: 1},
				{object: "t6", locality: "region=reg1,az=az1", atRiskRanges: 1},
				{object: "t6", locality: "region=reg1,az=az2", atRiskRanges: 1},
				{object: "t6", locality: "region=reg8", atRiskRanges: 1},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCriticalLocalitiesTestCase(ctx, t, tc)
		})
	}
}

func runCriticalLocalitiesTestCase(
	ctx context.Context, t *testing.T, tc criticalLocalitiesTestCase,
) {
	ctc, err := compileTestCase(tc.baseReportTestCase)
	require.NoError(t, err)

	rep, err := computeCriticalLocalitiesReport(
		ctx, ctc.nodeLocalities, &ctc.iter, ctc.checker, ctc.cfg, ctc.resolver,
	)
	require.NoError(t, err)

	gotRows := make([]criticalLocality, len(rep))
	i := 0
	for k, v := range rep {
		gotRows[i] = criticalLocality{
			object:       ctc.zoneToObject[k.ZoneKey],
			locality:     k.locality,
			atRiskRanges: v.atRiskRanges,
		}
		i++
	}
	// Sort the report's keys.
	sort.Slice(gotRows, func(i, j int) bool {
		return gotRows[i].less(gotRows[j])
	})
	sort.Slice(tc.exp, func(i, j int) bool {
		return tc.exp[i].less(tc.exp[j])
	})

	require.Equal(t, tc.exp, gotRows)
}

func TestCriticalLocalitiesSaving(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// This test uses the cluster as a recipient for a report saved from outside
	// the cluster. We disable the cluster's own production of reports so that it
	// doesn't interfere with the test.
	ReporterInterval.Override(ctx, &st.SV, 0)
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	con := s.InternalExecutor().(sqlutil.InternalExecutor)
	defer s.Stopper().Stop(ctx)

	// Verify that tables are empty.
	require.ElementsMatch(t, TableData(ctx, "system.replication_constraint_stats", con), [][]string{})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{})

	// Add several localities and verify the result
	report := make(LocalityReport)
	report.CountRangeAtRisk(MakeZoneKey(1, 3), "region=US")
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")
	report.CountRangeAtRisk(MakeZoneKey(7, 8), "dc=B")
	report.CountRangeAtRisk(MakeZoneKey(7, 8), "dc=B")

	time1 := time.Date(2001, 1, 1, 10, 0, 0, 0, time.UTC)
	saver := makeReplicationCriticalLocalitiesReportSaver()
	require.NoError(t, saver.Save(ctx, report, time1, db, con))
	report = make(LocalityReport)

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"1", "3", "'region=US'", "2", "1"},
		{"5", "6", "'dc=A'", "2", "1"},
		{"7", "8", "'dc=B'", "2", "2"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 10:00:00+00:00'"},
	})
	require.Equal(t, 3, saver.LastUpdatedRowCount())

	// Add new set of localities and verify the old ones are deleted
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")
	report.CountRangeAtRisk(MakeZoneKey(7, 8), "dc=B")
	report.CountRangeAtRisk(MakeZoneKey(7, 8), "dc=B")
	report.CountRangeAtRisk(MakeZoneKey(15, 6), "dc=A")

	time2 := time.Date(2001, 1, 1, 11, 0, 0, 0, time.UTC)
	require.NoError(t, saver.Save(ctx, report, time2, db, con))
	report = make(LocalityReport)

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"5", "6", "'dc=A'", "2", "2"},
		{"7", "8", "'dc=B'", "2", "2"},
		{"15", "6", "'dc=A'", "2", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 11:00:00+00:00'"},
	})
	require.Equal(t, 3, saver.LastUpdatedRowCount())

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
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")
	report.CountRangeAtRisk(MakeZoneKey(7, 8), "dc=B")
	report.CountRangeAtRisk(MakeZoneKey(7, 8), "dc=B")
	report.CountRangeAtRisk(MakeZoneKey(15, 6), "dc=A")

	time4 := time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, saver.Save(ctx, report, time4, db, con))
	report = make(LocalityReport)

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"5", "6", "'dc=A'", "2", "3"},
		{"7", "8", "'dc=B'", "2", "2"},
		{"15", "6", "'dc=A'", "2", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 12:00:00+00:00'"},
	})
	require.Equal(t, 2, saver.LastUpdatedRowCount())

	// A brand new report (after restart for example) - still works.
	saver = makeReplicationCriticalLocalitiesReportSaver()
	report.CountRangeAtRisk(MakeZoneKey(5, 6), "dc=A")

	time5 := time.Date(2001, 1, 1, 12, 30, 0, 0, time.UTC)
	require.NoError(t, saver.Save(ctx, report, time5, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_critical_localities", con), [][]string{
		{"5", "6", "'dc=A'", "2", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"2", "'2001-01-01 12:30:00+00:00'"},
	})
	require.Equal(t, 3, saver.LastUpdatedRowCount())
}

// TableData reads a table and returns the rows as strings.
func TableData(
	ctx context.Context, tableName string, executor sqlutil.InternalExecutor,
) [][]string {
	if it, err := executor.QueryIterator(
		ctx, "test-select-"+tableName, nil /* txn */, "select * from "+tableName,
	); err == nil {
		var result [][]string
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			stringRow := make([]string, 0, row.Len())
			for _, item := range row {
				stringRow = append(stringRow, item.String())
			}
			result = append(result, stringRow)
		}
		if err == nil {
			return result
		}
	}
	return nil
}

func TestExpandLocalities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	nodeLocalities := map[roachpb.NodeID]roachpb.Locality{
		roachpb.NodeID(1): {Tiers: []roachpb.Tier{
			{Key: "region", Value: "r1"},
			{Key: "dc", Value: "dc1"},
			{Key: "az", Value: "az1"},
		}},
		roachpb.NodeID(2): {Tiers: []roachpb.Tier{
			{Key: "region", Value: "r2"},
			{Key: "dc", Value: "dc2"},
			{Key: "az", Value: "az2"},
		}},
	}
	expanded := expandLocalities(nodeLocalities)
	require.Equal(t,
		map[roachpb.NodeID]map[string]roachpb.Locality{
			1: {
				"region=r1":               {Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}}},
				"region=r1,dc=dc1":        {Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc1"}}},
				"region=r1,dc=dc1,az=az1": {Tiers: []roachpb.Tier{{Key: "region", Value: "r1"}, {Key: "dc", Value: "dc1"}, {Key: "az", Value: "az1"}}},
			},
			2: {
				"region=r2":               {Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}}},
				"region=r2,dc=dc2":        {Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc2"}}},
				"region=r2,dc=dc2,az=az2": {Tiers: []roachpb.Tier{{Key: "region", Value: "r2"}, {Key: "dc", Value: "dc2"}, {Key: "az", Value: "az2"}}},
			},
		},
		expanded,
	)
}
