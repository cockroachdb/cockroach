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

func TestRangeReport(t *testing.T) {
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
	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{})

	// Add several localities and verify the result
	stats := make(RangeReport)
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       false,
		UnderReplicated: true,
		OverReplicated:  true,
	})
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  true,
	})
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: false,
		OverReplicated:  true,
	})
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       false,
		UnderReplicated: true,
		OverReplicated:  false,
	})
	stats.CountRange(MakeZoneKey(2, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: false,
		OverReplicated:  false,
	})
	stats.CountRange(MakeZoneKey(2, 4), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  false,
	})

	r := makeReplicationStatsReportSaver()
	time1 := time.Date(2001, 1, 1, 10, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, stats, time1, db, con))
	stats = make(RangeReport)

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
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  true,
	})
	stats.CountRange(MakeZoneKey(2, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: false,
		OverReplicated:  false,
	})
	stats.CountRange(MakeZoneKey(4, 4), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  true,
	})

	time2 := time.Date(2001, 1, 1, 11, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, stats, time2, db, con))
	stats = make(RangeReport)

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
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  true,
	})
	stats.CountRange(MakeZoneKey(2, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: false,
		OverReplicated:  false,
	})
	stats.CountRange(MakeZoneKey(4, 4), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  true,
	})

	time4 := time.Date(2001, 1, 1, 12, 0, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, stats, time4, db, con))
	stats = make(RangeReport)

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
	stats.CountRange(MakeZoneKey(1, 3), roachpb.RangeStatusReport{
		Available:       true,
		UnderReplicated: true,
		OverReplicated:  true,
	})

	time5 := time.Date(2001, 1, 1, 12, 30, 0, 0, time.UTC)
	require.NoError(t, r.Save(ctx, stats, time5, db, con))

	require.ElementsMatch(t, TableData(ctx, "system.replication_stats", con), [][]string{
		{"1", "3", "3", "1", "0", "1", "1"},
	})
	require.ElementsMatch(t, TableData(ctx, "system.reports_meta", con), [][]string{
		{"3", "'2001-01-01 12:30:00+00:00'"},
	})
	require.Equal(t, 2, r.LastUpdatedRowCount())
}

type replicationStatsEntry struct {
	zoneRangeStatus
	object string
}

type replicationStatsTestCase struct {
	baseReportTestCase
	name string
	exp  []replicationStatsEntry
}

// runReplicationStatsTest runs one test case. It processes the input schema,
// runs the reports, and verifies that the report looks as expected.
func runReplicationStatsTest(t *testing.T, tc replicationStatsTestCase) {
	ctc, err := compileTestCase(tc.baseReportTestCase)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := computeReplicationStatsReport(context.Background(), &ctc.iter, ctc.checker, ctc.cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Sort the report's keys.
	gotRows := make([]replicationStatsEntry, len(rep))
	i := 0
	for zone, stats := range rep {
		object := ctc.zoneToObject[zone]
		gotRows[i] = replicationStatsEntry{
			zoneRangeStatus: stats,
			object:          object,
		}
		i++
	}
	sort.Slice(gotRows, func(i, j int) bool {
		return strings.Compare(gotRows[i].object, gotRows[j].object) < 0
	})
	sort.Slice(tc.exp, func(i, j int) bool {
		return strings.Compare(tc.exp[i].object, tc.exp[j].object) < 0
	})

	require.Equal(t, tc.exp, gotRows)
}

func TestReplicationStatsReport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tests := []replicationStatsTestCase{
		{
			name: "simple no violations",
			baseReportTestCase: baseReportTestCase{
				defaultZone: zone{replicas: 3},
				schema: []database{
					{
						name: "db1",
						tables: []table{
							{name: "t1",
								partitions: []partition{{
									name:  "p1",
									start: []int{100},
									end:   []int{200},
									zone:  &zone{constraints: "[+p1]"},
								}},
							},
							{name: "t2"},
						},
						zone: &zone{
							// Change replication options so that db1 gets a report entry.
							replicas: 3,
						},
					},
					{
						name:   "db2",
						tables: []table{{name: "sentinel"}},
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
					{
						// This range is not covered by the db1's zone config; it'll be
						// counted for the default zone.
						key: "/Table/sentinel", stores: "1 2 3",
					},
				},
				nodes: []node{
					{id: 1, stores: []store{{id: 1}}},
					{id: 2, stores: []store{{id: 2}}},
					{id: 3, stores: []store{{id: 3}}},
				},
			},
			exp: []replicationStatsEntry{
				{
					object: "default",
					zoneRangeStatus: zoneRangeStatus{
						numRanges:       1,
						unavailable:     0,
						underReplicated: 0,
						overReplicated:  0,
					},
				},
				{
					object: "db1",
					zoneRangeStatus: zoneRangeStatus{
						numRanges:       8,
						unavailable:     0,
						underReplicated: 0,
						overReplicated:  0,
					},
				},
				{
					object: "t1.p1",
					zoneRangeStatus: zoneRangeStatus{
						numRanges:       2,
						unavailable:     0,
						underReplicated: 0,
						overReplicated:  0,
					},
				},
			},
		},
		{
			name: "simple violations",
			baseReportTestCase: baseReportTestCase{
				defaultZone: zone{replicas: 3},
				schema: []database{
					{
						name: "db1",
						tables: []table{
							{name: "t1",
								partitions: []partition{{
									name:  "p1",
									start: []int{100},
									end:   []int{200},
									zone:  &zone{constraints: "[+p1]"},
								}},
							},
						},
					},
				},
				splits: []split{
					// No problem.
					{key: "/Table/t1/pk/100", stores: "1 2 3"},
					// Under-replicated.
					{key: "/Table/t1/pk/101", stores: "1"},
					// Under-replicated.
					{key: "/Table/t1/pk/102", stores: "1 2"},
					// Under-replicated because 4 is dead.
					{key: "/Table/t1/pk/103", stores: "1 2 4"},
					// Under-replicated and unavailable.
					{key: "/Table/t1/pk/104", stores: "3"},
					// Over-replicated.
					{key: "/Table/t1/pk/105", stores: "1 2 3 4"},
					// Under-replicated and over-replicated.
					{key: "/Table/t1/pk/106", stores: "1 2 4 5"},
				},
				nodes: []node{
					{id: 1, stores: []store{{id: 1}}},
					{id: 2, stores: []store{{id: 2}}},
					{id: 3, stores: []store{{id: 3}}},
					{id: 4, stores: []store{{id: 4}}, dead: true},
					{id: 5, stores: []store{{id: 5}}, dead: true},
				},
			},
			exp: []replicationStatsEntry{
				{
					object: "t1.p1",
					zoneRangeStatus: zoneRangeStatus{
						numRanges:       7,
						unavailable:     1,
						underReplicated: 5,
						overReplicated:  2,
					},
				},
			},
		},
		{
			name: "joint consensus",
			baseReportTestCase: baseReportTestCase{
				defaultZone: zone{replicas: 3},
				schema: []database{
					{
						name: "db1",
						tables: []table{
							{name: "t1"},
						},
					},
				},
				splits: []split{
					// No problem.
					{key: "/Table/t1/pk/100", stores: "1v 2v 3v"},
					// Under-replication in the "old group".
					{key: "/Table/t1/pk/101", stores: "1v 2v 3i"},
					// Under-replication in the "new group".
					{key: "/Table/t1/pk/102", stores: "1v 2v 3o"},
					// Under-replicated in the old group because 4 is dead.
					{key: "/Table/t1/pk/103", stores: "1v 2v 4o 3i"},
					// Unavailable in the new group (and also under-replicated), and also
					// over-replicated in the new group.
					{key: "/Table/t1/pk/104", stores: "1v 2v 3o 4i 5i"},
					// Over-replicated in the new group.
					{key: "/Table/t1/pk/105", stores: "1v 2v 3o 5i 6i"},
					// Many learners. No problems, since learners don't count.
					{key: "/Table/t1/pk/106", stores: "1v 2v 3v 4l 5l 6l"},
					// Underreplicated. Learners don't count.
					{key: "/Table/t1/pk/107", stores: "1v 2v 3l"},
				},
				nodes: []node{
					{id: 1, stores: []store{{id: 1}}},
					{id: 2, stores: []store{{id: 2}}},
					{id: 3, stores: []store{{id: 3}}},
					{id: 4, stores: []store{{id: 4}}, dead: true},
					{id: 5, stores: []store{{id: 5}}, dead: true},
					{id: 6, stores: []store{{id: 6}}},
				},
			},
			exp: []replicationStatsEntry{
				{
					object: "default",
					zoneRangeStatus: zoneRangeStatus{
						numRanges:       8,
						unavailable:     1,
						underReplicated: 5,
						overReplicated:  2,
					},
				},
			},
		}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runReplicationStatsTest(t, tc)
		})
	}
}
