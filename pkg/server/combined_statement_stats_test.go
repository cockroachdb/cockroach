// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestGetCombinedStatementsQueryClausesAndArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	settings := cluster.MakeClusterSettings()
	testingKnobs := &sqlstats.TestingKnobs{}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "combined_statement_stats"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "query":
			var limit int64
			var start int64
			var end int64
			var sortString string
			if d.HasArg("sort") {
				d.ScanArgs(t, "sort", &sortString)
			}
			if d.HasArg("limit") {
				d.ScanArgs(t, "limit", &limit)
			}
			if d.HasArg("start") {
				d.ScanArgs(t, "start", &start)
			}
			if d.HasArg("end") {
				d.ScanArgs(t, "end", &end)
			}

			gotWhereClause, gotOrderAndLimitClause, gotArgs := getCombinedStatementsQueryClausesAndArgs(
				&serverpb.CombinedStatementsStatsRequest{
					Start: start,
					End:   end,
					FetchMode: &serverpb.CombinedStatementsStatsRequest_FetchMode{
						StatsType: serverpb.CombinedStatementsStatsRequest_StmtStatsOnly,
						Sort:      serverpb.StatsSortOptions(serverpb.StatsSortOptions_value[sortString]),
					},
					Limit: limit,
				},
				testingKnobs,
				false,
				settings,
			)

			return fmt.Sprintf("--WHERE--\n%s\n--ORDER AND LIMIT--\n%s\n--ARGS--\n%v", gotWhereClause, gotOrderAndLimitClause, gotArgs)
		default:
			t.Fatalf("unknown cmd: %s", d.Cmd)
		}
		return ""
	})
}
