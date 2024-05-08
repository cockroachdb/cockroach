// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

func TestGetCombinedStatementsQueryClausesAndArgs(t *testing.T) {
	settings := cluster.MakeClusterSettings()
	testingKnobs := &sqlstats.TestingKnobs{}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "combined_statement_stats"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "query":
			var limit int
			var sortString string
			var internal bool
			d.ScanArgs(t, "sort", &sortString)
			d.ScanArgs(t, "limit", &limit)
			d.ScanArgs(t, "internal", &internal)

			gotWhereClause, gotOrderAndLimitClause, gotArgs := getCombinedStatementsQueryClausesAndArgs(
				&serverpb.CombinedStatementsStatsRequest{
					FetchMode: &serverpb.CombinedStatementsStatsRequest_FetchMode{
						StatsType: serverpb.CombinedStatementsStatsRequest_StmtStatsOnly,
						Sort:      serverpb.StatsSortOptions(serverpb.StatsSortOptions_value[sortString]),
					},
				},
				testingKnobs,
				internal,
				settings,
			)

			return fmt.Sprintf("--WHERE--\n%s\n--ORDER AND LIMIT--\n%s\n--ARGS--\n%v", gotWhereClause, gotOrderAndLimitClause, gotArgs)
		default:
			t.Fatalf("unknown cmd: %s", d.Cmd)
		}
		return ""
	})
}
