// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package persistedsqlstats_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

const (
	timeArgs        = "time"
	dbNameArgs      = "db"
	implicitTxnArgs = "implicitTxn"
	fingerprintArgs = "fingerprint"
)

// TestSQLStatsDataDriven runs the data-driven tests in
// pkg/sql/sqlstats/persistedsqlstats/testdata. It has the following directives:
// * exec-sql: executes SQL statements in the exec connection. This should be
//             executed under a specific app_name in order to get deterministic
//             results when testing for stmt/txn statistics. No output will be
//             returned for SQL statements executed under this directive.
// * observe-sql: executes SQL statements in the observer connection. This
//                should be executed under a different app_name. This is used
//                to test the statement/transaction statistics executed under
//                exec-sql. Running them in different connection ensures that
//                observing statements will not mess up the statistics of the
//                statements that they are observing.
// * sql-stats-flush: this triggers the SQL Statistics to be flushed into
//                    system table.
// * set-time: this changes the clock time perceived by SQL Stats subsystem.
//             This is useful when unit tests need to manipulate times.
// * should-sample-logical-plan: this checks if the given tuple of
//                               (db, implicitTxn, fingerprint) will be sampled
//                               next time it is being executed.
func TestSQLStatsDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stubTime := &stubTime{}
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLStatsKnobs.(*sqlstats.TestingKnobs).StubTimeNow = stubTime.Now

	cluster := serverutils.StartNewTestCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer cluster.Stopper().Stop(ctx)

	server := cluster.Server(0 /* idx */)
	sqlStats := server.SQLServer().(*sql.Server).GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats)

	appStats := sqlStats.GetApplicationStats("app1")

	// Open two connections so that we can run statements without messing up
	// the SQL stats.
	sqlConn := cluster.ServerConn(0 /* idx */)
	observerConn := cluster.ServerConn(1 /* idx */)

	observer := sqlutils.MakeSQLRunner(observerConn)

	datadriven.Walk(t, "testdata/", func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "exec-sql":
				stmts := strings.Split(d.Input, "\n")
				for i := range stmts {
					_, err := sqlConn.Exec(stmts[i])
					if err != nil {
						t.Errorf("failed to execute stmt %s due to %s", stmts[i], err.Error())
					}
				}
			case "observe-sql":
				actual := observer.QueryStr(t, d.Input)
				rows := make([]string, len(actual))

				for rowIdx := range actual {
					rows[rowIdx] = strings.Join(actual[rowIdx], ",")
				}
				return strings.Join(rows, "\n")
			case "sql-stats-flush":
				sqlStats.Flush(ctx)
			case "set-time":
				mustHaveArgsOrFatal(t, d, timeArgs)

				var timeString string
				d.ScanArgs(t, timeArgs, &timeString)
				tm, err := time.Parse(time.RFC3339, timeString)
				if err != nil {
					return err.Error()
				}
				stubTime.setTime(tm)
				return stubTime.Now().String()
			case "should-sample-logical-plan":
				mustHaveArgsOrFatal(t, d, fingerprintArgs, implicitTxnArgs, dbNameArgs)

				var dbName string
				var implicitTxn bool
				var fingerprint string

				d.ScanArgs(t, fingerprintArgs, &fingerprint)
				d.ScanArgs(t, implicitTxnArgs, &implicitTxn)
				d.ScanArgs(t, dbNameArgs, &dbName)

				// Since the data driven tests framework does not support space
				// in args, we used % as a placeholder for space and manually replace
				// them.
				fingerprint = strings.Replace(fingerprint, "%", " ", -1)

				return fmt.Sprintf("%t",
					appStats.ShouldSaveLogicalPlanDesc(
						fingerprint,
						implicitTxn,
						dbName,
					),
				)
			}

			return ""
		})
	})
}

func mustHaveArgsOrFatal(t *testing.T, d *datadriven.TestData, args ...string) {
	for _, arg := range args {
		if !d.HasArg(arg) {
			t.Fatalf("no %q provided", arg)
		}
	}
}
