// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

const (
	timeArgs                 = "time"
	dbNameArgs               = "db"
	implicitTxnArgs          = "implicitTxn"
	fingerprintArgs          = "fingerprint"
	eventArgs                = "event"
	eventCallbackCmd         = "callbackCmd"
	eventCallbackCmdArgKey   = "callbackCmdArgKey"
	eventCallbackCmdArgValue = "callbackCmdArgValue"
)

// TestSQLStatsDataDriven runs the data-driven tests in
// pkg/sql/sqlstats/persistedsqlstats/testdata. It has the following directives:
//   - exec-sql: executes SQL statements in the exec connection. This should be
//     executed under a specific app_name in order to get deterministic
//     results when testing for stmt/txn statistics. No output will be
//     returned for SQL statements executed under this directive.
//   - observe-sql: executes SQL statements in the observer connection. This
//     should be executed under a different app_name. This is used
//     to test the statement/transaction statistics executed under
//     exec-sql. Running them in different connection ensures that
//     observing statements will not mess up the statistics of the
//     statements that they are observing.
//   - sql-stats-flush: this triggers the SQL Statistics to be flushed into
//     system table.
//   - set-time: this changes the clock time perceived by SQL Stats subsystem.
//     This is useful when unit tests need to manipulate times.
//   - should-sample: this checks if the given tuple of
//     (db, implicitTxn, fingerprint) will be sampled
//     next time it is being executed.
func TestSQLStatsDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t)

	stubTime := &stubTime{}
	injector := newRuntimeKnobsInjector()

	ctx := context.Background()
	var params base.TestServerArgs
	knobs := sqlstats.CreateTestingKnobs()
	knobs.StubTimeNow = stubTime.Now
	knobs.OnStmtStatsFlushFinished = injector.invokePostStmtStatsFlushCallback
	knobs.OnTxnStatsFlushFinished = injector.invokePostTxnStatsFlushCallback
	params.Knobs.SQLStatsKnobs = knobs

	cluster := serverutils.StartCluster(t, 3 /* numNodes */, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer cluster.Stopper().Stop(ctx)

	server := cluster.Server(0 /* idx */).ApplicationLayer()
	sqlStats := server.SQLServer().(*sql.Server).GetSQLStatsProvider()

	appStats := sqlStats.GetApplicationStats("app1")

	// Open two connections so that we can run statements without messing up
	// the SQL stats.
	sqlConn := cluster.ServerConn(0 /* idx */)
	observerConn := cluster.ServerConn(1 /* idx */)

	observer := sqlutils.MakeSQLRunner(observerConn)

	execDataDrivenTestCmd := func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "exec-sql":
			stmts := strings.Split(d.Input, "\n")
			for i := range stmts {
				testutils.SucceedsSoon(t, func() error {
					_, exSqlErr := sqlConn.Exec(stmts[i])
					if exSqlErr != nil {
						return errors.NewAssertionErrorWithWrappedErrf(exSqlErr, "failed to execute stmt %s", stmts[i])
					}
					return nil
				})
			}
		case "observe-sql":
			actual := observer.QueryStr(t, d.Input)
			rows := make([]string, len(actual))

			for rowIdx := range actual {
				rows[rowIdx] = strings.Join(actual[rowIdx], ",")
			}
			return strings.Join(rows, "\n")
		case "sql-stats-flush":
			sqlStats.MaybeFlush(ctx, cluster.ApplicationLayer(0).AppStopper())
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
		case "should-sample":
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

			previouslySampled := appStats.StatementSampled(
				fingerprint,
				implicitTxn,
				dbName,
			)
			return fmt.Sprintf("%t", previouslySampled)
		case "skip":
			var issue int
			d.ScanArgs(t, "issue-num", &issue)
			skip.WithIssue(t, issue)
			return ""
		}

		return ""
	}

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if d.Cmd == "register-callback" {
				mustHaveArgsOrFatal(
					t,
					d,
					eventArgs,
					eventCallbackCmd,
					eventCallbackCmdArgKey,
					eventCallbackCmdArgValue,
				)

				var event string
				var callbackCmd string
				var callbackCmdArgKey string
				var callbackCmdArgValue string

				d.ScanArgs(t, eventArgs, &event)
				d.ScanArgs(t, eventCallbackCmd, &callbackCmd)
				d.ScanArgs(t, eventCallbackCmdArgKey, &callbackCmdArgKey)
				d.ScanArgs(t, eventCallbackCmdArgValue, &callbackCmdArgValue)

				injectedDataDrivenCmd := &datadriven.TestData{
					Cmd: callbackCmd,
					CmdArgs: []datadriven.CmdArg{
						{
							Key: callbackCmdArgKey,
							Vals: []string{
								callbackCmdArgValue,
							},
						},
					},
				}

				switch event {
				case "stmt-stats-flushed":
					injector.registerPostStmtStatsFlushCallback(func() {
						execDataDrivenTestCmd(t, injectedDataDrivenCmd)
					})
				case "txn-stats-flushed":
					injector.registerPostTxnStatsFlushCallback(func() {
						execDataDrivenTestCmd(t, injectedDataDrivenCmd)
					})
				default:
					return "invalid callback event"
				}
			} else if d.Cmd == "unregister-callback" {
				mustHaveArgsOrFatal(t, d, eventArgs)

				var event string
				d.ScanArgs(t, eventArgs, &event)

				switch event {
				case "stmt-stats-flushed":
					injector.unregisterPostStmtStatsFlushCallback()
				case "txn-stats-flushed":
					injector.unregisterPostTxnStatsFlushCallback()
				default:
					return "invalid event"
				}
			}
			return execDataDrivenTestCmd(t, d)
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

type runtimeKnobsInjector struct {
	mu struct {
		syncutil.Mutex
		knobs *sqlstats.TestingKnobs
	}
}

func newRuntimeKnobsInjector() *runtimeKnobsInjector {
	r := &runtimeKnobsInjector{}
	r.mu.knobs = &sqlstats.TestingKnobs{}
	return r
}

func (r *runtimeKnobsInjector) registerPostStmtStatsFlushCallback(cb func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.knobs.OnStmtStatsFlushFinished = cb
}

func (r *runtimeKnobsInjector) unregisterPostStmtStatsFlushCallback() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.knobs.OnStmtStatsFlushFinished = nil
}

func (r *runtimeKnobsInjector) invokePostStmtStatsFlushCallback() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.knobs.OnStmtStatsFlushFinished != nil {
		r.mu.knobs.OnStmtStatsFlushFinished()
	}
}

func (r *runtimeKnobsInjector) registerPostTxnStatsFlushCallback(cb func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.knobs.OnTxnStatsFlushFinished = cb
}

func (r *runtimeKnobsInjector) unregisterPostTxnStatsFlushCallback() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.knobs.OnTxnStatsFlushFinished = nil
}

func (r *runtimeKnobsInjector) invokePostTxnStatsFlushCallback() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.knobs.OnTxnStatsFlushFinished != nil {
		r.mu.knobs.OnTxnStatsFlushFinished()
	}
}
