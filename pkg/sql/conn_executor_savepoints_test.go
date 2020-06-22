// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	datadriven.Walk(t, "testdata/savepoints", func(t *testing.T, path string) {

		params := base.TestServerArgs{}
		s, sqlConn, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		if _, err := sqlConn.Exec("CREATE TABLE progress(n INT, marker BOOL)"); err != nil {
			t.Fatal(err)
		}

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "sql":
				// Implicitly abort any previously-ongoing txn.
				_, _ = sqlConn.Exec("ABORT")
				// Prepare for the next test.
				if _, err := sqlConn.Exec("DELETE FROM progress"); err != nil {
					td.Fatalf(t, "cleaning up: %v", err)
				}

				// Prepare a buffer to accumulate the results.
				var buf strings.Builder

				// We're going to execute the input line-by-line.
				stmts := strings.Split(td.Input, "\n")

				// progressBar is going to show the cancellation of writes
				// during rollbacks.
				progressBar := make([]byte, len(stmts))
				erase := func(status string) {
					char := byte('.')
					if !isOpenTxn(status) {
						char = 'X'
					}
					for i := range progressBar {
						progressBar[i] = char
					}
				}

				// stepNum is the index of the current statement
				// in the input.
				var stepNum int

				// updateProgress loads the current set of writes
				// into the progress bar.
				updateProgress := func() {
					rows, err := sqlConn.Query("SELECT n FROM progress")
					if err != nil {
						t.Logf("%d: reading progress: %v", stepNum, err)
						// It's OK if we can't read this.
						return
					}
					defer rows.Close()
					for rows.Next() {
						var n int
						if err := rows.Scan(&n); err != nil {
							td.Fatalf(t, "%d: unexpected error while reading progress: %v", stepNum, err)
						}
						if n < 1 || n > len(progressBar) {
							td.Fatalf(t, "%d: unexpected stepnum in progress table: %d", stepNum, n)
						}
						progressBar[n-1] = '#'
					}
				}

				// getTxnStatus retrieves the current txn state.
				// This is guaranteed to always succeed because SHOW TRANSACTION STATUS
				// is an observer statement.
				getTxnStatus := func() string {
					row := sqlConn.QueryRow("SHOW TRANSACTION STATUS")
					var status string
					if err := row.Scan(&status); err != nil {
						td.Fatalf(t, "%d: unable to retrieve txn status: %v", stepNum, err)
					}
					return status
				}
				// showSavepointStatus is like getTxnStatus but retrieves the
				// savepoint stack.
				showSavepointStatus := func() {
					rows, err := sqlConn.Query("SHOW SAVEPOINT STATUS")
					if err != nil {
						td.Fatalf(t, "%d: unable to retrieve savepoint status: %v", stepNum, err)
					}
					defer rows.Close()

					comma := ""
					hasSavepoints := false
					for rows.Next() {
						var name string
						var isRestart bool
						if err := rows.Scan(&name, &isRestart); err != nil {
							td.Fatalf(t, "%d: unexpected error while reading savepoints: %v", stepNum, err)
						}
						if isRestart {
							name += "(r)"
						}
						buf.WriteString(comma)
						buf.WriteString(name)
						hasSavepoints = true
						comma = ">"
					}
					if !hasSavepoints {
						buf.WriteString("(none)")
					}
				}
				// report shows the progress of execution so far after
				// each statement executed.
				report := func(beforeStatus, afterStatus string) {
					erase(afterStatus)
					if isOpenTxn(afterStatus) {
						updateProgress()
					}
					fmt.Fprintf(&buf, "-- %-11s -> %-11s %s ", beforeStatus, afterStatus, string(progressBar))
					buf.WriteByte(' ')
					showSavepointStatus()
					buf.WriteByte('\n')
				}

				// The actual execution of the statements starts here.

				beforeStatus := getTxnStatus()
				for i, stmt := range stmts {
					stepNum = i + 1
					// Before each statement, mark the progress so far with
					// a KV write.
					if isOpenTxn(beforeStatus) {
						_, err := sqlConn.Exec("INSERT INTO progress(n, marker) VALUES ($1, true)", stepNum)
						if err != nil {
							td.Fatalf(t, "%d: before-stmt: %v", stepNum, err)
						}
					}

					// Run the statement and report errors/results.
					fmt.Fprintf(&buf, "%d: %s -- ", stepNum, stmt)
					execRes, err := sqlConn.Exec(stmt)
					if err != nil {
						fmt.Fprintf(&buf, "%v\n", err)
					} else {
						nRows, err := execRes.RowsAffected()
						if err != nil {
							fmt.Fprintf(&buf, "error retrieving rows: %v\n", err)
						} else {
							fmt.Fprintf(&buf, "%d row%s\n", nRows, util.Pluralize(nRows))
						}
					}

					// Report progress on the next line
					afterStatus := getTxnStatus()
					report(beforeStatus, afterStatus)
					beforeStatus = afterStatus
				}

				return buf.String()

			default:
				td.Fatalf(t, "unknown directive: %s", td.Cmd)
			}
			return ""
		})
	})
}

func isOpenTxn(status string) bool {
	return status == sql.OpenStateStr || status == sql.NoTxnStateStr
}
