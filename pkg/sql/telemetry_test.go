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
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// TestTelemetry runs the datadriven telemetry tests. The test sets up a
// database and a testing diagnostics reporting server. The test implements the
// following data-driven commands:
//
//  - exec
//
//    Executes SQL statements against the database. Outputs no results on
//    success. In case of error, outputs the error message.
//
//  - feature-allowlist
//
//    The input for this command is not SQL, but a list of regular expressions.
//    Tests that follow (until the next feature-allowlist command) will only
//    output counters that match a regexp in this allow list.
//
//  - feature-usage, feature-counters
//
//    Executes SQL statements and then outputs the feature counters from the
//    allowlist that have been reported to the diagnostic server. The first
//    variant outputs only the names of the counters that changed; the second
//    variant outputs the counts as well. It is necessary to use
//    feature-allowlist before these commands to avoid test flakes (e.g. because
//    of counters that are changed by looking up descriptors)
//
//  - schema
//
//    Outputs reported schema information.
//
//  - sql-stats
//
//    Executes SQL statements and then outputs information about reported sql
//    statement statistics.
//
func TestTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	// Note: these tests cannot be run in parallel (with each other or with other
	// tests) because telemetry counters are global.
	datadriven.Walk(t, "testdata/telemetry", func(t *testing.T, path string) {
		// Disable cloud info reporting (it would make these tests really slow).
		defer cloudinfo.Disable()()

		diagSrv := diagutils.NewServer()
		defer diagSrv.Close()

		diagSrvURL := diagSrv.URL()
		params := base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DiagnosticsTestingKnobs: diagnosticspb.TestingKnobs{
						OverrideReportingURL: &diagSrvURL,
					},
				},
			},
		}
		s, sqlConn, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		runner := sqlutils.MakeSQLRunner(sqlConn)
		// Disable automatic reporting so it doesn't interfere with the test.
		runner.Exec(t, "SET CLUSTER SETTING diagnostics.reporting.enabled = false")
		runner.Exec(t, "SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false")
		// Disable plan caching to get accurate counts if the same statement is
		// issued multiple times.
		runner.Exec(t, "SET CLUSTER SETTING sql.query_cache.enabled = false")

		var allowlist featureAllowlist
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "exec":
				_, err := sqlConn.Exec(td.Input)
				if err != nil {
					if errors.HasAssertionFailure(err) {
						td.Fatalf(t, "%+v", err)
					}
					return fmt.Sprintf("error: %v\n", err)
				}
				return ""

			case "schema":
				s.ReportDiagnostics(ctx)
				last := diagSrv.LastRequestData()
				var buf bytes.Buffer
				for i := range last.Schema {
					buf.WriteString(formatTableDescriptor(&last.Schema[i]))
				}
				return buf.String()

			case "feature-allowlist":
				var err error
				allowlist, err = makeAllowlist(strings.Split(td.Input, "\n"))
				if err != nil {
					td.Fatalf(t, "error parsing feature regex: %s", err)
				}
				return ""

			case "feature-usage", "feature-counters":
				// Report diagnostics once to reset the counters.
				s.ReportDiagnostics(ctx)
				_, err := sqlConn.Exec(td.Input)
				var buf bytes.Buffer
				if err != nil {
					fmt.Fprintf(&buf, "error: %v\n", err)
				}
				s.ReportDiagnostics(ctx)
				last := diagSrv.LastRequestData()
				usage := last.FeatureUsage
				keys := make([]string, 0, len(usage))
				for k, v := range usage {
					if v == 0 {
						// Ignore zero values (shouldn't happen in practice)
						continue
					}
					if !allowlist.Match(k) {
						// Feature key not in allowlist.
						continue
					}
					keys = append(keys, k)
				}
				sort.Strings(keys)
				tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
				for _, k := range keys {
					// Report either just the key or the key and the count.
					if td.Cmd == "feature-counters" {
						fmt.Fprintf(tw, "%s\t%d\n", k, usage[k])
					} else {
						fmt.Fprintf(tw, "%s\n", k)
					}
				}
				_ = tw.Flush()
				return buf.String()

			case "sql-stats":
				// Report diagnostics once to reset the stats.
				s.SQLServer().(*sql.Server).ResetSQLStats(ctx)
				s.ReportDiagnostics(ctx)

				_, err := sqlConn.Exec(td.Input)
				var buf bytes.Buffer
				if err != nil {
					fmt.Fprintf(&buf, "error: %v\n", err)
				}
				s.SQLServer().(*sql.Server).ResetSQLStats(ctx)
				s.ReportDiagnostics(ctx)
				last := diagSrv.LastRequestData()
				buf.WriteString(formatSQLStats(last.SqlStats))
				return buf.String()

			default:
				td.Fatalf(t, "unknown command %s", td.Cmd)
				return ""
			}
		})
	})
}

type featureAllowlist []*regexp.Regexp

func makeAllowlist(strings []string) (featureAllowlist, error) {
	w := make(featureAllowlist, len(strings))
	for i := range strings {
		var err error
		w[i], err = regexp.Compile("^" + strings[i] + "$")
		if err != nil {
			return nil, err
		}
	}
	return w, nil
}

func (w featureAllowlist) Match(feature string) bool {
	if w == nil {
		// Unset allowlist matches all counters.
		return true
	}
	for _, r := range w {
		if r.MatchString(feature) {
			return true
		}
	}
	return false
}

func formatTableDescriptor(desc *sqlbase.TableDescriptor) string {
	tp := treeprinter.New()
	n := tp.Childf("table:%s", desc.Name)
	cols := n.Child("columns")
	for _, col := range desc.Columns {
		var colBuf bytes.Buffer
		fmt.Fprintf(&colBuf, "%s:%s", col.Name, col.Type.String())
		if col.DefaultExpr != nil {
			fmt.Fprintf(&colBuf, " default: %s", *col.DefaultExpr)
		}
		if col.ComputeExpr != nil {
			fmt.Fprintf(&colBuf, " computed: %s", *col.ComputeExpr)
		}
		cols.Child(colBuf.String())
	}
	if len(desc.Checks) > 0 {
		checks := n.Child("checks")
		for _, chk := range desc.Checks {
			checks.Childf("%s: %s", chk.Name, chk.Expr)
		}
	}
	return tp.String()
}

func formatSQLStats(stats []roachpb.CollectedStatementStatistics) string {
	bucketByApp := make(map[string][]roachpb.CollectedStatementStatistics)
	for i := range stats {
		s := &stats[i]

		if strings.HasPrefix(s.Key.App, sqlbase.InternalAppNamePrefix) {
			// Let's ignore all internal queries for this test.
			continue
		}
		bucketByApp[s.Key.App] = append(bucketByApp[s.Key.App], *s)
	}
	var apps []string
	for app, s := range bucketByApp {
		apps = append(apps, app)
		sort.Slice(s, func(i, j int) bool {
			return s[i].Key.Query < s[j].Key.Query
		})
		bucketByApp[app] = s
	}
	sort.Strings(apps)
	tp := treeprinter.New()
	n := tp.Child("sql-stats")

	for _, app := range apps {
		nodeApp := n.Child(app)
		for _, s := range bucketByApp[app] {
			var flags []string
			if s.Key.Failed {
				flags = append(flags, "failed")
			}
			if !s.Key.DistSQL {
				flags = append(flags, "nodist")
			}
			var buf bytes.Buffer
			if len(flags) > 0 {
				buf.WriteString("[")
				for i := range flags {
					if i > 0 {
						buf.WriteByte(',')
					}
					buf.WriteString(flags[i])
				}
				buf.WriteString("] ")
			}
			buf.WriteString(s.Key.Query)
			nodeApp.Child(buf.String())
		}
	}
	return tp.String()
}
