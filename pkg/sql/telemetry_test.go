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
	"sort"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
//  - feature-whitelist
//
//    The input for this command is not SQL, but just a list of counter values.
//    Tests that follow (until the next feature-whitelist command) will only
//    output counters in this white list.
//
//  - feature-usage, feature-counters
//
//    Executes SQL statements and then outputs the feature counters from the
//    white list that have been reported to the diagnostic server. The first
//    variant outputs only the names of the counters that changed; the second
//    variant outputs the counts as well. It is necessary to use
//    feature-whitelist before these commands to avoid test flakes (e.g. because
//    of counters that are changed by looking up descriptors)
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

		var featureKeyWhitelist map[string]struct{}
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

			case "feature-whitelist":
				featureKeyWhitelist = make(map[string]struct{})
				for _, k := range strings.Split(td.Input, "\n") {
					featureKeyWhitelist[k] = struct{}{}
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
					if featureKeyWhitelist != nil {
						if _, ok := featureKeyWhitelist[k]; !ok {
							// Feature key not in whitelist.
							continue
						}
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

			default:
				td.Fatalf(t, "unknown command %s", td.Cmd)
				return ""
			}
		})
	})
}
