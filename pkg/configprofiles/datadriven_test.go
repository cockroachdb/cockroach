// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package configprofiles_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/configprofiles"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var alreadyStarted bool
		var provider acprovider.Provider
		var s serverutils.TestServerInterface
		var db *sqlutils.SQLRunner
		defer func() {
			if s == nil {
				return
			}
			s.Stopper().Stop(ctx)
		}()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "profile":
				if alreadyStarted {
					t.Fatalf("%s: cannot use profile more than once", d.Pos)
				}
				setter := configprofiles.NewProfileSetter(&provider)
				if err := setter.Set(d.Input); err != nil {
					t.Fatalf("%s: %v", d.Pos, err)
				}
				var res strings.Builder
				fmt.Fprintf(&res, "canonical profile name: %s\n", setter.String())

				numExpectedTasks := len(configprofiles.TestingGetProfiles()[setter.String()])

				s, _, _ = serverutils.StartServer(t, base.TestServerArgs{
					AutoConfigProvider: provider,
					// This test does not exercise security parameters, so we
					// keep the configuration simpler to keep the test code also
					// simple.
					Insecure: true,
					// The test controls secondary tenants manually.
					DisableDefaultTestTenant: true,
				})
				// We need to force the connection to the system tenant,
				// because at least one of the config profiles changes the
				// default tenant.
				sysTenantDB := serverutils.OpenDBConn(t, s.SQLAddr(), "cluster:system/defaultdb",
					true /* insecure */, s.Stopper())
				db = sqlutils.MakeSQLRunner(sysTenantDB)
				res.WriteString("server started\n")

				testutils.SucceedsSoon(t, func() error {
					var numTasksCompleted int
					db.QueryRow(t, `SELECT count(*)
FROM [SHOW AUTOMATIC JOBS]
WHERE job_type = 'AUTO CONFIG TASK'
AND   status = 'succeeded'`).Scan(&numTasksCompleted)
					if numTasksCompleted < numExpectedTasks {
						return fmt.Errorf("expected %d tasks to be completed, got %d", numExpectedTasks, numTasksCompleted)
					}
					return nil
				})

				alreadyStarted = true

				return res.String()

			case "system-sql":
				if !alreadyStarted {
					t.Fatalf("%s: must use profile before sql", d.Pos)
				}
				var res strings.Builder
				rows := db.QueryStr(t, d.Input)
				if len(rows) == 0 {
					res.WriteString("<no rows>\n")
				} else {
					for _, row := range rows {
						res.WriteString(strings.Join(row, " "))
						res.WriteString("\n")
					}
				}
				return res.String()

			case "connect-tenant":
				if !alreadyStarted {
					t.Fatalf("%s: must use profile before sql", d.Pos)
				}
				sqlAddr := s.(*server.TestServer).SQLAddr()
				testutils.SucceedsSoon(t, func() error {
					goDB := serverutils.OpenDBConn(t, sqlAddr, "cluster:"+d.Input+"/defaultdb", true /* insecure */, s.Stopper())
					return goDB.Ping()
				})
				return "ok"

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
