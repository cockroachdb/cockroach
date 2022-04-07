// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package systemschema_test

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func createTestServerParams() base.TestServerArgs {
	params, _ := tests.CreateTestServerParams()
	params.Settings = cluster.MakeTestingClusterSettings()
	return params
}

func TestValidateSystemSchemaAfterBootStrap(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	datadriven.Walk(t, testutils.TestDataPath(t, "bootstrap"), func(t *testing.T, path string) {
		// initialize per-test state
		// New database for each test file.
		s, db, _ := serverutils.StartServer(t, createTestServerParams())
		defer s.Stopper().Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "bootstrap":
				// Create a connection to the database cluster.
				sqlRunner := sqlutils.MakeSQLRunner(db)

				// Prepare the SQL query.
				sql := `USE SYSTEM; SHOW CREATE ALL TABLES;`

				// Execute the SQL query.
				rows := sqlRunner.QueryStr(t, sql)

				// Extract return and return.
				var sb strings.Builder
				for _, row := range rows {
					if len(row) != 1 {
						d.Fatalf(t, "`SHOW CREATE ALL TABLES` returns has zero column.")
					}
					sb.WriteString(row[0])
					sb.WriteString("\n")
				}
				return sb.String()
			}

			d.Fatalf(t, "unsupported command: %s", d.Cmd)
			return ""
		})
	})
}
