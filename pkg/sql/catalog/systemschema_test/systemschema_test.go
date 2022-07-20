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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
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
		execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "show_create":
				// Create a connection to the database cluster.
				sqlRunner := sqlutils.MakeSQLRunner(db)

				// Execute the SQL query.
				rows := sqlRunner.QueryStr(t, d.Input)

				// Extract results and return.
				var sb strings.Builder
				for _, row := range rows {
					if len(row) != 1 {
						d.Fatalf(t, "expect 1 column in %q result set, instead found %d", d.Input, len(row))
					}
					sb.WriteString(row[0])
					sb.WriteString("\n")
				}
				return sb.String()

			case "schema_telemetry":
				// Collect a projection of the bootstrapped cluster's schema.
				ess, err := schematelemetry.CollectClusterSchemaForTelemetry(ctx, &execCfg, execCfg.Clock.Now())
				require.NoError(t, err)

				// Return the results, one element per line.
				var sb strings.Builder
				jsonEncoder := jsonpb.Marshaler{}
				for _, es := range ess {
					str, err := jsonEncoder.MarshalToString(&es)
					require.NoError(t, err)
					sb.WriteString(str)
					sb.WriteRune('\n')
				}
				return sb.String()
			}
			d.Fatalf(t, "unsupported command: %s", d.Cmd)
			return ""
		})
	})
}
