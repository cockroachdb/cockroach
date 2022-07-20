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
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/datadriven"
	"github.com/gogo/protobuf/jsonpb"
	gogotypes "github.com/gogo/protobuf/types"
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
	wallTimeRE, err := regexp.Compile(`"wallTime":"\d*"`)
	require.NoError(t, err)

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
				events, err := schematelemetry.CollectClusterSchemaForTelemetry(ctx, &execCfg, execCfg.Clock.Now())
				require.NoError(t, err)

				// Return the results, one descriptor per line.
				var sb strings.Builder
				je := jsonpb.Marshaler{}
				for _, event := range events {
					ev, ok := event.(*eventpb.Schema)
					require.True(t, ok)
					if ev.ID == keys.PublicSchemaID {
						// The public schema of the system database has no descriptor.
						continue
					}
					require.NotNil(t, ev.Desc)
					desc := &descpb.Descriptor{}
					require.NoError(t, gogotypes.UnmarshalAny(ev.Desc, desc))
					str, err := je.MarshalToString(desc)
					require.NoError(t, err)
					str = wallTimeRE.ReplaceAllString(str, `"wallTime":"0"`)
					sb.WriteString(str)
					sb.WriteRune('\n')
				}
				return sb.String()

			default:
				d.Fatalf(t, "unsupported command: %s", d.Cmd)
			}
			return ""
		})
	})
}
