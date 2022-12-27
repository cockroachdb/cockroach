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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schematelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	hlcRE, err := regexp.Compile(`"wallTime":"\d*"(,"logical":\d*)?`)
	require.NoError(t, err)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "bootstrap"), func(t *testing.T, path string) {
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
				snapshotID := uuid.FastMakeV4()
				maxRecords := 100000
				// By default, collect the entirety of the system schema.
				// In that case, the snapshot ID won't matter.
				// When `max_records` is specified in the command, the record set
				// will be truncated accordingly. This is done in a pseudo-random
				// fashion and the snapshot ID is used as a seed value.
				for _, arg := range d.CmdArgs {
					switch arg.Key {
					case "snapshot_id":
						var snapshotIDString string
						arg.Scan(t, 0, &snapshotIDString)
						snapshotID, err = uuid.FromString(snapshotIDString)
						require.NoErrorf(t, err, "invalid UUID for snapshot_id: %q", snapshotIDString)
					case "max_records":
						arg.Scan(t, 0, &maxRecords)
					}
				}
				// Collect a projection of the bootstrapped cluster's schema.
				events, err := schematelemetry.CollectClusterSchemaForTelemetry(ctx, &execCfg, execCfg.Clock.Now(), snapshotID, maxRecords)
				require.NoError(t, err, "expected schema snapshotting to succeed")
				require.NotEmpty(t, events)

				// Return the results, one descriptor per line.
				var sb strings.Builder
				je := jsonpb.Marshaler{}
				meta, ok := events[0].(*eventpb.SchemaSnapshotMetadata)
				require.Truef(t, ok, "expected a SchemaSnapshotMetadata event, instead got %T", events[0])
				require.EqualValues(t, len(events), 1+meta.NumRecords, "unexpected record count")
				for _, event := range events[1:] {
					ev, ok := event.(*eventpb.SchemaDescriptor)
					require.Truef(t, ok, "expected a SchemaDescriptor event, instead got %T", event)
					require.EqualValues(t, meta.SnapshotID, ev.SnapshotID, "unexpected snapshot ID")
					if ev.DescID == keys.PublicSchemaID && ev.Desc == nil {
						// The public schema of the system database has no descriptor.
						continue
					}
					require.NotNilf(t, ev.Desc, "unexpectedly missing descriptor in %s", ev)
					str, err := je.MarshalToString(ev.Desc)
					require.NoError(t, err, "unexpected descriptor marshal error")
					str = hlcRE.ReplaceAllString(str, `"wallTime":"0"`)
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
