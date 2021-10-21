// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigkvaccessor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	sctestutils "github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestDatadriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				EnableSpanConfigs: true,
			},
		})
		defer tc.Stopper().Stop(ctx)

		const dummySpanConfigurationsFQN = "defaultdb.public.dummy_span_configurations"
		tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, `SET CLUSTER SETTING spanconfig.experimental_kvaccessor.enabled = true`)
		tdb.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummySpanConfigurationsFQN))
		accessor := spanconfigkvaccessor.New(
			tc.Server(0).DB(),
			tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor),
			tc.Server(0).ClusterSettings(),
			dummySpanConfigurationsFQN,
		)

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "kvaccessor-get":
				var spans []roachpb.Span
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}

					const spanPrefix = "span "
					if !strings.HasPrefix(line, spanPrefix) {
						t.Fatalf("malformed line %q, expected to find spanPrefix %q", line, spanPrefix)
					}
					line = strings.TrimPrefix(line, spanPrefix)
					spans = append(spans, sctestutils.ParseSpan(t, line))
				}

				entries, err := accessor.GetSpanConfigEntriesFor(ctx, spans)
				if err != nil {
					return fmt.Sprintf("err: %s", err.Error())
				}

				var output strings.Builder
				for _, entry := range entries {
					output.WriteString(fmt.Sprintf("%s\n", sctestutils.PrintSpanConfigEntry(entry)))
				}
				return output.String()
			case "kvaccessor-update":
				var toDelete []roachpb.Span
				var toUpsert []roachpb.SpanConfigEntry
				for _, line := range strings.Split(d.Input, "\n") {
					line = strings.TrimSpace(line)
					if line == "" {
						continue
					}

					const upsertPrefix, deletePrefix = "upsert ", "delete "
					if !strings.HasPrefix(line, upsertPrefix) && !strings.HasPrefix(line, deletePrefix) {
						t.Fatalf("malformed line %q, expected to find prefix %q or %q",
							line, upsertPrefix, deletePrefix)
					}

					if strings.HasPrefix(line, deletePrefix) {
						line = strings.TrimPrefix(line, line[:len(deletePrefix)])
						toDelete = append(toDelete, sctestutils.ParseSpan(t, line))
					} else {
						line = strings.TrimPrefix(line, line[:len(upsertPrefix)])
						toUpsert = append(toUpsert, sctestutils.ParseSpanConfigEntry(t, line))
					}
				}
				if err := accessor.UpdateSpanConfigEntries(ctx, toDelete, toUpsert); err != nil {
					return fmt.Sprintf("err: %s", err.Error())
				}
				return "ok"
			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}
			return ""
		})
	})
}
