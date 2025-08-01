// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/assert"
)

// TestDataDriven is a data-driven test framework for the schemafeed.
// It provides a mechanism to classify and understand how the schemafeed
// will interpret different schema change operations.
//
//   - "exec"
//     Executes the input SQL query.
//
//   - "create" f=<int>
//     Creates a schemafeed with the targets specified as the input with the
//     provided ID.
//
//   - "pop" f=<int>
//     Pop all events from the schemafeed with the given ID.
//     The structure of the events looks like as follows:
//
//     t 1->2: Unknown
//     t 2->3: Unknown
//     t 3->4: Unknown
//     t 4->5: Unknown
//     t 5->6: Unknown
//     t 6->7: PrimaryKeyChange
//     t 7->8: Unknown
//     t 8->9: Unknown
//
//     The first column is the name of the table in question.
//     The second is the version transition. The third indicates
//     the event classification.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer log.Scope(t).Close(t)

	testData := datapathutils.TestDataPath(t, "")
	datadriven.Walk(t, testData, func(t *testing.T, path string) {
		ctx := context.Background()
		srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		ts := srv.ApplicationLayer()

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		ctx, cancel := ts.AppStopper().WithCancelOnQuiesce(ctx)
		defer cancel()
		schemaFeeds := map[int]schemafeed.SchemaFeed{}
		parseTargets := func(t *testing.T, in string) (targets changefeedbase.Targets) {
			tables := strings.Fields(in)
			for _, tab := range tables {
				var tableID descpb.ID
				var statementTimeName changefeedbase.StatementTimeName
				tdb.QueryRow(t, "SELECT $1::regclass::int, $1::regclass::string", tab).Scan(
					&tableID, &statementTimeName)
				targets.Add(changefeedbase.Target{
					Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
					TableID:           tableID,
					FamilyName:        "primary",
					StatementTimeName: statementTimeName,
				})
			}
			return targets
		}
		errCh := make(chan error, 1)
		defer func() {
			select {
			case err := <-errCh:
				t.Fatalf("unexpected error: %v", err)
			default:
			}
		}()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			select {
			case err := <-errCh:
				d.Fatalf(t, "unexpected error: %v", err)
			default:
			}
			t.Log(d.Cmd)
			switch d.Cmd {
			case "exec":
				if _, err := sqlDB.Exec(d.Input); err != nil {
					d.Fatalf(t, "failed to exec: %v", err)
				}
				return ""
			case "create":
				var i int
				d.ScanArgs(t, "f", &i)
				if _, exists := schemaFeeds[i]; exists {
					d.Fatalf(t, "feed %d already created", i)
				}
				cfg := &ts.SQLServer().(*sql.Server).GetExecutorConfig().DistSQLSrv.ServerConfig
				now := ts.Clock().Now()
				targets := parseTargets(t, d.Input)
				f := schemafeed.New(ctx, cfg, schemafeed.TestingAllEventFilter, targets, now, nil, changefeedbase.CanHandle{
					MultipleColumnFamilies: true,
					VirtualColumns:         true,
				})
				schemaFeeds[i] = f

				go func() {
					if err := f.Run(ctx); !assert.NoError(t, err) {
						select {
						case errCh <- err:
						default:
						}
					}
				}()
				return ""
			case "pop":
				var i int
				d.ScanArgs(t, "f", &i)
				sf, exists := schemaFeeds[i]
				if !exists {
					d.Fatalf(t, "feed %d does not exist", i)
				}
				events, err := sf.Pop(ctx, ts.Clock().Now())
				if err != nil {
					return err.Error()
				}
				var buf strings.Builder
				for i, ev := range events {
					if i > 0 {
						buf.WriteString("\n")
					}
					_, _ = fmt.Fprintf(&buf, "%s %d->%d: %s",
						ev.After.GetName(), ev.Before.GetVersion(), ev.After.GetVersion(),
						schemafeed.ClassifyEvent(ev),
					)
					if _, noColumnChanges := schemafeed.IsPrimaryIndexChange(ev, changefeedbase.Targets{}); noColumnChanges {
						buf.WriteString(" (no column changes)")
					}
				}
				return buf.String()
			default:
				d.Fatalf(t, "unexpected command %s", d.Cmd)
				return ""
			}
		})
	})
}
