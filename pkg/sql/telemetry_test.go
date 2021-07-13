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
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Dummy import to pull in kvtenantccl. This allows us to start tenants.
// TODO(yuzefovich): break up the dependency on CCL code.
var _ = kvtenantccl.Connector{}

func TestTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "takes >1min under race")

	sqltestutils.TelemetryTest(
		t,
		[]base.TestServerArgs{{}},
		true, /* testTenant */
	)
}

func TestTelemetryRecordCockroachShell(t *testing.T) {

	diagSrv := diagutils.NewServer()

	tempExternalIODir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()

	diagSrvURL := diagSrv.URL()
	mapServerArgs := make(map[int]base.TestServerArgs)
	serverArgs := []base.TestServerArgs{{}}
	for i, v := range serverArgs {
		v.Knobs.Server = &server.TestingKnobs{
			DiagnosticsTestingKnobs: diagnostics.TestingKnobs{
				OverrideReportingURL: &diagSrvURL,
			},
		}
		v.ExternalIODir = tempExternalIODir
		mapServerArgs[i] = v
	}
	cluster := serverutils.StartNewTestCluster(
		t,
		len(serverArgs),
		base.TestClusterArgs{ServerArgsPerNode: mapServerArgs},
	)

	pgUrl, cleanupFn := sqlutils.PGUrl(
		t,
		cluster.Server(0).ServingSQLAddr(),
		"TestTelemetryRecordCockroachShell",
		url.User("root"),
	)
	defer cleanupFn()
	q := pgUrl.Query()

	q.Add("application_name", "$ cockroach internal test")
	pgUrl.RawQuery = q.Encode()
	fmt.Println(pgUrl)

	db, err := gosql.Open("postgres", pgUrl.String())
	if err != nil {
		//return nil, err
		t.Fatal(err)
	}
	defer db.Close()

	var app string
	db.QueryRow("SHOW application_name").Scan(&app)

	reporter := cluster.Server(0).DiagnosticsReporter().(*diagnostics.Reporter)
	reporter.ReportDiagnostics(context.Background())

	last := diagSrv.LastRequestData()
	fmt.Printf("%v", last)
	//require.Equal(t, appName, last.)

}
