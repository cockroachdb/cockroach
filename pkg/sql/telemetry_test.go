// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTelemetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Enable enterprise features to test READ COMMITTED telemetry.
	defer ccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "takes >1min under race")
	skip.UnderDeadlock(t, "takes >1min under deadlock")

	sqltestutils.TelemetryTest(
		t,
		[]base.TestServerArgs{{}},
		true, /* testTenant */
	)
}

func TestTelemetryRecordCockroachShell(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cluster := serverutils.StartCluster(
		t,
		1,
		base.TestClusterArgs{},
	)
	defer cluster.Stopper().Stop(context.Background())

	pgUrl, cleanupFn := pgurlutils.PGUrl(
		t,
		cluster.Server(0).AdvSQLAddr(),
		"TestTelemetryRecordCockroachShell",
		url.User("root"),
	)
	defer cleanupFn()
	q := pgUrl.Query()

	q.Add("application_name", catconstants.ReportableAppNamePrefix+catconstants.InternalSQLAppName)
	pgUrl.RawQuery = q.Encode()

	db, err := gosql.Open("postgres", pgUrl.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var appName string
	err = db.QueryRow("SHOW application_name").Scan(&appName)
	require.NoError(t, err)
	require.Equal(t, catconstants.ReportableAppNamePrefix+catconstants.InternalSQLAppName, appName)

	var counter int
	err = db.QueryRow(
		"SELECT usage_count FROM crdb_internal.feature_usage WHERE feature_name = 'sql.connection.cockroach_cli'",
	).Scan(&counter)
	require.NoError(t, err)
	require.Equal(t, 1, counter)

}
