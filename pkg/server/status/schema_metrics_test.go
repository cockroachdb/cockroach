// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package status_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/metrictestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSchemaMetrics_DatabaseCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()
	server, sql0, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	http0, err := server.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)

	databaseCount := scrapingMetricsFrom(t, http0, server.HTTPAddr()).
		Gauge("sql_schema_database_count").
		WithLabel("internal", "false").
		Single()
	databaseCount.ShouldEventuallyEqual(1) // defaultdb

	_, err = sql0.Exec("CREATE DATABASE foo")
	require.NoError(t, err)
	databaseCount.ShouldEventuallyEqual(2) // defaultdb, foo

	_, err = sql0.Exec("DROP DATABASE foo")
	require.NoError(t, err)
	databaseCount.ShouldEventuallyEqual(1) // defaultdb

	_, err = sql0.Exec("CREATE DATABASE bar")
	require.NoError(t, err)
	databaseCount.ShouldEventuallyEqual(2) // defaultdb, bar

	_, err = sql0.Exec("ALTER DATABASE bar CONVERT TO SCHEMA WITH PARENT defaultdb")
	require.NoError(t, err)
	databaseCount.ShouldEventuallyEqual(1) // defaultdb
}

func TestSchemaMetrics_TableCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()
	server, sql0, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	http0, err := server.GetAdminAuthenticatedHTTPClient()
	require.NoError(t, err)

	tableCount := scrapingMetricsFrom(t, http0, server.HTTPAddr()).
		Gauge("sql_schema_table_count").
		WithLabel("internal", "false").
		Single()
	tableCount.ShouldEventuallyEqual(0)

	_, err = sql0.Exec("CREATE TABLE foo (id INT)")
	require.NoError(t, err)
	tableCount.ShouldEventuallyEqual(1)

	_, err = sql0.Exec("DROP TABLE foo")
	require.NoError(t, err)
	tableCount.ShouldEventuallyEqual(0)
}

func scrapingMetricsFrom(
	t *testing.T, client http.Client, addr string,
) *metrictestutils.MetricsTester {
	return metrictestutils.NewMetricsTester(t, func() string {
		resp, err := client.Get(fmt.Sprintf("https://%s/_status/vars", addr))
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return string(body)
	})
}
