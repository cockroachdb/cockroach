// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

var adminPrefix = "/_admin/v1/"

// TestAdminAPIDataDistributionPartitioning partitions a table and verifies
// that we see all zone configs (#27718).
func TestAdminAPIDataDistributionPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)
	sqlDB := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

	sqlDB.Exec(t, `CREATE DATABASE roachblog`)
	sqlDB.Exec(t, `USE roachblog`)
	sqlDB.Exec(t, `CREATE TABLE posts (id INT PRIMARY KEY, title text, body text)`)
	sqlDB.Exec(t, `CREATE TABLE comments (
		id INT,
		post_id INT REFERENCES posts,
		user_region STRING,
		body text,
		PRIMARY KEY (user_region, id)
	) PARTITION BY LIST (user_region) (
		PARTITION us VALUES IN ('US'),
		PARTITION eu VALUES IN ('EU'),
		PARTITION DEFAULT VALUES IN (default)
	)`)

	// Create a zone config for each partition.
	// Would use locality constraints except this test cluster hasn't been started up with localities.
	sqlDB.Exec(t, `ALTER PARTITION us OF TABLE comments CONFIGURE ZONE USING gc.ttlseconds = 9001`)
	sqlDB.Exec(t, `ALTER PARTITION eu OF TABLE comments CONFIGURE ZONE USING gc.ttlseconds = 9002`)

	// Assert that we get all roachblog zone configs back.
	expectedZoneConfigNames := map[string]struct{}{
		"PARTITION eu OF INDEX roachblog.public.comments@primary": {},
		"PARTITION us OF INDEX roachblog.public.comments@primary": {},
	}

	var resp serverpb.DataDistributionResponse
	if err := serverutils.GetJSONProto(firstServer, adminPrefix+"data_distribution", &resp); err != nil {
		t.Fatal(err)
	}

	actualZoneConfigNames := map[string]struct{}{}
	for name := range resp.ZoneConfigs {
		if strings.Contains(name, "roachblog") {
			actualZoneConfigNames[name] = struct{}{}
		}
	}
	if !reflect.DeepEqual(actualZoneConfigNames, expectedZoneConfigNames) {
		t.Fatalf("expected zone config names %v; got %v", expectedZoneConfigNames, actualZoneConfigNames)
	}
}

// TestAdminAPIChartCatalog verifies that an error doesn't happen.
func TestAdminAPIChartCatalog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{})
	defer testCluster.Stopper().Stop(context.Background())

	firstServer := testCluster.Server(0)

	var resp serverpb.ChartCatalogResponse
	err := serverutils.GetJSONProto(firstServer, adminPrefix+"chartcatalog", &resp)
	require.NoError(t, err)
}
