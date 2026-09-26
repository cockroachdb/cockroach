// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func TestSplitAtTableBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The span-config split notification can be lost if the range transfers its
	// lease concurrently (#173741), leaving the split to the periodic scanner
	// (default 10m, longer than our deadline). Shorten the scan interval so the
	// backstop fires in time. Revert once #173741 is fixed.
	serverArgs := base.TestServerArgs{
		ScanInterval: time.Second,
	}
	// Skip secondary tenants when running under race to avoid flakes.
	if util.RaceEnabled {
		serverArgs.DefaultTestTenant = base.TestSkipSecondaryTenantsUnderDuress
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: serverArgs,
	})
	defer tc.Stopper().Stop(context.Background())
	s := tc.ApplicationLayer(0)

	runner := sqlutils.MakeSQLRunner(s.SQLConn(t, serverutils.DBName("system")))
	runner.Exec(t, `CREATE DATABASE test`)
	runner.Exec(t, `CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`)

	const tableIDQuery = `
SELECT tables.id FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	var tableID uint32
	runner.QueryRow(t, tableIDQuery, "test", "t").Scan(&tableID)
	tableStartKey := s.Codec().TablePrefix(tableID)

	// Wait for new table to split.
	testutils.SucceedsSoon(t, func() error {
		desc, err := tc.LookupRange(tableStartKey)
		if err != nil {
			t.Fatal(err)
		}
		if !desc.StartKey.Equal(tableStartKey) {
			log.Dev.Infof(context.Background(), "waiting on split results")
			return errors.Errorf("expected range start key %s; got %s", tableStartKey, desc.StartKey)
		}
		return nil
	})
}
