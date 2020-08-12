// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func TestSplitAtTableBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	tc := testcluster.StartTestCluster(t, 3, testClusterArgs)
	defer tc.Stopper().Stop(context.Background())

	runner := sqlutils.MakeSQLRunner(tc.Conns[0])
	runner.Exec(t, `CREATE DATABASE test`)
	runner.Exec(t, `CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`)

	const tableIDQuery = `
SELECT tables.id FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables."parentID"
  WHERE dbs.name = $1 AND tables.name = $2
`
	var tableID uint32
	runner.QueryRow(t, tableIDQuery, "test", "t").Scan(&tableID)
	tableStartKey := keys.SystemSQLCodec.TablePrefix(tableID)

	// Wait for new table to split.
	testutils.SucceedsSoon(t, func() error {
		desc, err := tc.LookupRange(tableStartKey)
		if err != nil {
			t.Fatal(err)
		}
		if !desc.StartKey.Equal(tableStartKey) {
			log.Infof(context.Background(), "waiting on split results")
			return errors.Errorf("expected range start key %s; got %s", tableStartKey, desc.StartKey)
		}
		return nil
	})
}
