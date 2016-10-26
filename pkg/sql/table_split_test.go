// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func TestSplitAtTableBoundary(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testClusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationAuto,
	}
	tc := testcluster.StartTestCluster(t, 3, testClusterArgs)
	defer tc.Stopper().Stop()
	if err := tc.WaitForFullReplication(); err != nil {
		t.Error(err)
	}

	sqlDB := sqlutils.MakeSQLRunner(t, tc.Conns[0])
	_ = sqlDB.Exec(`CREATE DATABASE test`)
	_ = sqlDB.Exec(`CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`)

	tableIDQuery := `
SELECT tables.id FROM system.namespace tables
  JOIN system.namespace dbs ON dbs.id = tables.parentid
  WHERE dbs.name = $1 AND tables.name = $2
`
	var tableID uint32
	sqlDB.QueryRow(tableIDQuery, "test", "t").Scan(&tableID)
	tableStartKey := keys.MakeTablePrefix(tableID)

	// Wait for new table to split.
	util.SucceedsSoon(t, func() error {
		desc, err := tc.LookupRange(keys.MakeRowSentinelKey(tableStartKey))
		if err != nil {
			t.Fatal(err)
		}
		if !desc.StartKey.Equal(tableStartKey) {
			log.Infof(context.TODO(), "waiting on split results")
			return errors.Errorf("expected range start key %s; got %s", tableStartKey, desc.StartKey)
		}
		return nil
	})
}
