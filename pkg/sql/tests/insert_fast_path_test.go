// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInsertFastPath verifies that the 1PC "insert fast path" optimization
// is applied.
func TestInsertFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var db *gosql.DB
	wg := sync.WaitGroup{}
	wg.Add(1)

	params, _ := CreateTestServerParams()
	params.Settings = cluster.MakeTestingClusterSettings()
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		AfterExecute: func(ctx context.Context, stmt string, err error) {
			if strings.HasPrefix(stmt, "INSERT INTO fast_path_test") {
				var c int
				// If the fast path did not get triggered, then the following query
				// would block indefinitely, since it would need to wait for the
				// original INSERT to be committed.
				err := db.QueryRow("SELECT count(*) FROM fast_path_test").Scan(&c)
				assert.NoError(t, err)
				assert.Equal(t, 1, c, "expected 1 row, got %d", c)
				wg.Done()
			}
		},
	}

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)
	db = tc.ServerConn(0)
	_, err := db.Exec(`CREATE TABLE fast_path_test(val int);`)
	require.NoError(t, err)

	// Use placeholders to force usage of extended protocol.
	_, err = db.Exec("INSERT INTO fast_path_test VALUES($1)", 1)
	require.NoError(t, err)
	wg.Wait()
}
