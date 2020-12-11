// Copyright 2020 The Cockroach Authors.
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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVirtualTableGenCancel is a regression test for a bug whereby cancellation
// from a virtual table generator led to a race on internal planner state.
//
// This test reproduced that race.
func TestVirtualTableGenCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	const workers = 10
	const iterations = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		conn, err := tc.ServerConn(0).Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "SET statement_timeout='100us'")
		require.NoError(t, err)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, err := conn.ExecContext(ctx, "SELECT * FROM crdb_internal.table_columns")
				// We expect to always see an error but it may be possible to not catch
				// the timeout and not see the error and that's not what we're testing
				// anyway so allow it.
				if err != nil {
					assert.Regexp(t, "query execution canceled due to statement timeout", err)
				}
			}
		}()
	}
	wg.Wait()
}
