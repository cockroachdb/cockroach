// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colflow_test

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestVectorizedFlowDeadlocksWhenSpilling is a regression test for the
// vectorized flow being deadlocked when multiple operators have to spill to
// disk exhausting the file descriptor limit.
func TestVectorizedFlowDeadlocksWhenSpilling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderDuress(t, "the query might take longer than timeout under duress making the test flaky")

	vecFDsLimit := 8
	envutil.TestSetEnv(t, "COCKROACH_VEC_MAX_OPEN_FDS", strconv.Itoa(vecFDsLimit))
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{
			// Set the testing knob so that the first operator to spill would
			// use up the whole FD limit.
			VecFDsToAcquire: vecFDsLimit,
		}},
	}
	ctx := context.Background()
	s, conn, _ := serverutils.StartServer(t, serverArgs)
	defer s.Stopper().Stop(ctx)

	_, err := conn.ExecContext(ctx, "CREATE TABLE t (a, b) AS SELECT i, i FROM generate_series(1, 10000) AS g(i)")
	require.NoError(t, err)
	// Lower the workmem budget so that all buffering operators have to spill to
	// disk.
	_, err = conn.ExecContext(ctx, "SET distsql_workmem = '1KiB'")
	require.NoError(t, err)
	// Allow just one retry to speed up the test.
	_, err = conn.ExecContext(ctx, "SET CLUSTER SETTING sql.distsql.acquire_vec_fds.max_retries = 1")
	require.NoError(t, err)

	queryCtx, queryCtxCancel := context.WithDeadline(ctx, timeutil.Now().Add(time.Minute))
	defer queryCtxCancel()
	// Run a query with a hash joiner feeding into a hash aggregator, with both
	// operators spilling to disk. We expect that the hash aggregator won't be
	// able to spill though since the FD limit has been used up, and we'd like
	// to see the query timing out (when acquiring the file descriptor quota)
	// rather than being canceled due to the context deadline.
	query := "SELECT max(a) FROM (SELECT t1.a, t1.b FROM t AS t1 INNER HASH JOIN t AS t2 ON t1.a = t2.b) GROUP BY b"
	_, err = conn.ExecContext(queryCtx, query)
	// We expect an error that is different from the query cancellation (which
	// is what SQL layer returns on a context cancellation).
	require.NotNil(t, err)
	require.False(t, strings.Contains(err.Error(), cancelchecker.QueryCanceledError.Error()), err)
}
