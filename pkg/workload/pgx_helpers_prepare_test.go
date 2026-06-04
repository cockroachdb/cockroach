// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/stretchr/testify/require"
)

// TestMultiConnPoolPrepareRetry verifies that the pool rides out a transient
// statement-preparation failure rather than giving up. This mirrors the
// production hazard where a workload begins preparing an AS OF SYSTEM TIME
// statement against a table created within the follower-read window: historical
// name resolution returns "does not exist" for a few seconds until the AOST
// timestamp advances past the table's creation. pgxpool's Acquire only retries
// the PrepareConn hook a bounded number of times before returning a fatal
// "too many failed attempts acquiring connection" error, so the pool itself
// must absorb the transient failure.
func TestMultiConnPoolPrepareRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	app := srv.ApplicationLayer()

	pgURL, cleanup := app.PGUrl(t)
	defer cleanup()

	cfg := workload.MultiConnPoolCfg{
		MaxTotalConnections: 4,
		Method:              "prepare",
		// The pool's DNS refresh goroutine only exits on context cancellation;
		// disable it so it doesn't outlive the test.
		DNSRefreshInterval: -1,
	}
	mp, err := workload.NewMultiConnPool(ctx, cfg, pgURL.String())
	require.NoError(t, err)
	defer mp.Close()

	// Register a statement that references a table that does not exist yet, so
	// preparing it on a connection fails.
	mp.AddPreparedStatement("transient", "SELECT k FROM deferred_table")

	// Make the table resolvable after a short delay, simulating the transient
	// window passing.
	const createDelay = time.Second
	createErr := make(chan error, 1)
	go func() {
		time.Sleep(createDelay)
		_, err := app.SQLConn(t).ExecContext(
			ctx, "CREATE TABLE deferred_table (k INT PRIMARY KEY)")
		createErr <- err
	}()

	// Acquire must succeed: the pool retries the failing prepare until the table
	// exists. Without the retry, Acquire fails almost immediately with
	// "too many failed attempts acquiring connection".
	conn, err := mp.Get().Acquire(ctx)
	require.NoError(t, err)
	conn.Release()
	require.NoError(t, <-createErr)
}
