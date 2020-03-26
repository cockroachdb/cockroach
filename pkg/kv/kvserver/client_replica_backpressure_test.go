// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Test that mitigations to backpressure when reducing the range size works.
func TestBackpressureNotAppliedWhenReducingRangeSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rRand, _ := randutil.NewPseudoRand()
	ctx := context.Background()

	const (
		rowSize  = 32 << 10 // 32 KiB
		dataSize = 32 << 20 // 32 MiB
		numRows  = dataSize / rowSize
	)

	// setup will set up a testcluster with a table filled with data. All splits
	// will be blocked until the returned closure is called.
	setup := func(t *testing.T, numServers int) (
		tc *testcluster.TestCluster, tdb *sqlutils.SQLRunner, tablePrefix roachpb.Key, unblockSplit func(),
	) {
		// Add a testing knob to block split transactions which we'll enable before
		// we return from setup.
		var allowSplits atomic.Value
		allowSplits.Store(true)
		unblockCh := make(chan struct{}, 1)
		tc = testcluster.StartTestCluster(t, numServers, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
							if ba.Header.Txn != nil && ba.Header.Txn.Name == "split" && !allowSplits.Load().(bool) {
								select {
								case <-unblockCh:
									return roachpb.NewError(errors.Errorf("splits disabled"))
								case <-ctx.Done():
									<-ctx.Done()
								}

							}
							return nil
						},
					},
				},
			},
		})
		require.NoError(t, tc.WaitForFullReplication())

		// Create the table, split it off, and load it up with data.
		tdb = sqlutils.MakeSQLRunner(tc.ServerConn(0))
		tdb.Exec(t, "CREATE TABLE foo (k INT PRIMARY KEY, v BYTES NOT NULL)")

		var tableID int
		tdb.QueryRow(t, "SELECT table_id FROM crdb_internal.tables WHERE name = 'foo'").Scan(&tableID)
		require.NotEqual(t, 0, tableID)
		tablePrefix = keys.MakeTablePrefix(uint32(tableID))
		tc.SplitRangeOrFatal(t, tablePrefix)
		require.NoError(t, tc.WaitForSplitAndInitialization(tablePrefix))

		for i := 0; i < dataSize/rowSize; i++ {
			tdb.Exec(t, "UPSERT INTO foo VALUES ($1, $2)",
				rRand.Intn(numRows), randutil.RandBytes(rRand, rowSize))
		}

		// Block splits and return.
		allowSplits.Store(false)
		var closeOnce sync.Once
		return tc, tdb, tablePrefix, func() {
			closeOnce.Do(func() { close(unblockCh) })
		}
	}

	waitForZoneConfig := func(t *testing.T, tc *testcluster.TestCluster, tablePrefix roachpb.Key, exp int64) {
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < tc.NumServers(); i++ {
				s := tc.Server(i)
				_, r := getFirstStoreReplica(t, s, tablePrefix)
				_, zone := r.DescAndZone()
				if *zone.RangeMaxBytes != exp {
					return fmt.Errorf("expected %d, got %d", exp, *zone.RangeMaxBytes)
				}
			}
			return nil
		})
	}

	t.Run("no backpressure when much larger", func(t *testing.T) {
		tc, tdb, tablePrefix, unblockSplits := setup(t, 1)
		defer tc.Stopper().Stop(ctx)
		defer unblockSplits()

		tdb.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING "+
			"range_max_bytes = $1, range_min_bytes = $2", dataSize/5, dataSize/10)
		waitForZoneConfig(t, tc, tablePrefix, dataSize/5)

		// Don't observe backpressure.
		tdb.Exec(t, "UPSERT INTO foo VALUES ($1, $2)",
			rRand.Intn(10000000), randutil.RandBytes(rRand, rowSize))
	})

	t.Run("backpressure when near limit", func(t *testing.T) {
		tc, tdb, tablePrefix, unblockSplits := setup(t, 1)
		defer tc.Stopper().Stop(ctx)
		defer unblockSplits()
		tdb.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING "+
			"range_max_bytes = $1, range_min_bytes = $2", dataSize/5, dataSize/10)
		waitForZoneConfig(t, tc, tablePrefix, dataSize/5)

		// Don't observe backpressure.
		tdb.Exec(t, "UPSERT INTO foo VALUES ($1, $2)",
			rRand.Intn(10000000), randutil.RandBytes(rRand, rowSize))

		// Now we'll change the range_max_bytes to be half the range size less a
		// bit. This is the range where we expect to observe backpressure.
		_, repl := getFirstStoreReplica(t, tc.Server(0), roachpb.Key(tablePrefix).Next())
		newMax := repl.GetMVCCStats().Total()/2 - 32<<10
		newMin := newMax / 4
		tdb.Exec(t, "ALTER TABLE foo CONFIGURE ZONE USING "+
			"range_max_bytes = $1, range_min_bytes = $2", newMax, newMin)
		waitForZoneConfig(t, tc, tablePrefix, newMax)

		// Observe backpressure now that the range is just over the limit.
		// Use pgx so that cancellation does something reasonable.
		url, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(), "", url.User("root"))
		defer cleanup()
		conf, err := pgx.ParseConnectionString(url.String())
		require.NoError(t, err)
		c, err := pgx.Connect(conf)
		require.NoError(t, err)
		ctxWithCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		upsertErrCh := make(chan error)
		go func() {
			_, err := c.ExecEx(ctxWithCancel, "UPSERT INTO foo VALUES ($1, $2)",
				nil /* options */, rRand.Intn(numRows), randutil.RandBytes(rRand, rowSize))
			upsertErrCh <- err
		}()

		select {
		case <-time.After(10 * time.Millisecond):
			cancel()
		case err := <-upsertErrCh:
			t.Fatalf("expected no error because the request should hang, got %v", err)
		}
		require.Equal(t, context.Canceled, <-upsertErrCh)
	})

}
