// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gc_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// smallEngineBlocks configures Pebble with a block size of 1 byte, to provoke
// bugs in time-bound iterators. We disable this under race, due to the slowdown.
var smallEngineBlocks = !util.RaceEnabled &&
	util.ConstantWithMetamorphicTestBool("small-engine-blocks", false)

func init() {
	randutil.SeedForTests()
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
}

func TestEndToEndGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, d := range []struct {
		// Using range tombstones to remove data will promote full range deletions
		// as a fast path GC operation.
		rangeTombstones bool
		// Clear range enables usage of clear range operation to remove multiple
		// point keys by GC.
		clearRange bool
	}{
		{
			// With range tombstones, fast path will always take precedence so we
			// don't care about clearRange being enabled or not.
			rangeTombstones: true,
		},
		{
			rangeTombstones: false,
			clearRange:      true,
		},
		{
			rangeTombstones: false,
			clearRange:      false,
		},
	} {
		t.Run(fmt.Sprintf("rangeTombstones=%t/clearRange=%t", d.rangeTombstones, d.clearRange),
			func(t *testing.T) {
				ctx := context.Background()

				manualClock := hlc.NewHybridManualClock()
				tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								SmallEngineBlocks: smallEngineBlocks,
							},
							Server: &server.TestingKnobs{
								WallClock: manualClock,
							},
						},
						DisableDefaultTestTenant: true,
					},
				})
				tc.Start(t)
				defer tc.Stopper().Stop(context.Background())
				require.NoError(t, tc.WaitForFullReplication())

				sqlDb := tc.ServerConn(0)
				kvDb := tc.Server(0).DB()
				statusServer := tc.Server(0).StatusServer().(serverpb.StatusServer)

				execOrFatal := func(t *testing.T, db *gosql.DB, stmt string, args ...interface{}) {
					t.Helper()
					_, err := db.Exec(stmt, args...)
					require.NoError(t, err, "failed to execute %s", stmt)
				}

				getTableRangeIDs := func(t *testing.T, db *gosql.DB) ids {
					t.Helper()
					rows, err := db.Query("WITH r AS (SHOW RANGES FROM TABLE kv) SELECT range_id FROM r ORDER BY start_key")
					require.NoError(t, err, "failed to query ranges")
					var rangeIDs []int64
					for rows.Next() {
						var rangeID int64
						require.NoError(t, rows.Scan(&rangeID), "failed to read row with range id")
						rangeIDs = append(rangeIDs, rangeID)
					}
					return rangeIDs
				}

				readSomeKeys := func(t *testing.T, db *gosql.DB) []int64 {
					t.Helper()
					var ids []int64
					rows, err := db.Query("SELECT k FROM kv LIMIT 5")
					require.NoError(t, err, "failed to query kv data")
					for rows.Next() {
						var id int64
						require.NoError(t, rows.Scan(&id), "failed to scan value")
						ids = append(ids, id)
					}
					return ids
				}

				getRangeInfo := func(t *testing.T, rangeID int64, db *gosql.DB) (startKey, endKey []byte) {
					t.Helper()
					row := db.QueryRow("SELECT start_key, end_key FROM crdb_internal.ranges_no_leases WHERE range_id=$1",
						rangeID)
					require.NoError(t, row.Err(), "failed to query range info")
					require.NoError(t, row.Scan(&startKey, &endKey), "failed to scan range info")
					return startKey, endKey
				}

				deleteRangeDataWithRangeTombstone := func(t *testing.T, kvDb *kv.DB, db *gosql.DB) {
					t.Helper()
					var prevRangeIDs ids
					for i := 0; i < 3; i++ {
						rangeIDs := getTableRangeIDs(t, sqlDb)
						if rangeIDs.equal(prevRangeIDs) {
							return
						}
						for _, id := range rangeIDs {
							start, end := getRangeInfo(t, id, db)
							require.NoError(t, kvDb.DelRangeUsingTombstone(ctx, start, end),
								"failed to delete range with tombstone")
						}
						prevRangeIDs = rangeIDs
					}
					t.Fatal("failed to get consistent list of ranges for table after 3 attempts")
				}

				getRangeStats := func(t *testing.T, rangeID int64) enginepb.MVCCStats {
					t.Helper()
					rr := &serverpb.RangesRequest{
						NodeId:   "1",
						RangeIDs: []roachpb.RangeID{roachpb.RangeID(rangeID)},
					}
					infos, err := statusServer.Ranges(ctx, rr)
					require.NoError(t, err, "failed to query range info")
					return *infos.Ranges[0].State.Stats
				}

				findNonEmptyRanges := func(t *testing.T, rangeIDs ids) (nonEmptyRangeIDs ids) {
					t.Helper()
					for _, id := range rangeIDs {
						stats := getRangeStats(t, id)
						t.Logf("range %d stats: %s", id, &stats)
						// Test can't give meaningful results if stats contain estimates.
						// Test also doesn't perform any operations that result in estimated stats
						// being created, so it is a failure in the environment if that happens.
						require.Zerof(t, stats.ContainsEstimates, "we must not have estimates")
						if stats.RangeKeyCount > 0 || stats.KeyCount > 0 {
							nonEmptyRangeIDs = append(nonEmptyRangeIDs, id)
						}
					}
					return nonEmptyRangeIDs
				}

				rng, _ := randutil.NewTestRand()

				// Set closed timestamp duration, this is needed to avoid waiting for default
				// 2 min interval for protected timestamp to get bumped and letting GC collect
				// old values.
				execOrFatal(t, sqlDb, `SET CLUSTER SETTING kv.protectedts.poll_interval = '5s'`)

				if d.clearRange {
					execOrFatal(t, sqlDb, `SET CLUSTER SETTING kv.gc.clear_range_min_keys = 5`)
				} else {
					execOrFatal(t, sqlDb, `SET CLUSTER SETTING kv.gc.clear_range_min_keys = 0`)
				}

				execOrFatal(t, sqlDb, `CREATE TABLE kv (k BIGINT NOT NULL PRIMARY KEY, v BYTES NOT NULL)`)

				for i := 0; i < 1000; i++ {
					execOrFatal(t, sqlDb, "UPSERT INTO kv VALUES ($1, $2)", rng.Int63(), "hello")
				}
				t.Logf("found table range after initializing table data: %s", getTableRangeIDs(t, sqlDb))

				require.NotEmptyf(t, readSomeKeys(t, sqlDb), "found no keys in table")

				// Since ranges query and checking range stats are non atomic there could be
				// a range split/merge operation caught in between. That could produce empty
				// or incomplete results. Moreover, range info produced by ranges doesn't
				// provide start/end keys for the range in binary form, so it is hard to make
				// consistency check. We rely on retrying several times for simplicity.
				const tableRangesRetry = 3
				var tableRangeIDs, nonEmptyRangeIDs ids
				for i := 0; i < tableRangesRetry; i++ {
					tableRangeIDs = getTableRangeIDs(t, sqlDb)
					if len(tableRangeIDs) == 0 {
						continue
					}
					nonEmptyRangeIDs = findNonEmptyRanges(t, tableRangeIDs)
					if len(nonEmptyRangeIDs) > 0 {
						break
					}
				}
				require.NotEmpty(t, tableRangeIDs, "failed to query ranges belonging to table")
				require.NotEmpty(t, nonEmptyRangeIDs, "all table ranges are empty according to MVCCStats")

				t.Logf("found non-empty table ranges before deletion: %v", nonEmptyRangeIDs)

				if d.rangeTombstones {
					deleteRangeDataWithRangeTombstone(t, kvDb, sqlDb)
				} else {
					execOrFatal(t, sqlDb, "DELETE FROM kv WHERE k IS NOT NULL")
				}
				t.Logf("found table ranges after range deletion: %s", getTableRangeIDs(t, sqlDb))

				require.Empty(t, readSomeKeys(t, sqlDb), "table still contains data after range deletion")

				// Push clock forward to make all data eligible for GC. Mind that this is not
				// enough just to push the clock, we need to wait for protected timestamp to
				// be pushed by periodic task.
				manualClock.Increment((time.Hour * 50).Nanoseconds())

				// Keep pushing replicas through the queue and checking that ranges were
				// cleared up. We do both operations in the retry loop because we are dealing
				// with two async processes: 1 - protected timestamp update, 2 - queue
				// processing as we could only enqueue, but not force GC op.
				enqueueSucceeded := false
				testutils.SucceedsSoon(t, func() error {
					tableRangeIDs := getTableRangeIDs(t, sqlDb)
					t.Logf("pushing kv table ranges through mvcc gc queue: %s", tableRangeIDs)

					for _, id := range tableRangeIDs {
						_, err := sqlDb.Exec(`SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, id)
						if err != nil {
							t.Logf("failed to enqueue range to mvcc gc queue: %s", err)
						}
						enqueueSucceeded = enqueueSucceeded || err == nil
					}

					// Enqueue operations could fail if ranges change underneath, test will
					// report different error if we didn't enqueue replicas at least once.
					// This is giving us a better visibility if failure is because of GC queue
					// misbehaving and not actual GC behaviour test is checking.
					if !enqueueSucceeded {
						return errors.New("failed to enqueue replicas to GC queue")
					}

					nonEmptyRangeIDs := findNonEmptyRanges(t, tableRangeIDs)
					if len(nonEmptyRangeIDs) > 0 {
						t.Logf("non empty ranges after GC queue: %s", nonEmptyRangeIDs)
						return errors.New("not all ranges were cleared")
					}
					return nil
				})
			})
	}
}

type ids []int64

func (r ids) String() string {
	s := make([]string, len(r))
	for i, r := range r {
		s[i] = fmt.Sprintf("%d", r)
	}
	return fmt.Sprintf("[%s]", strings.Join(s, ","))
}

func (r ids) equal(o ids) bool {
	if len(r) != len(o) {
		return false
	}
	for i := range r {
		if r[i] != o[i] {
			return false
		}
	}
	return true
}
