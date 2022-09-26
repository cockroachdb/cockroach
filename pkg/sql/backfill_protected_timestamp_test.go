// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func getFirstStoreReplica(
	t *testing.T, s serverutils.TestServerInterface, key roachpb.Key,
) (*kvserver.Store, *kvserver.Replica) {
	t.Helper()
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	var repl *kvserver.Replica
	testutils.SucceedsSoon(t, func() error {
		repl = store.LookupReplica(roachpb.RKey(key))
		if repl == nil {
			return errors.New(`could not find replica`)
		}
		return nil
	})
	return store, repl
}

func TestValidationWithProtectedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	indexValidationQueryWait := make(chan struct{})
	indexValidationQueryResume := make(chan struct{})

	indexScanQuery := regexp.MustCompile(`SELECT count\(1\) FROM \[\d+ AS t\]@\[2\]`)
	settings := cluster.MakeTestingClusterSettings()
	protectedts.PollInterval.Override(ctx, &settings.SV, time.Millisecond)
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings:                 settings,
				DisableDefaultTestTenant: true,
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeExecute: func(ctx context.Context, sql string) {
							if indexScanQuery.MatchString(sql) {
								indexValidationQueryWait <- struct{}{}
								<-indexValidationQueryResume
								time.Sleep(time.Second * 60)
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.Background())

	refreshTo := func(t *testing.T, tableKey roachpb.Key, asOf hlc.Timestamp) {
		for i := 0; i < tc.NumServers(); i++ {
			ptsReader := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().ProtectedTimestampReader
			_, r := getFirstStoreReplica(t, tc.Server(i), tableKey)
			require.NoError(
				t,
				spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, asOf),
			)
			require.NoError(t, r.ReadProtectedTimestampsForTesting(ctx))
		}
	}
	refreshPTSCacheTo := func(t *testing.T, asOf hlc.Timestamp) {
		for i := 0; i < tc.NumServers(); i++ {
			s := tc.Server(i)
			ptp := s.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
			require.NoError(t, ptp.Refresh(ctx, asOf))
		}
	}
	for _, sql := range []string{
		"ALTER DATABASE defaultdb CONFIGURE ZONE USING gc.ttlseconds = 1",
		"CREATE TABLE t(n int)",
		"INSERT INTO t(n) SELECT * FROM generate_series(1, 50000)",
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1ms'",
		"ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 90000, gc.ttlseconds = 1",
	} {
		_, err := tc.ServerConn(0).Exec(sql)
		require.NoError(t, err)
	}
	db, _ := tc.ServerConn(0).Conn(ctx)
	dbConn2, _ := tc.ServerConn(0).Conn(ctx)
	r := sqlutils.MakeSQLRunner(db)

	getTableID := func() (tableID uint32) {
		r.QueryRow(t, `SELECT table_id FROM crdb_internal.tables`+
			` WHERE name = 't' AND database_name = current_database()`).Scan(&tableID)
		return tableID
	}
	tableID := getTableID()
	tableKey := keys.SystemSQLCodec.TablePrefix(tableID)

	go func() {
		<-indexValidationQueryWait
		getTableRangeIDs := func(t *testing.T) []int64 {
			t.Helper()
			rows, err := dbConn2.QueryContext(ctx, "with r as (show ranges from table t) select range_id from r order by start_key")
			require.NoError(t, err, "failed to query ranges")
			var rangeIDs []int64
			for rows.Next() {
				var rangeID int64
				require.NoError(t, rows.Scan(&rangeID), "failed to read row with range id")
				rangeIDs = append(rangeIDs, rangeID)
			}
			require.NoError(t, rows.Close())
			return rangeIDs
		}
		ranges := getTableRangeIDs(t)
		_, err := dbConn2.ExecContext(ctx, "BEGIN")
		require.NoError(t, err)
		_, err = dbConn2.ExecContext(ctx, "SET sql_safe_updates=off")
		require.NoError(t, err)
		_, err = dbConn2.ExecContext(ctx, "DELETE FROM t;")
		require.NoError(t, err)
		_, err = dbConn2.ExecContext(ctx, "INSERT INTO t VALUES('9999999')")
		require.NoError(t, err)
		_, err = dbConn2.ExecContext(ctx, "COMMIT")
		require.NoError(t, err)
		refreshTo(t, tableKey, tc.Server(0).Clock().Now())
		refreshPTSCacheTo(t, tc.Server(0).Clock().Now())
		for _, id := range ranges {
			_, err := dbConn2.ExecContext(ctx, `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', false)`, id)
			require.NoError(t, err)
		}
		indexValidationQueryResume <- struct{}{}

	}()
	if _, err := db.ExecContext(ctx, `CREATE INDEX foo ON t (n)`); err != nil {
		t.Fatal(err)
	}
	// Validate the number of entries in the index to confirm that are reduced.
	res := r.QueryStr(t, `SELECT n FROM t@foo`)
	if len(res) != 1 {
		t.Errorf("expected %d entries, got %d", 1, len(res))
	}
	// Check res is sorted.
	curr := ""
	for i, str := range res {
		if curr > str[0] {
			t.Errorf("unexpected unsorted %s > %s at %d", curr, str[0], i)
		}
		curr = str[0]
	}
	require.NoError(t, db.Close())
	require.NoError(t, dbConn2.Close())
}
