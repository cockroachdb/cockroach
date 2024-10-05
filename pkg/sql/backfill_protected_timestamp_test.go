// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestValidationWithProtectedTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t, "test takes too long")
	skip.UnderRace(t, "test takes too long")

	ctx := context.Background()
	indexValidationQueryWait := make(chan struct{})
	indexValidationQueryResume := make(chan struct{})
	validationQuerySeen := sync.Once{}

	indexScanQuery := regexp.MustCompile(`SELECT count\(1\) FROM \[\d+ AS t\]@\[2\]`)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &eval.TestingKnobs{
				ForceProductionValues: true,
			},
			Store: &kvserver.StoreTestingKnobs{
				DisableSplitQueue: true,
			},
			SQLExecutor: &sql.ExecutorTestingKnobs{
				BeforeExecute: func(ctx context.Context, sql string, descriptors *descs.Collection) {
					if indexScanQuery.MatchString(sql) {
						validationQuerySeen.Do(func() {
							indexValidationQueryWait <- struct{}{}
							<-indexValidationQueryResume
						})

					}
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	tenantSettings := ts.ClusterSettings()
	protectedts.PollInterval.Override(ctx, &tenantSettings.SV, time.Millisecond)
	r := sqlutils.MakeSQLRunner(db)

	systemSqlDb := s.SystemLayer().SQLConn(t, serverutils.DBName("system"))
	rSys := sqlutils.MakeSQLRunner(systemSqlDb)

	// Refreshes the in-memory protected timestamp state to asOf.
	refreshTo := func(t *testing.T, tableKey roachpb.Key, asOf hlc.Timestamp) {
		store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
		require.NoError(t, err)
		var repl *kvserver.Replica
		testutils.SucceedsSoon(t, func() error {
			repl = store.LookupReplica(roachpb.RKey(tableKey))
			if repl == nil {
				return errors.New(`could not find replica`)
			}
			return nil
		})
		ptsReader := store.GetStoreConfig().ProtectedTimestampReader
		require.NoError(
			t,
			spanconfigptsreader.TestingRefreshPTSState(ctx, t, ptsReader, asOf),
		)
		require.NoError(t, repl.ReadProtectedTimestampsForTesting(ctx))
	}
	// Refresh forces the PTS cache to update to at least asOf.
	refreshPTSCacheTo := func(t *testing.T, asOf hlc.Timestamp) {
		ptp := ts.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		require.NoError(t, ptp.Refresh(ctx, asOf))
	}

	for _, sql := range []string{
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10ms'",
		"SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval ='10ms'",
		"SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval ='10ms'",
	} {
		rSys.Exec(t, sql)
	}
	for _, sql := range []string{
		"SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false",
		"ALTER DATABASE defaultdb CONFIGURE ZONE USING gc.ttlseconds = 1",
		"CREATE TABLE t(n int)",
		"ALTER TABLE t CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 67108864, gc.ttlseconds = 1",
		"INSERT INTO t(n) SELECT * FROM generate_series(1, 250000)",
	} {
		r.Exec(t, sql)
	}

	getTableID := func() (tableID uint32) {
		r.QueryRow(t, `SELECT table_id FROM crdb_internal.tables`+
			` WHERE name = 't' AND database_name = current_database()`).Scan(&tableID)
		return tableID
	}
	tableID := getTableID()
	tableKey := ts.Codec().TablePrefix(tableID)

	grp := ctxgroup.WithContext(ctx)
	grp.Go(func() error {
		<-indexValidationQueryWait
		getTableRangeIDs := func(t *testing.T) ([]int64, error) {
			t.Helper()
			rows, err := db.QueryContext(ctx, "WITH r AS (SHOW RANGES FROM TABLE t) SELECT range_id FROM r ORDER BY start_key")
			if err != nil {
				return nil, errors.Wrap(err, "failed to query ranges")
			}
			var rangeIDs []int64
			for rows.Next() {
				var rangeID int64
				if err := rows.Scan(&rangeID); err != nil {
					return nil, errors.Wrapf(err, "failed to read row with range id")
				}
				rangeIDs = append(rangeIDs, rangeID)
			}
			if err := rows.Close(); err != nil {
				return nil, err
			}
			return rangeIDs, nil
		}
		ranges, err := getTableRangeIDs(t)
		if err != nil {
			return err
		}
		const retryTxnErrorSubstring = "restart transaction"
		for {
			if _, err := db.ExecContext(ctx, "BEGIN"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, "SET sql_safe_updates=off"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, "DELETE FROM t;"); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES('9999999')"); err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, "COMMIT")
			if err != nil {
				if strings.Contains(err.Error(), retryTxnErrorSubstring) {
					err = nil
					continue
				}
				return err
			}
			break
		}
		refreshTo(t, tableKey, ts.Clock().Now())
		refreshPTSCacheTo(t, ts.Clock().Now())
		for _, id := range ranges {
			if _, err := systemSqlDb.ExecContext(ctx, `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true)`, id); err != nil {
				return err
			}
		}
		indexValidationQueryResume <- struct{}{}
		return nil
	})
	grp.Go(func() error {
		_, err := db.ExecContext(ctx, `CREATE INDEX foo ON t (n)`)
		return err
	})

	require.NoError(t, grp.Wait())
	// Validate the rows were removed due to the drop
	res := r.QueryStr(t, `SELECT n FROM t@foo`)
	if len(res) != 1 {
		t.Errorf("expected %d entries, got %d", 1, len(res))
	}
	require.NoError(t, db.Close())
}
