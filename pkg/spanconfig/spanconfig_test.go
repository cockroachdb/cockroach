// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestScalability(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderStress(t)
	skip.UnderRace(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		// This test creates a lot of tables (i.e. hard split points); disable the
		// split queue.
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					SkipCreateTableEventLogging: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	ts := tc.Server(0)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	tdb.Exec(t, `SET CLUSTER SETTING server.eventlog.enabled = 'false'`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	tdb.Exec(t, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false`)
	tdb.Exec(t, `SET CLUSTER SETTING sql.stats.flush.enabled = false`)
	tdb.Exec(t, `SET CLUSTER SETTING sql.metrics.statement_details.enabled = false`)
	tdb.Exec(t, `SET CLUSTER SETTING kv.transaction.max_intents_bytes = $1`, 12<<20 /* 12 MB */)

	tdb.Exec(t, `CREATE DATABASE db`)

	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	require.NoError(t, store.WaitForSpanConfigSubscription(ctx))

	scKVAccessor := ts.SpanConfigKVAccessor().(spanconfig.KVAccessor)
	scKVSubscriber := ts.SpanConfigKVSubscriber().(spanconfig.KVSubscriber).(*spanconfigkvsubscriber.KVSubscriber)

	var numInitialRecords int
	{
		records, err := scKVAccessor.GetSpanConfigRecords(
			ctx,
			spanconfig.TestingEntireSpanConfigurationStateTargets(),
		)
		require.NoError(t, err)
		require.NotZero(t, len(records), "empty global span configuration state")
		numInitialRecords = len(records)
	}

	const numEpochs = 10
	const numBatchesPerEpoch = 10
	const numTablesPerBatch = 10000
	tableCreationStart := timeutil.Now()
	for epochIdx := 0; epochIdx < numEpochs; epochIdx++ {
		epochStart := timeutil.Now()
		require.NoError(t, ctxgroup.GroupWorkers(ctx, numBatchesPerEpoch, func(ctx context.Context, batchIdx int) error {
			return crdb.ExecuteTx(ctx, tdb.DB.(*gosql.DB), nil /* txopts */, func(tx *gosql.Tx) error {
				var createTablesBuffer strings.Builder
				for tableIdx := 0; tableIdx < numTablesPerBatch; tableIdx++ {
					createTablesBuffer.WriteString(fmt.Sprintf(`CREATE TABLE db.t_%d_%d_%d();`, epochIdx, batchIdx, tableIdx))
				}
				_, err := tx.Exec(createTablesBuffer.String())
				return err
			})
		}))

		log.Infof(ctx, "finished epoch=%d (took %s); running total: %d tables in %s",
			epochIdx,
			timeutil.Since(epochStart),
			numTablesPerBatch*numBatchesPerEpoch*(epochIdx+1),
			timeutil.Since(tableCreationStart),
		)
	}

	// XXX: Scalability test -- disable/block reconciler; create all the tables;
	// unblock and expect records, and expect subscriber to update in-mem state
	// with everything. Then incrementally update all configs.
	// Change default zone config and see how long that takes.
	// - Initial batch size: 250k.
	// - Incremental batch size: 50k.

	reconciliationStart := timeutil.Now()
	expectedNumRecords := numInitialRecords + (numBatchesPerEpoch * numTablesPerBatch * numEpochs)
	testutils.SucceedsWithin(t, func() error {
		records, err := scKVAccessor.GetSpanConfigRecords(
			ctx,
			spanconfig.TestingEntireSpanConfigurationStateTargets(),
		)
		if err != nil {
			return err
		}
		if len(records) != expectedNumRecords {
			return fmt.Errorf("expected kvaccessor to hold %d records, found %d", expectedNumRecords, len(records))
		}
		return nil
	}, 10*time.Minute)

	testutils.SucceedsWithin(t, func() error {
		if got := scKVSubscriber.TestingNumberOfSpanConfigs(); got != expectedNumRecords {
			return fmt.Errorf("expected kvsubscriber to hold %d span configs, found %d", expectedNumRecords, got)
		}
		return nil
	}, 10*time.Minute)

	log.Infof(ctx, "end-to-end span config reconciliation (post-creation) for %d tables took %s",
		expectedNumRecords, timeutil.Since(reconciliationStart))
}
