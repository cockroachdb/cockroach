// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestIngestSpanConfigs validates that spanConfig updates written at a given
// source timestamp will then commit together on the destination side. To
// simulate full-fledged span config replication, this test writes to a dummy
// span configuration table, listens to updates on the dummy table, and
// replicates these updates to the actual span configuration table.
func TestIngestSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	toIngestCh := make(chan ingestedRecords)
	streamingTestKnobs := &sql.StreamingTestingKnobs{
		RightAfterSpanConfigFlush: initFlushHook(toIngestCh),
	}

	h, sourceAccessor, sourceTenant, cleanup := replicationtestutils.NewReplicationHelperWithDummySpanConfigTable(ctx, t, streamingTestKnobs)
	defer cleanup()

	destTenantID := roachpb.MustMakeTenantID(sourceTenant.ID.InternalValue + 1)

	i1Source, i2Source, i12Source, _ := makeTestSpans(sourceTenant.ID)
	i1Dest, i2Dest, i12Dest, destTenantSplitPoint := makeTestSpans(destTenantID)
	i12DestTarget := []spanconfig.Target{spanconfig.MakeTargetFromSpan(i12Dest)}

	makeRecord := func(targetSpan roachpb.Span, ttl int) spanconfig.Record {
		return replicationtestutils.MakeSpanConfigRecord(t, targetSpan, ttl)
	}

	ingestor, cleanupIngestor := createDummySpanConfigIngestor(
		ctx,
		t,
		h,
		sourceTenant.ID,
		destTenantID)
	group := ctxgroup.WithContext(ctx)
	defer func() {
		cleanupIngestor()
		require.NoError(t, group.Wait())
	}()
	group.GoCtx(func(ctx context.Context) error {
		return ingestor.ingestSpanConfigs(ctx, sourceTenant.Name)
	})

	for ts, toApply := range []struct {
		updates                    []spanconfig.Record
		deletes                    []spanconfig.Target
		expectedUpdates            []spanconfig.Record
		expectedDeletes            spanconfig.Targets
		expectedPersistedUserSpans []spanconfig.Record
	}{
		{
			// Observe the initial scan.
			//
			// NB: If the client were listening to an actual span config table, it
			// would replicate all existing span configs from the current tenant,
			// replacing the deleted span config records. In this test, the only
			// replicated span config record is the dummy tenant split point created
			// by replicationtestutils.NewReplicationHelperWithDummySpanConfigTable().
			expectedUpdates: []spanconfig.Record{makeRecord(destTenantSplitPoint, 14400)},
			expectedDeletes: []spanconfig.Target{spanconfig.MakeTargetFromSpan(destTenantSplitPoint)},

			// The destTenantSplitPoint was indeed persisted but doesn't appear here
			// since the test only inspects persisted span configs with a
			// start key within the i12Dest key span.
			expectedPersistedUserSpans: []spanconfig.Record{},
		},
		{
			// Create a Record.
			updates:                    []spanconfig.Record{makeRecord(i1Source, 2)},
			expectedUpdates:            []spanconfig.Record{makeRecord(i1Dest, 2)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 2)},
		},
		{
			// Update the Span.
			updates:                    []spanconfig.Record{makeRecord(i1Source, 4)},
			expectedUpdates:            []spanconfig.Record{makeRecord(i1Dest, 4)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 4)},
		},
		{
			// Update two records at the same time
			updates:                    []spanconfig.Record{makeRecord(i1Source, 3), makeRecord(i2Source, 2)},
			expectedUpdates:            []spanconfig.Record{makeRecord(i1Dest, 3), makeRecord(i2Dest, 2)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 3), makeRecord(i2Dest, 2)},
		},
		{
			// Merge these Records
			deletes:                    []spanconfig.Target{spanconfig.MakeTargetFromSpan(i2Source)},
			updates:                    []spanconfig.Record{makeRecord(i12Source, 10)},
			expectedUpdates:            []spanconfig.Record{makeRecord(i12Dest, 10)},
			expectedDeletes:            []spanconfig.Target{spanconfig.MakeTargetFromSpan(i2Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i12Dest, 10)},
		},
		{
			// Split the records
			updates:                    []spanconfig.Record{makeRecord(i1Source, 1), makeRecord(i2Source, 2)},
			expectedUpdates:            []spanconfig.Record{makeRecord(i1Dest, 1), makeRecord(i2Dest, 2)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 1), makeRecord(i2Dest, 2)},
		},
	} {
		toApply := toApply
		require.NoError(t, sourceAccessor.UpdateSpanConfigRecords(ctx, toApply.deletes, toApply.updates,
			hlc.MinTimestamp,
			hlc.MaxTimestamp), "failed on update %d (index starts at 0)", ts)
		toIngest := <-toIngestCh
		require.Equal(t, replicationtestutils.PrettyRecords(toApply.expectedUpdates), replicationtestutils.PrettyRecords(toIngest.toUpdate), "failed on step %d (0 indexed)", ts)
		require.Equal(t, fmt.Sprintf("%s", toApply.expectedDeletes), fmt.Sprintf("%s", toIngest.toDelete), "failed on step %d (0 indexed)", ts)

		actualSpanConfigRecords, err := ingestor.accessor.GetSpanConfigRecords(ctx, i12DestTarget)
		require.NoError(t, err)
		require.Equal(t,
			replicationtestutils.PrettyRecords(toApply.expectedPersistedUserSpans),
			replicationtestutils.PrettyRecords(actualSpanConfigRecords),
			"failed on step %d (0 indexed)", ts)
	}
}

// TestIngestSpanConfigsInitialScan tests that the correct span configs are
// updated after the rangefeed which listens to the span config table completes
// an initial scan. To do so, the test does the following in a loop:
//
// 1. Write some updates to a dummy span config table
//
// 2. Open a new span config client on the dummy table, inducing a range feed
// initial scan
//
// 3. Ingest the initial scan updates on the real span config table and validate
// the expected span configs were updated and deleted.
func TestIngestSpanConfigsInitialScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	toIngestCh := make(chan ingestedRecords)
	streamingTestKnobs := &sql.StreamingTestingKnobs{
		RightAfterSpanConfigFlush: initFlushHook(toIngestCh),
	}

	h, sourceAccessor, sourceTenant, cleanup := replicationtestutils.NewReplicationHelperWithDummySpanConfigTable(ctx, t, streamingTestKnobs)
	defer cleanup()

	destTenantID := roachpb.MustMakeTenantID(sourceTenant.ID.InternalValue + 1)

	i1Source, i2Source, i12Source, _ := makeTestSpans(sourceTenant.ID)
	i1Dest, i2Dest, i12Dest, destTenantSplitPoint := makeTestSpans(destTenantID)
	splitRecord := replicationtestutils.MakeSpanConfigRecord(t, destTenantSplitPoint, 14400)

	type testCase struct {
		updatesDuringPause         [][]spanconfig.Record
		deletesDuringPause         [][]spanconfig.Target
		expectedInitScanUpdates    []spanconfig.Record
		expectedInitScanDeletes    spanconfig.Targets
		expectedPersistedUserSpans []spanconfig.Record
	}
	execTestCase := func(tc testCase, step int) {
		if len(tc.deletesDuringPause) == 0 {
			tc.deletesDuringPause = make([][]spanconfig.Target, len(tc.expectedInitScanUpdates))
		}
		for i := range tc.updatesDuringPause {
			require.NoError(t, sourceAccessor.UpdateSpanConfigRecords(ctx, tc.deletesDuringPause[i], tc.updatesDuringPause[i],
				hlc.MinTimestamp,
				hlc.MaxTimestamp), "failed on update %d (index starts at 0)", step)
		}

		ingestor, cleanupIngestor := createDummySpanConfigIngestor(
			ctx,
			t,
			h,
			sourceTenant.ID,
			destTenantID)
		group := ctxgroup.WithContext(ctx)
		defer func() {
			cleanupIngestor()
			require.NoError(t, group.Wait())
		}()
		group.GoCtx(func(ctx context.Context) error {
			return ingestor.ingestSpanConfigs(ctx, sourceTenant.Name)
		})

		toIngest := <-toIngestCh
		require.Equal(t, replicationtestutils.PrettyRecords(tc.expectedInitScanUpdates), replicationtestutils.PrettyRecords(toIngest.toUpdate), "failed on step %d (0 indexed)", step)
		require.Equal(t, fmt.Sprintf("%s", tc.expectedInitScanDeletes), fmt.Sprintf("%s", toIngest.toDelete), "failed on step %d (0 indexed)", step)

		i12DestTarget := []spanconfig.Target{spanconfig.MakeTargetFromSpan(i12Dest)}
		actualSpanConfigRecords, err := ingestor.accessor.GetSpanConfigRecords(ctx, i12DestTarget)
		require.NoError(t, err)
		require.Equal(t,
			replicationtestutils.PrettyRecords(tc.expectedPersistedUserSpans),
			replicationtestutils.PrettyRecords(actualSpanConfigRecords),
			"failed on step %d (0 indexed)", step)
	}
	makeRecord := func(targetSpan roachpb.Span, ttl int) spanconfig.Record {
		return replicationtestutils.MakeSpanConfigRecord(t, targetSpan, ttl)
	}
	for step, tc := range []testCase{
		{
			// Observe the initial scan delete
			//
			// NB: If the client were listening to an actual span config table, it
			// would replicate all existing span configs from the current tenant,
			// replacing the deleted span config records. In this test, the only
			// replicated span config record is the dummy tenant split point created
			// by replicationtestutils.NewReplicationHelperWithDummySpanConfigTable().
			expectedInitScanUpdates: []spanconfig.Record{splitRecord},
			expectedInitScanDeletes: []spanconfig.Target{splitRecord.GetTarget()},
		},
		{
			// Create a Record.
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i1Source, 2)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i1Dest, 2)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget()},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 2)},
		},
		{
			// Update the Span.
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i1Source, 1)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i1Dest, 1)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget(), spanconfig.MakeTargetFromSpan(i1Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 1)},
		},
		{
			// Update the same span config record twice during the pause, and assert that the initial scan replicates only the latest update
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i1Source, 3)}, {makeRecord(i1Source, 4)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i1Dest, 4)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget(), spanconfig.MakeTargetFromSpan(i1Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 4)},
		},
		{
			// Update two records at the same time
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i1Source, 2), makeRecord(i2Source, 1)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i1Dest, 2), makeRecord(i2Dest, 1)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget(), spanconfig.MakeTargetFromSpan(i1Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 2), makeRecord(i2Dest, 1)},
		},
		{
			// Update two records in two different txns
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i1Source, 3)}, {makeRecord(i2Source, 2)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i1Dest, 3), makeRecord(i2Dest, 2)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget(), spanconfig.MakeTargetFromSpan(i1Dest), spanconfig.MakeTargetFromSpan(i2Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 3), makeRecord(i2Dest, 2)},
		},
		{
			// Merge these Records
			deletesDuringPause:         [][]spanconfig.Target{{spanconfig.MakeTargetFromSpan(i2Source)}},
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i12Source, 10)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i12Dest, 10)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget(), spanconfig.MakeTargetFromSpan(i1Dest), spanconfig.MakeTargetFromSpan(i2Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i12Dest, 10)},
		},
		{
			// Split the records
			updatesDuringPause:         [][]spanconfig.Record{{makeRecord(i1Source, 1), makeRecord(i2Source, 2)}},
			expectedInitScanUpdates:    []spanconfig.Record{splitRecord, makeRecord(i1Dest, 1), makeRecord(i2Dest, 2)},
			expectedInitScanDeletes:    []spanconfig.Target{splitRecord.GetTarget(), spanconfig.MakeTargetFromSpan(i12Dest)},
			expectedPersistedUserSpans: []spanconfig.Record{makeRecord(i1Dest, 1), makeRecord(i2Dest, 2)},
		},
	} {
		execTestCase(tc, step)
	}
}

type ingestedRecords struct {
	toUpdate []spanconfig.Record
	toDelete []spanconfig.Target
}

func initFlushHook(
	toIngestCh chan ingestedRecords,
) func(ctx context.Context, bufferedUpdates []spanconfig.Record, bufferedDeletes []spanconfig.Target) {
	return func(ctx context.Context, bufferedUpdates []spanconfig.Record, bufferedDeletes []spanconfig.Target) {
		// Send the updates to the test runner for validation. The deep copy
		// ensures the test runner can inspect the buffer updates event after the
		// ingestor clears the buffers.
		latestIngest := ingestedRecords{}
		latestIngest.toUpdate = make([]spanconfig.Record, len(bufferedUpdates))
		latestIngest.toDelete = make([]spanconfig.Target, len(bufferedDeletes))
		copy(latestIngest.toUpdate, bufferedUpdates)
		copy(latestIngest.toDelete, bufferedDeletes)
		toIngestCh <- latestIngest
	}
}

func createDummySpanConfigIngestor(
	ctx context.Context,
	t *testing.T,
	h *replicationtestutils.ReplicationHelper,
	sourceTenantID, destTenantID roachpb.TenantID,
) (spanConfigIngestor, func()) {
	maybeInlineURL := h.MaybeGenerateInlineURL(t)
	client, err := streamclient.NewSpanConfigStreamClient(ctx, maybeInlineURL)
	require.NoError(t, err)

	rekeyCfg := execinfrapb.TenantRekey{
		OldID: sourceTenantID,
		NewID: destTenantID,
	}

	destTenantStartKey := keys.MakeTenantPrefix(destTenantID)
	destTenantSpan := roachpb.Span{Key: destTenantStartKey, EndKey: destTenantStartKey.PrefixEnd()}

	rekeyer, err := backupccl.MakeKeyRewriterFromRekeys(keys.SystemSQLCodec,
		nil /* tableRekeys */, []execinfrapb.TenantRekey{rekeyCfg},
		true /* restoreTenantFromStream */)
	require.NoError(t, err)

	session, err := h.SysServer.SQLLivenessProvider().(sqlliveness.Provider).Session(ctx)
	require.NoError(t, err)

	stopperCh := make(chan struct{})

	ingestor := spanConfigIngestor{
		accessor:                 h.SysServer.SpanConfigKVAccessor().(spanconfig.KVAccessor),
		settings:                 h.SysServer.ClusterSettings(),
		session:                  session,
		client:                   client,
		rekeyer:                  rekeyer,
		stopperCh:                stopperCh,
		destinationTenantKeySpan: destTenantSpan,
		db:                       h.SysServer.DB(),
		testingKnobs:             h.SysServer.TestingKnobs().Streaming.(*sql.StreamingTestingKnobs),
	}
	return ingestor, func() { close(stopperCh) }
}

// makeTestSpans generates some interesting spans that will appear in the test.
// These include some fake index spans and the tenantSplitPoint. The
// tenantSplitPoint mocks the span config target created during tenant creation.
// For more background on the significance of this span, take a look at
// https://github.com/cockroachdb/cockroach/pull/104920
func makeTestSpans(
	tenantID roachpb.TenantID,
) (roachpb.Span, roachpb.Span, roachpb.Span, roachpb.Span) {
	makeIndexSpan := func(idx uint32, codec keys.SQLCodec) roachpb.Span {
		startKey := codec.IndexPrefix(100, idx)
		return roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()}
	}

	sourceCodec := keys.MakeSQLCodec(tenantID)
	i1, i2 := makeIndexSpan(1, sourceCodec), makeIndexSpan(2, sourceCodec)
	i12 := roachpb.Span{Key: i1.Key, EndKey: i2.EndKey}
	tenantSplitPoint := roachpb.Span{Key: sourceCodec.TenantPrefix(), EndKey: sourceCodec.TenantPrefix().Next()}
	return i1, i2, i12, tenantSplitPoint
}
