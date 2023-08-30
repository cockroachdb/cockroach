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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	const dummySpanConfigurationsName = "dummy_span_configurations"
	dummyFQN := tree.NewTableNameWithSchema("d", catconstants.PublicSchemaName, dummySpanConfigurationsName)

	type toIngest struct {
		toUpdate []spanconfig.Record
		toDelete []spanconfig.Target
	}
	toIngestCh := make(chan toIngest)

	streamingTestKnobs := &sql.StreamingTestingKnobs{
		MockSpanConfigTableName: dummyFQN,
		BeforeIngestSpanConfigFlush: func(ctx context.Context, bufferedUpdates []spanconfig.Record, bufferedDeletes []spanconfig.Target) {
			// Send the updates to the test runner for validation. The deep copy
			// ensures the test runner can inspect the buffer updates event after the
			// ingestor clears the buffers.
			latestIngest := toIngest{}
			latestIngest.toUpdate = make([]spanconfig.Record, len(bufferedUpdates))
			latestIngest.toDelete = make([]spanconfig.Target, len(bufferedDeletes))
			copy(latestIngest.toUpdate, bufferedUpdates)
			copy(latestIngest.toDelete, bufferedDeletes)
			toIngestCh <- latestIngest
		},
	}

	h, cleanup := replicationtestutils.NewReplicationHelper(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Streaming: streamingTestKnobs,
		},
	})
	defer cleanup()

	h.SysSQL.Exec(t, `
CREATE DATABASE d;
USE d;`)
	h.SysSQL.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyFQN))

	sourceAccessor := spanconfigkvaccessor.New(
		h.SysServer.DB(),
		h.SysServer.InternalExecutor().(isql.Executor),
		h.SysServer.ClusterSettings(),
		h.SysServer.Clock(),
		dummyFQN.String(),
		nil, /* knobs */
	)

	sourceTenantID := roachpb.MustMakeTenantID(uint64(10))
	destTenantID := roachpb.MustMakeTenantID(uint64(11))
	tenantName := roachpb.TenantName("app")
	_, tenantCleanup := h.CreateTenant(t, sourceTenantID, tenantName)
	defer tenantCleanup()
	ingestor, cleanupIngestor := createDummySpanConfigIngestor(
		ctx,
		t,
		h,
		execinfrapb.TenantRekey{
			OldID: sourceTenantID,
			NewID: destTenantID,
		})
	group := ctxgroup.WithContext(ctx)
	defer func() {
		cleanupIngestor()
		require.NoError(t, group.Wait())
	}()
	group.GoCtx(func(ctx context.Context) error {
		return ingestor.ingestSpanConfigs(ctx, tenantName)
	})

	makeRecord := func(targetSpan roachpb.Span, ttl int) spanconfig.Record {
		return replicationtestutils.MakeSpanConfigRecord(t, targetSpan, ttl)
	}

	prettyRecords := func(records []spanconfig.Record) string {
		var b strings.Builder
		for _, update := range records {
			b.WriteString(fmt.Sprintf(" %s: ttl %d,", update.GetTarget().GetSpan(), update.GetConfig().GCPolicy.TTLSeconds))
		}
		return b.String()
	}

	i1Source, i2Source, i12Source := makeTestSpans(sourceTenantID)
	i1Dest, i2Dest, i12Dest := makeTestSpans(destTenantID)
	destTarget := []spanconfig.Target{spanconfig.MakeTargetFromSpan(i12Dest)}
	for ts, toApply := range []struct {
		updates           []spanconfig.Record
		deletes           []spanconfig.Target
		expectedUpdates   []spanconfig.Record
		expectedDeletes   spanconfig.Targets
		expectedPersisted []spanconfig.Record
	}{
		{
			// Create a Record.
			updates:           []spanconfig.Record{makeRecord(i1Source, 2)},
			expectedUpdates:   []spanconfig.Record{makeRecord(i1Dest, 2)},
			expectedPersisted: []spanconfig.Record{makeRecord(i1Dest, 2)},
		},
		{
			// Update the Span.
			updates:           []spanconfig.Record{makeRecord(i1Source, 3)},
			expectedUpdates:   []spanconfig.Record{makeRecord(i1Dest, 3)},
			expectedPersisted: []spanconfig.Record{makeRecord(i1Dest, 3)},
		},
		{
			// Add Two new records at the same time
			updates:           []spanconfig.Record{makeRecord(i2Source, 2)},
			expectedUpdates:   []spanconfig.Record{makeRecord(i2Dest, 2)},
			expectedPersisted: []spanconfig.Record{makeRecord(i1Dest, 3), makeRecord(i2Dest, 2)},
		},
		{
			// Merge these Records
			deletes:           []spanconfig.Target{spanconfig.MakeTargetFromSpan(i2Source)},
			updates:           []spanconfig.Record{makeRecord(i12Source, 10)},
			expectedUpdates:   []spanconfig.Record{makeRecord(i12Dest, 10)},
			expectedDeletes:   []spanconfig.Target{spanconfig.MakeTargetFromSpan(i2Dest)},
			expectedPersisted: []spanconfig.Record{makeRecord(i12Dest, 10)},
		},
		{
			// Split the records
			updates:           []spanconfig.Record{makeRecord(i1Source, 1), makeRecord(i2Source, 2)},
			expectedUpdates:   []spanconfig.Record{makeRecord(i1Dest, 1), makeRecord(i2Dest, 2)},
			expectedPersisted: []spanconfig.Record{makeRecord(i1Dest, 1), makeRecord(i2Dest, 2)},
		},
	} {
		toApply := toApply
		require.NoError(t, sourceAccessor.UpdateSpanConfigRecords(ctx, toApply.deletes, toApply.updates,
			hlc.MinTimestamp,
			hlc.MaxTimestamp), "failed on update %d (index starts at 0)", ts)
		toIngest := <-toIngestCh
		require.Equal(t, prettyRecords(toApply.expectedUpdates), prettyRecords(toIngest.toUpdate), "failed on step %d (0 indexed)", ts)
		require.Equal(t, fmt.Sprintf("%s", toApply.expectedDeletes), fmt.Sprintf("%s", toIngest.toDelete), "failed on step %d (0 indexed)", ts)

		testutils.SucceedsWithin(t, func() error {
			actualSpanConfigRecords, err := ingestor.accessor.GetSpanConfigRecords(ctx, destTarget)
			if err != nil {
				return err
			}
			actualPretty := prettyRecords(actualSpanConfigRecords)
			expectedPretty := prettyRecords(toApply.expectedPersisted)
			if actualPretty != expectedPretty {
				return errors.Newf("Actual records %s not equal to expected records %s", actualSpanConfigRecords, toApply.expectedPersisted)
			}
			return nil
		}, time.Second*5)
	}
}

func createDummySpanConfigIngestor(
	ctx context.Context,
	t *testing.T,
	h *replicationtestutils.ReplicationHelper,
	rekeyCfg execinfrapb.TenantRekey,
) (spanConfigIngestor, func()) {
	maybeInlineURL := h.MaybeGenerateInlineURL(t)
	client, err := streamclient.NewSpanConfigStreamClient(ctx, maybeInlineURL)
	require.NoError(t, err)

	rekeyer, err := backupccl.MakeKeyRewriterFromRekeys(keys.SystemSQLCodec,
		nil /* tableRekeys */, []execinfrapb.TenantRekey{rekeyCfg},
		true /* restoreTenantFromStream */)
	require.NoError(t, err)

	session, err := h.SysServer.SQLLivenessProvider().(sqlliveness.Provider).Session(ctx)
	require.NoError(t, err)

	stopperCh := make(chan struct{})

	ingestor := spanConfigIngestor{
		accessor:     h.SysServer.SpanConfigKVAccessor().(spanconfig.KVAccessor),
		settings:     h.SysServer.ClusterSettings(),
		session:      session,
		client:       client,
		rekeyer:      rekeyer,
		stopperCh:    stopperCh,
		testingKnobs: h.SysServer.TestingKnobs().Streaming.(*sql.StreamingTestingKnobs),
	}
	return ingestor, func() { close(stopperCh) }
}

func makeTestSpans(tenantID roachpb.TenantID) (roachpb.Span, roachpb.Span, roachpb.Span) {
	makeIndexSpan := func(idx uint32, codec keys.SQLCodec) roachpb.Span {
		startKey := codec.IndexPrefix(100, idx)
		return roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()}
	}

	sourceCodec := keys.MakeSQLCodec(tenantID)
	i1, i2 := makeIndexSpan(1, sourceCodec), makeIndexSpan(2, sourceCodec)
	i12 := roachpb.Span{Key: i1.Key, EndKey: i2.EndKey}
	return i1, i2, i12
}
