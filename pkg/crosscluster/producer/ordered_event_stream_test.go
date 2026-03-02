// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestOrderedStreamE2E_RandomKVStream tests end-to-end ordered event streaming
// by generating random KV operations (inserts and updates), streaming them over
// a SQL connection, and verifying they arrive in (timestamp, key) sorted order.
func TestOrderedStreamE2E_RandomKVStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Test takes 30 seconds to run.
	skip.UnderStress(t, "slow test")

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	// Set up test cluster with tenant.
	h, cleanup := replicationtestutils.NewReplicationHelper(
		t,
		base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	)
	defer cleanup()

	testTenantName := roachpb.TenantName("test-tenant")
	srcTenant, cleanupTenant := h.CreateTenant(t, serverutils.TestTenantID(), testTenantName)
	defer cleanupTenant()

	srcTenant.SQL.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.kv1 (k INT PRIMARY KEY, v STRING);
CREATE TABLE d.kv2 (k INT PRIMARY KEY, v STRING);
`)

	// Start replication stream with ordered mode enabled.
	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	streamID := replicationProducerSpec.StreamID
	initialScanTimestamp := replicationProducerSpec.ReplicationStartTime

	// Get descriptors for all tables.
	tableNames := []string{"kv1", "kv2"}
	spans := make([]roachpb.Span, len(tableNames))
	progress := make([]jobspb.ResolvedSpan, len(tableNames))
	for i, tableName := range tableNames {
		desc := desctestutils.TestingGetPublicTableDescriptor(
			h.SysServer.DB(),
			srcTenant.Codec,
			"d",
			tableName,
		)
		spans[i] = desc.PrimaryIndexSpan(srcTenant.Codec)
		progress[i] = jobspb.ResolvedSpan{Span: spans[i], Timestamp: hlc.Timestamp{}}
	}

	// Build partition spec with ordered mode.
	spec := &streampb.StreamPartitionSpec{
		InitialScanTimestamp:        initialScanTimestamp,
		PreviousReplicatedTimestamp: hlc.Timestamp{},
		Spans:                       spans,
		Progress:                    progress,
		WrappedEvents:               true,
		Type:                        streampb.ReplicationType_LOGICAL,
		WithMvccOrdering:            true, // Enable ordered stream handler.
		Config: streampb.StreamPartitionSpec_ExecutionConfig{
			MinCheckpointFrequency: 100 * time.Millisecond,
			// Smaller batch size to increase non-checkpoint flushes to disk.
			BatchByteSize: 1000,
		},
	}

	opaqueSpec, err := protoutil.Marshal(spec)
	require.NoError(t, err)

	// Start streaming over SQL connection.
	sink := h.PGUrl
	sink.RawQuery = h.PGUrl.Query().Encode()

	pgxConfig, err := pgx.ParseConfig(sink.String())
	require.NoError(t, err)

	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	conn, err := pgx.ConnectConfig(queryCtx, pgxConfig)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close(ctx)) }()

	rows, err := conn.Query(queryCtx, `SET avoid_buffering = true`)
	require.NoError(t, err)
	rows.Close()

	const streamPartitionQuery = `SELECT * FROM crdb_internal.stream_partition($1, $2)`
	rows, err = conn.Query(queryCtx, streamPartitionQuery, streamID, opaqueSpec)
	require.NoError(t, err)
	defer rows.Close()

	// Generate and execute random operations concurrently across all tables.
	const numKeys = 20
	const numOps = 100

	type op struct {
		table string
		key   int
		value string
	}

	ops := make([]op, numOps)
	for i := 0; i < numOps; i++ {
		ops[i] = op{
			table: tableNames[rng.Intn(len(tableNames))],
			key:   rng.Intn(numKeys),
			value: fmt.Sprintf("op-%d", i),
		}
	}

	// Shuffle operations to ensure they happen out of key and table order.
	rng.Shuffle(len(ops), func(i, j int) {
		ops[i], ops[j] = ops[j], ops[i]
	})

	// Execute all operations in background using UPSERT.
	go func() {
		for _, operation := range ops {
			srcTenant.SQL.Exec(t,
				fmt.Sprintf("INSERT INTO d.%s (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v", operation.table),
				operation.key,
				operation.value,
			)
		}
	}()

	// Consume events and verify ordering.
	var lastTS hlc.Timestamp
	var lastKey roachpb.Key
	checkpointFrontier, err := span.MakeFrontier(spans...)
	require.NoError(t, err)

	kvCount := 0
	checkpointCount := 0
	timeout := time.After(30 * time.Second)

consumeStream:
	for {
		select {
		case <-timeout:
			t.Fatalf("timeout: received %d unique events, expected %d", kvCount, numOps)
		default:
		}

		if !rows.Next() {
			break consumeStream
		}

		var data []byte
		require.NoError(t, rows.Scan(&data))

		var streamEvent streampb.StreamEvent
		require.NoError(t, protoutil.Unmarshal(data, &streamEvent))

		if streamEvent.Batch != nil {
			for _, kv := range streamEvent.Batch.KVs {
				// Verify ordering: (timestamp, key) must be non-decreasing.
				currentTS := kv.KeyValue.Value.Timestamp
				currentKey := kv.KeyValue.Key

				// Ensure no new events arrive with timestamp before the checkpoint frontier.
				// A checkpoint at time T for a span means all events with ts <= T for that span
				//  have been sent.
				frontierTS := checkpointFrontier.Frontier()
				require.False(
					t,
					!frontierTS.IsEmpty() && currentTS.Less(frontierTS),
					"new event at ts %s arrived after checkpoint frontier advanced to %s (key: %s)",
					currentTS,
					frontierTS,
					currentKey,
				)

				if !lastTS.IsEmpty() {
					cmpTS := currentTS.Compare(lastTS)
					require.False(t, cmpTS < 0, "ordering violation: current ts %s < last ts %s", currentTS, lastTS)
					require.False(
						t,
						cmpTS == 0 && currentKey.Compare(lastKey) < 0,
						"ordering violation at same ts %s: current key %s < last key %s",
						currentTS,
						currentKey,
						lastKey,
					)
				}

				lastTS = currentTS
				lastKey = currentKey
				kvCount++

				// Stop after receiving all expected events.
				if kvCount >= numOps {
					break consumeStream
				}
			}
		}

		if streamEvent.Checkpoint != nil {
			checkpointCount++
			for _, resolvedSpan := range streamEvent.Checkpoint.ResolvedSpans {
				var prevTS hlc.Timestamp
				for _, ts := range checkpointFrontier.SpanEntries(resolvedSpan.Span) {
					prevTS = ts
					break
				}
				// Ensure checkpoint timestamps don't go backwards for each span.
				require.False(
					t,
					!prevTS.IsEmpty() && resolvedSpan.Timestamp.Less(prevTS),
					"checkpoint timestamp for span %s went backwards: %s -> %s",
					resolvedSpan.Span,
					prevTS,
					resolvedSpan.Timestamp,
				)
				// Update frontier with this resolved span.
				_, err := checkpointFrontier.Forward(resolvedSpan.Span, resolvedSpan.Timestamp)
				require.NoError(t, err)
			}
		}
	}

	t.Logf("Received %d events and %d checkpoints from %d operations", kvCount, checkpointCount, numOps)

	require.GreaterOrEqual(
		t,
		kvCount,
		numOps,
		"expected to receive at least %d events, got %d",
		numOps,
		kvCount,
	)
}
