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
CREATE TABLE d.kv (k INT PRIMARY KEY, v STRING);
`)

	// Start replication stream with ordered mode enabled.
	replicationProducerSpec := h.StartReplicationStream(t, testTenantName)
	streamID := replicationProducerSpec.StreamID
	initialScanTimestamp := replicationProducerSpec.ReplicationStartTime

	t1Desc := desctestutils.TestingGetPublicTableDescriptor(
		h.SysServer.DB(),
		srcTenant.Codec,
		"d",
		"kv",
	)
	spans := []roachpb.Span{t1Desc.PrimaryIndexSpan(srcTenant.Codec)}

	// Build partition spec with ordered mode.
	spec := &streampb.StreamPartitionSpec{
		InitialScanTimestamp:        initialScanTimestamp,
		PreviousReplicatedTimestamp: hlc.Timestamp{},
		Spans:                       spans,
		Progress: []jobspb.ResolvedSpan{
			{Span: spans[0], Timestamp: hlc.Timestamp{}},
		},
		WrappedEvents:    true,
		Type:             streampb.ReplicationType_LOGICAL,
		WithMvccOrdering: true, // Enable ordered stream handler.
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

	// Generate and execute random operations concurrently.
	const numKeys = 20
	const numOps = 100

	type op struct {
		key   int
		value string
	}

	ops := make([]op, numOps)
	for i := 0; i < numOps; i++ {
		ops[i] = op{
			key:   rng.Intn(numKeys),
			value: fmt.Sprintf("op-%d", i),
		}
	}

	// Shuffle operations to ensure they happen out of key order.
	rng.Shuffle(len(ops), func(i, j int) {
		ops[i], ops[j] = ops[j], ops[i]
	})

	// Execute all operations in background using UPSERT.
	go func() {
		for _, operation := range ops {
			srcTenant.SQL.Exec(t,
				"INSERT INTO d.kv (k, v) VALUES ($1, $2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
				operation.key,
				operation.value,
			)
		}
	}()

	// Consume events and verify ordering.
	var lastTS hlc.Timestamp
	var lastKey roachpb.Key
	var lastCheckpointTS hlc.Timestamp

	kvCount := 0
	checkpointCount := 0
	seenEvents := make(map[string]bool)
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

				// Ensure no new events arrive with timestamp before the last checkpoint.
				// A checkpoint at time T means all events with ts <= T have been sent.
				// Duplicate events (resends) with ts <= checkpoint are OK.
				eventKey := fmt.Sprintf("%s@%s", currentKey, currentTS)
				isNewEvent := !seenEvents[eventKey]
				seenEvents[eventKey] = true
				require.False(
					t,
					!lastCheckpointTS.IsEmpty() && currentTS.Less(lastCheckpointTS) && isNewEvent,
					"new event at ts %s arrived after checkpoint advanced to %s",
					currentTS,
					lastCheckpointTS,
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
				if isNewEvent {
					kvCount++
				}

				// Stop after receiving all expected events.
				if kvCount >= numOps {
					break consumeStream
				}
			}
		}

		if streamEvent.Checkpoint != nil {
			checkpointCount++
			if len(streamEvent.Checkpoint.ResolvedSpans) > 0 {
				checkpointTS := streamEvent.Checkpoint.ResolvedSpans[0].Timestamp
				// Ensure checkpoint timestamps don't go backwards.
				require.False(
					t,
					!lastCheckpointTS.IsEmpty() && checkpointTS.Less(lastCheckpointTS),
					"checkpoint timestamp went backwards: %s -> %s",
					lastCheckpointTS,
					checkpointTS,
				)
				lastCheckpointTS = checkpointTS
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
