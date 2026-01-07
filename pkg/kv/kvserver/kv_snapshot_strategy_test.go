// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/snaprecv"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

// mockIncomingSnapshotStream implements incomingSnapshotStream for testing.
type mockIncomingSnapshotStream struct {
	requests []*kvserverpb.SnapshotRequest
	index    int
	sent     []*kvserverpb.SnapshotResponse
}

func (m *mockIncomingSnapshotStream) Send(resp *kvserverpb.SnapshotResponse) error {
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockIncomingSnapshotStream) Recv() (*kvserverpb.SnapshotRequest, error) {
	if m.index >= len(m.requests) {
		return nil, nil
	}
	req := m.requests[m.index]
	m.index++
	return req, nil
}

// mockKVAdmissionController is a minimal mock that returns nil for GetSnapshotQueue.
type mockKVAdmissionController struct{}

func (m *mockKVAdmissionController) AdmitKVWork(
	_ context.Context, _ roachpb.TenantID, _ roachpb.TenantID, _ *kvpb.BatchRequest,
) (kvadmission.Handle, error) {
	return kvadmission.Handle{}, nil
}

func (m *mockKVAdmissionController) AdmittedKVWorkDone(
	kvadmission.Handle, *kvadmission.StoreWriteBytes,
) {
}

func (m *mockKVAdmissionController) AdmitRangefeedRequest(
	_ roachpb.TenantID, _ *kvpb.RangeFeedRequest,
) *admission.Pacer {
	return nil
}

func (m *mockKVAdmissionController) SetTenantWeightProvider(
	kvadmission.TenantWeightProvider, *stop.Stopper,
) {
}

func (m *mockKVAdmissionController) SnapshotIngestedOrWritten(
	_ roachpb.StoreID, _ pebble.IngestOperationStats, _ uint64,
) {
}

func (m *mockKVAdmissionController) FollowerStoreWriteBytes(
	_ roachpb.StoreID, _ kvadmission.FollowerStoreWriteBytes,
) {
}

func (m *mockKVAdmissionController) AdmitRaftEntry(
	_ context.Context, _ roachpb.TenantID, _ roachpb.StoreID, _ roachpb.RangeID, _ raftpb.Entry,
) (admitted bool, err error) {
	return true, nil
}

func (m *mockKVAdmissionController) OnBypassed(_ roachpb.StoreID, _ roachpb.RangeID, _ int64) {
}

func (m *mockKVAdmissionController) OnDestroyRaftMuLocked(_ roachpb.StoreID, _ roachpb.RangeID) {
}

func (m *mockKVAdmissionController) Admit(
	_ context.Context, _ replica_rac2.EntryForAdmission,
) bool {
	return true
}

func (m *mockKVAdmissionController) GetSnapshotQueue(_ roachpb.StoreID) *admission.SnapshotQueue {
	return nil
}

func (m *mockKVAdmissionController) GetProvisionedBandwidth(_ roachpb.StoreID) int64 {
	return 0
}

// TestKVBatchSnapshotStrategyReceiveExternalReplicate tests the
// kvBatchSnapshotStrategy.Receive method with ExternalReplicate=true. This
// ensures the production code correctly handles DEL and other Pebble internal
// key kinds, when receiving external SST snapshots.
//
// The bug fixes covered by this test: (a) when ExternalReplicate=true (but
// SharedReplicate=false), the code was incorrectly passing false to ReadOne's
// sharedOrExternal parameter, causing errors when encountering Pebble
// internal keys, (b) DEL keys were having their value retrieved using
// BatchReader.Value, which resulted in a panic.
func TestKVBatchSnapshotStrategyReceiveExternalReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create test store configuration.
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.KVAdmissionController = &mockKVAdmissionController{}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Create a minimal Store with only the fields needed by Receive.
	testIdent := roachpb.StoreIdent{
		ClusterID: uuid.MakeV4(),
		NodeID:    1,
		StoreID:   1,
	}
	store := &Store{
		cfg:   cfg,
		Ident: &testIdent,
	}

	// Create range descriptor for the test.
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}

	// Create snapshot UUID.
	snapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))

	// Create SST snapshot storage and scratch space.
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, nil)

	// Helper to create a batch repr with specified operations.
	makeBatchRepr := func(fn func(storage.WriteBatch)) []byte {
		batch := eng.NewWriteBatch()
		defer batch.Close()
		fn(batch)
		repr := batch.Repr()
		reprCopy := make([]byte, len(repr))
		copy(reprCopy, repr)
		return reprCopy
	}

	now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	t.Run("external-replicate-with-del-succeeds", func(t *testing.T) {
		// Create a batch with a DEL operation (which requires sharedOrExternal=true).
		mvccKey := storage.MVCCKey{Key: roachpb.Key("e"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		kvBatch := makeBatchRepr(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindDelete),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, nil))
		})

		// Create the mock stream that returns the batch followed by a Final request.
		stream := &mockIncomingSnapshotStream{
			requests: []*kvserverpb.SnapshotRequest{
				{KVBatch: kvBatch},
				{Final: true},
			},
		}

		// Create the header with ExternalReplicate=true.
		header := kvserverpb.SnapshotRequest_Header{
			SharedReplicate:   false,
			ExternalReplicate: true,
			State: kvserverpb.ReplicaState{
				Desc: desc,
			},
			RaftMessageRequest: kvserverpb.RaftMessageRequest{
				Message: raftpb.Message{
					Snapshot: &raftpb.Snapshot{
						Data: snapUUID.GetBytes(),
					},
				},
			},
		}

		// Create the kvBatchSnapshotStrategy.
		kvSS := &kvBatchSnapshotStrategy{
			st:      cfg.Settings,
			scratch: scratch,
		}

		// Call the actual Receive method - this tests the production code.
		inSnap, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
		require.NoError(t, err, "Receive should succeed with ExternalReplicate=true and DEL operation")
		require.Equal(t, snapUUID, inSnap.SnapUUID)
	})

	t.Run("no-external-with-del-fails", func(t *testing.T) {
		// Create a new scratch space for this subtest.
		scratch2 := sstSnapshotStorage.NewScratchSpace(desc.RangeID, uuid.MakeV4(), nil)

		// Create a batch with a DEL operation.
		mvccKey := storage.MVCCKey{Key: roachpb.Key("f"), Timestamp: now}
		encodedKey := storage.EncodeMVCCKey(mvccKey)
		kvBatch := makeBatchRepr(func(b storage.WriteBatch) {
			ik := pebble.InternalKey{
				UserKey: encodedKey,
				Trailer: pebble.MakeInternalKeyTrailer(0, pebble.InternalKeyKindDelete),
			}
			require.NoError(t, b.PutInternalPointKey(&ik, nil))
		})

		// Create the mock stream.
		stream := &mockIncomingSnapshotStream{
			requests: []*kvserverpb.SnapshotRequest{
				{KVBatch: kvBatch},
				{Final: true},
			},
		}

		// Create the header with ExternalReplicate=false and SharedReplicate=false.
		snapUUID2 := uuid.MakeV4()
		header := kvserverpb.SnapshotRequest_Header{
			SharedReplicate:   false,
			ExternalReplicate: false,
			State: kvserverpb.ReplicaState{
				Desc: desc,
			},
			RaftMessageRequest: kvserverpb.RaftMessageRequest{
				Message: raftpb.Message{
					Snapshot: &raftpb.Snapshot{
						Data: snapUUID2.GetBytes(),
					},
				},
			},
		}

		// Create the kvBatchSnapshotStrategy.
		kvSS := &kvBatchSnapshotStrategy{
			st:      cfg.Settings,
			scratch: scratch2,
		}

		// Call the actual Receive method - should fail.
		_, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
		require.Error(t, err, "Receive should fail with DEL operation when neither SharedReplicate nor ExternalReplicate is set")
		require.Contains(t, err.Error(), "unexpected batch entry key kind")
	})
}

// TestSnapshotWithBlobFiles tests the snapshot receive and apply flow with
// blob files (value separation) enabled. This exercises:
// 1. MultiSSTWriter with blob files enabled (via kvBatchSnapshotStrategy.Receive).
// 2. snapWriter.commit() which calls IngestAndExciseWithBlobs.
// 3. Read-back verification to ensure values can be read after ingestion.
func TestSnapshotWithBlobFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a temp directory for the on-disk engine.
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	// Create settings with blob files enabled.
	settings := cluster.MakeTestingClusterSettings()
	storage.ValueSeparationEnabled.Override(ctx, &settings.SV, true)
	storage.ValueSeparationSnapshotSSTEnabled.Override(ctx, &settings.SV, true)
	storage.ValueSeparationMinimumSize.Override(ctx, &settings.SV, 256)

	// Create on-disk engine.
	eng, err := storage.Open(
		ctx,
		fs.MustInitPhysicalTestingEnv(dir),
		settings)
	require.NoError(t, err)
	defer eng.Close()

	// Test store configuration.
	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.Settings = settings
	cfg.KVAdmissionController = &mockKVAdmissionController{}

	testIdent := roachpb.StoreIdent{ClusterID: uuid.MakeV4(), NodeID: 1, StoreID: 1}
	store := &Store{cfg: cfg, Ident: &testIdent}

	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("z"),
	}

	snapUUID := uuid.MakeV4()
	sstSnapshotStorage := snaprecv.NewSSTSnapshotStorage(eng, rate.NewLimiter(rate.Inf, 0))
	scratch := sstSnapshotStorage.NewScratchSpace(desc.RangeID, snapUUID, settings)

	// Use a large value (>256 bytes to trigger blob separation).
	largeValueBytes := make([]byte, 512)
	for i := range largeValueBytes {
		largeValueBytes[i] = byte(i % 256)
	}
	largeValue := roachpb.MakeValueFromBytes(largeValueBytes)
	mvccValue := storage.MVCCValue{Value: largeValue}

	// Generate batches with large MVCC values.
	now := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	makeBatchRepr := func(fn func(storage.WriteBatch)) []byte {
		batch := eng.NewWriteBatch()
		defer batch.Close()
		fn(batch)
		repr := batch.Repr()
		reprCopy := make([]byte, len(repr))
		copy(reprCopy, repr)
		return reprCopy
	}

	// Generate 10 batches, each with one large value.
	var kvBatches [][]byte
	for i := 0; i < 10; i++ {
		key := roachpb.Key(fmt.Sprintf("key%03d", i))
		mvccKey := storage.MVCCKey{Key: key, Timestamp: now}
		kvBatch := makeBatchRepr(func(b storage.WriteBatch) {
			require.NoError(t, b.PutMVCC(mvccKey, mvccValue))
		})
		kvBatches = append(kvBatches, kvBatch)
	}

	// Generate mock stream requests.
	requests := make([]*kvserverpb.SnapshotRequest, 0, len(kvBatches)+1)
	for _, batch := range kvBatches {
		requests = append(requests, &kvserverpb.SnapshotRequest{KVBatch: batch})
	}
	requests = append(requests, &kvserverpb.SnapshotRequest{Final: true})
	stream := &mockIncomingSnapshotStream{requests: requests}

	header := kvserverpb.SnapshotRequest_Header{
		State: kvserverpb.ReplicaState{Desc: desc},
		RaftMessageRequest: kvserverpb.RaftMessageRequest{
			Message: raftpb.Message{
				Snapshot: &raftpb.Snapshot{Data: snapUUID.GetBytes()},
			},
		},
	}

	// Create and call kvBatchSnapshotStrategy.Receive.
	kvSS := &kvBatchSnapshotStrategy{st: settings, scratch: scratch}
	inSnap, err := kvSS.Receive(ctx, store, stream, header, func(int64) {})
	require.NoError(t, err)
	require.Equal(t, snapUUID, inSnap.SnapUUID)

	// Verify blob files were created.
	require.True(t, scratch.HasBlobFiles(), "expected blob files to be created")

	// Apply snapshot.
	engs := kvstorage.MakeEngines(eng)
	sw := snapWriter{
		eng:      engs,
		writeSST: scratch.WriteSST,
	}
	defer sw.close()

	// Commit the snapshot.
	_, err = sw.commit(ctx, snapIngestion{
		localSSTs:  scratch.SSTs(),
		exciseSpan: desc.KeySpan().AsRawSpanWithNoLocals(),
	})
	require.NoError(t, err)

	// Read back and verify values are readable after blob file ingestion.
	iter, err := eng.NewMVCCIterator(ctx, storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
		LowerBound: roachpb.Key("a"),
		UpperBound: roachpb.Key("z"),
	})
	require.NoError(t, err)
	defer iter.Close()

	count := 0
	iter.SeekGE(storage.MVCCKey{Key: roachpb.Key("a")})
	for {
		valid, err := iter.Valid()
		require.NoError(t, err)
		if !valid {
			break
		}
		k := iter.UnsafeKey()
		expectedKey := roachpb.Key(fmt.Sprintf("key%03d", count))
		require.Equal(t, expectedKey, k.Key)
		require.Equal(t, now, k.Timestamp)
		rawVal, err := iter.UnsafeValue()
		require.NoError(t, err)
		val, err := storage.DecodeMVCCValue(rawVal)
		require.NoError(t, err)
		readBytes, err := val.Value.GetBytes()
		require.NoError(t, err)
		require.Equal(t, largeValueBytes, readBytes)
		iter.Next()
		count++
	}
	require.Equal(t, 10, count, "expected 10 keys")
}
