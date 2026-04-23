// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var _ txnfeed.Stream = (*txnFeedStream)(nil)

// txnFeedStream is a test implementation of txnfeed.Stream that collects
// events and errors.
type txnFeedStream struct {
	mu struct {
		sync.Mutex
		events []*kvpb.TxnFeedEvent
	}
	errCh chan *kvpb.Error
}

func newTxnFeedStream() *txnFeedStream {
	return &txnFeedStream{
		errCh: make(chan *kvpb.Error, 1),
	}
}

// SendBuffered implements txnfeed.Stream.
func (s *txnFeedStream) SendBuffered(event *kvpb.TxnFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.events = append(s.mu.events, event)
	return nil
}

// SendUnbuffered implements txnfeed.Stream.
func (s *txnFeedStream) SendUnbuffered(event *kvpb.TxnFeedEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.events = append(s.mu.events, event)
	return nil
}

// SendError implements txnfeed.Stream.
func (s *txnFeedStream) SendError(pErr *kvpb.Error) {
	s.errCh <- pErr
}

func (s *txnFeedStream) events() []*kvpb.TxnFeedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]*kvpb.TxnFeedEvent, len(s.mu.events))
	copy(result, s.mu.events)
	return result
}

// committedTxnIDs returns the set of transaction IDs from Committed events.
func (s *txnFeedStream) committedTxnIDs() map[uuid.UUID]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make(map[uuid.UUID]struct{})
	for _, ev := range s.mu.events {
		if ev.Committed != nil {
			ids[ev.Committed.TxnID] = struct{}{}
		}
	}
	return ids
}

// maxCheckpointTS returns the maximum resolved timestamp seen across all
// checkpoint events.
func (s *txnFeedStream) maxCheckpointTS() hlc.Timestamp {
	s.mu.Lock()
	defer s.mu.Unlock()
	var maxTS hlc.Timestamp
	for _, ev := range s.mu.events {
		if ev.Checkpoint != nil {
			maxTS.Forward(ev.Checkpoint.ResolvedTS)
		}
	}
	return maxTS
}

// txnResult captures the transaction ID and the keys that were read during the
// transaction.
type txnResult struct {
	txnID    uuid.UUID
	readKeys []roachpb.Key
}

// run1PCTxn executes a 1PC transaction that reads a key and puts a single key
// in the given span.
func run1PCTxn(
	t *testing.T, ctx context.Context, db *kv.DB, readKey, writeKey roachpb.Key,
) txnResult {
	t.Helper()
	// Pre-populate the read key so the Get returns a value.
	require.NoError(t, db.Put(ctx, readKey, "pre-existing"))

	var result txnResult
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		result.txnID = txn.ID()
		// Perform a read.
		_, err := txn.Get(ctx, readKey)
		if err != nil {
			return err
		}
		b := txn.NewBatch()
		b.Put(writeKey, "1pc-value")
		return txn.CommitInBatch(ctx, b)
	})
	require.NoError(t, err)
	result.readKeys = []roachpb.Key{readKey}
	return result
}

// run2PCTxn executes a 2PC transaction that reads a key and puts keys in two
// different ranges. The two write keys must be in different ranges to force the
// 2PC path.
func run2PCTxn(
	t *testing.T, ctx context.Context, db *kv.DB, readKey, writeKey1, writeKey2 roachpb.Key,
) txnResult {
	t.Helper()
	// Pre-populate the read key so the Get returns a value.
	require.NoError(t, db.Put(ctx, readKey, "pre-existing"))

	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	result := txnResult{txnID: txn.ID()}
	// Perform a read.
	_, err := txn.Get(ctx, readKey)
	require.NoError(t, err)
	require.NoError(t, txn.Put(ctx, writeKey1, "2pc-value-1"))
	require.NoError(t, txn.Put(ctx, writeKey2, "2pc-value-2"))
	require.NoError(t, txn.Commit(ctx))
	result.readKeys = []roachpb.Key{readKey}
	return result
}

// registerTxnFeed registers a txnfeed on the store for the given range and
// span. It returns the stream and a disconnector.
func registerTxnFeed(
	t *testing.T, store *kvserver.Store, rangeID roachpb.RangeID, span roachpb.Span, ts hlc.Timestamp,
) (*txnFeedStream, txnfeed.Disconnector) {
	t.Helper()
	stream := newTxnFeedStream()
	req := &kvpb.TxnFeedRequest{
		Header: kvpb.Header{
			RangeID:   rangeID,
			Timestamp: ts,
			Replica: roachpb.ReplicaDescriptor{
				StoreID: store.StoreID(),
			},
		},
		AnchorSpan: span,
	}
	disconnector, err := store.TxnFeed(context.Background(), req, stream)
	require.NoError(t, err)
	return stream, disconnector
}

// TestTxnFeed verifies that the TxnFeed delivers committed transaction events
// for both 1PC and 2PC transactions. It runs two subtests:
//   - "live" registers the feed before writing, so events arrive via Raft apply.
//   - "catchup" writes first, then registers with an older cursor so events
//     arrive via the catch-up scan.
func TestTxnFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	// TODO(txnfeed): Enable test tenants once the txnFeedReadTracker interceptor
	// correctly propagates the TxnFeedEnabled setting across tenant boundaries.
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Set a short closed timestamp interval so checkpoints arrive quickly.
	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	ts := tc.Server(0)
	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	db := ts.DB()

	// Split off two scratch ranges so the 2PC txn spans ranges. The 1PC txn
	// writes into the first range; the 2PC txn writes one key in each range
	// to force the two-phase commit path.
	scratchKey := tc.ScratchRange(t)
	midKey := append(scratchKey.Clone(), 'm')
	tc.SplitRangeOrFatal(t, midKey)

	feedSpan := roachpb.Span{Key: scratchKey, EndKey: midKey}
	rangeID := store.LookupReplica(roachpb.RKey(scratchKey)).RangeID

	// Read and write keys for the 1PC transaction.
	readKey1PC := append(scratchKey.Clone(), '0')       // in [scratchKey, midKey)
	writeKey1PC := append(scratchKey.Clone(), '1')      // in [scratchKey, midKey)
	readKey2PC := append(scratchKey.Clone(), '3')       // in [scratchKey, midKey)
	writeKey2PCLocal := append(scratchKey.Clone(), '2') // in [scratchKey, midKey)
	writeKey2PCRemote := append(midKey.Clone(), 'r')    // in second range

	t.Run("live", func(t *testing.T) {
		// Register the feed before writing so events arrive via Raft apply.
		startTS := db.Clock().Now()
		stream, disconnector := registerTxnFeed(t, store, rangeID, feedSpan, startTS)
		defer disconnector.Disconnect(nil)

		txn1PC := run1PCTxn(t, ctx, db, readKey1PC, writeKey1PC)
		txn2PC := run2PCTxn(t, ctx, db, readKey2PC, writeKey2PCLocal, writeKey2PCRemote)
		afterWriteTS := db.Clock().Now()

		awaitAndVerifyTxnFeedEvents(t, stream, feedSpan, afterWriteTS, txn1PC, txn2PC)
	})

	t.Run("catchup", func(t *testing.T) {
		// Record a timestamp before writes so we can use it as the cursor.
		cursorTS := db.Clock().Now()

		// Use different keys so we don't pick up events from the "live" subtest.
		readKey1PC := append(scratchKey.Clone(), 'a')
		writeKey1PC := append(scratchKey.Clone(), 'b')
		readKey2PC := append(scratchKey.Clone(), 'f')
		writeKey2PCLocal := append(scratchKey.Clone(), 'g')
		writeKey2PCRemote := append(midKey.Clone(), 'h')

		txn1PC := run1PCTxn(t, ctx, db, readKey1PC, writeKey1PC)
		txn2PC := run2PCTxn(t, ctx, db, readKey2PC, writeKey2PCLocal, writeKey2PCRemote)
		afterWriteTS := db.Clock().Now()

		// Register the feed AFTER writes with a cursor before the writes. The
		// catch-up scan should find both the 1PC and 2PC committed records in
		// MVCC history.
		stream, disconnector := registerTxnFeed(t, store, rangeID, feedSpan, cursorTS)
		defer disconnector.Disconnect(nil)

		awaitAndVerifyTxnFeedEvents(t, stream, feedSpan, afterWriteTS, txn1PC, txn2PC)
	})
}

// awaitAndVerifyTxnFeedEvents waits for both 1PC and 2PC committed events to
// appear on the stream, then verifies anchor keys, write spans, and read spans.
func awaitAndVerifyTxnFeedEvents(
	t *testing.T,
	stream *txnFeedStream,
	feedSpan roachpb.Span,
	afterWriteTS hlc.Timestamp,
	txn1PC, txn2PC txnResult,
) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		ids := stream.committedTxnIDs()
		_, has1PC := ids[txn1PC.txnID]
		_, has2PC := ids[txn2PC.txnID]
		if has1PC && has2PC {
			return nil
		}
		if ts := stream.maxCheckpointTS(); afterWriteTS.Less(ts) || afterWriteTS.Equal(ts) {
			if !has1PC {
				t.Fatalf("checkpoint at %s >= afterWriteTS %s but missing 1PC txn %s",
					ts, afterWriteTS, txn1PC.txnID)
			}
			if !has2PC {
				t.Fatalf("checkpoint at %s >= afterWriteTS %s but missing 2PC txn %s",
					ts, afterWriteTS, txn2PC.txnID)
			}
		}
		return &missingTxnFeedEventsError{has1PC: has1PC, has2PC: has2PC}
	})

	for _, ev := range stream.events() {
		if ev.Committed == nil {
			continue
		}
		c := ev.Committed
		switch c.TxnID {
		case txn1PC.txnID:
			verifyCommittedEvent(t, c, feedSpan, txn1PC, "1PC")
		case txn2PC.txnID:
			verifyCommittedEvent(t, c, feedSpan, txn2PC, "2PC")
		}
	}
}

// verifyCommittedEvent checks that a committed event has correct anchor key,
// write spans, and read spans. Read spans must contain all read keys and must
// not exceed the feed span.
func verifyCommittedEvent(
	t *testing.T, c *kvpb.TxnFeedCommitted, feedSpan roachpb.Span, result txnResult, label string,
) {
	t.Helper()
	require.True(t, feedSpan.ContainsKey(c.AnchorKey),
		"%s anchor key %s outside feed span %s", label, c.AnchorKey, feedSpan)
	require.NotEmpty(t, c.WriteSpans, "%s txn should have write spans", label)
	require.NotEmpty(t, c.ReadSpans, "%s txn should have read spans", label)

	// Verify each read key is contained within the reported read spans.
	for _, readKey := range result.readKeys {
		contained := false
		for _, rs := range c.ReadSpans {
			if rs.Contains(roachpb.Span{Key: readKey}) {
				contained = true
				break
			}
		}
		require.True(t, contained,
			"%s read key %s not contained in any read span %v", label, readKey, c.ReadSpans)
	}

	// Verify read spans don't exceed the feed span (which represents the range
	// boundary for the first range).
	for _, rs := range c.ReadSpans {
		require.True(t, feedSpan.ContainsKey(rs.Key),
			"%s read span start %s outside feed span %s", label, rs.Key, feedSpan)
		if len(rs.EndKey) > 0 {
			// EndKey is exclusive, so it can equal feedSpan.EndKey.
			require.True(t, rs.EndKey.Compare(feedSpan.EndKey) <= 0,
				"%s read span end %s exceeds feed span end %s", label, rs.EndKey, feedSpan.EndKey)
		}
	}
}

// TestGetTxnDetailsWriteSet verifies that GetTxnDetails returns the correct
// write set for a committed transaction, including previous values.
func TestGetTxnDetailsWriteSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)

	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	ts := tc.Server(0)
	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	db := ts.DB()

	scratchKey := tc.ScratchRange(t)
	feedSpan := roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	}
	rangeID := store.LookupReplica(roachpb.RKey(scratchKey)).RangeID

	// Pre-populate keys that the transaction will overwrite and delete.
	overwriteKey := append(scratchKey.Clone(), 'b')
	deleteKey := append(scratchKey.Clone(), 'c')
	require.NoError(t, db.Put(ctx, overwriteKey, "old-value"))
	require.NoError(t, db.Put(ctx, deleteKey, "doomed-value"))

	// Register a txn feed so we can capture the commit event.
	startTS := db.Clock().Now()
	stream, disconnector := registerTxnFeed(t, store, rangeID, feedSpan, startTS)
	defer disconnector.Disconnect(nil)

	// Run a transaction that creates, overwrites, and deletes keys.
	newKey := append(scratchKey.Clone(), 'a')
	var txnID uuid.UUID
	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txnID = txn.ID()
		if err := txn.Put(ctx, newKey, "new-value"); err != nil {
			return err
		}
		if err := txn.Put(ctx, overwriteKey, "updated-value"); err != nil {
			return err
		}
		_, err := txn.Del(ctx, deleteKey)
		return err
	})
	require.NoError(t, err)

	// Wait for the committed event to appear on the feed.
	var committed *kvpb.TxnFeedCommitted
	testutils.SucceedsSoon(t, func() error {
		for _, ev := range stream.events() {
			if ev.Committed != nil && ev.Committed.TxnID == txnID {
				committed = ev.Committed
				return nil
			}
		}
		return errors.New("waiting for committed event")
	})

	// Send GetTxnDetailsRequest using the write spans from the feed event.
	desc := store.LookupReplica(roachpb.RKey(scratchKey)).Desc()
	resp, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(),
		&kvpb.GetTxnDetailsRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    desc.StartKey.AsRawKey(),
				EndKey: desc.EndKey.AsRawKey(),
			},
			TxnID:           committed.TxnID,
			CommitTimestamp: committed.CommitTimestamp,
			WriteSpans:      committed.WriteSpans,
		})
	require.Nil(t, pErr)
	details := resp.(*kvpb.GetTxnDetailsResponse)

	verifyWriteSet(t, details.Writes, []expectedWrite{
		{key: newKey, value: "new-value"},
		{key: overwriteKey, value: "updated-value", prevVal: "old-value"},
		{key: deleteKey, prevVal: "doomed-value"},
	})
}

// TestGetTxnDetailsMultiRange verifies that GetTxnDetails correctly collects
// writes from a transaction that spans multiple ranges.
func TestGetTxnDetailsMultiRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)

	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	ts := tc.Server(0)
	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	db := ts.DB()

	// Create two ranges: [scratchKey, midKey) and [midKey, ...).
	scratchKey := tc.ScratchRange(t)
	midKey := append(scratchKey.Clone(), 'm')
	tc.SplitRangeOrFatal(t, midKey)

	// Register a txn feed on the first range to capture the commit event.
	feedSpan := roachpb.Span{Key: scratchKey, EndKey: midKey}
	rangeID := store.LookupReplica(roachpb.RKey(scratchKey)).RangeID
	startTS := db.Clock().Now()
	stream, disconnector := registerTxnFeed(t, store, rangeID, feedSpan, startTS)
	defer disconnector.Disconnect(nil)

	// Pre-populate a key in the second range to test prev_value across ranges.
	key2Old := append(midKey.Clone(), 'b')
	require.NoError(t, db.Put(ctx, key2Old, "range2-old"))

	// Run a 2PC transaction writing one key per range.
	key1 := append(scratchKey.Clone(), 'a')
	key2 := key2Old
	var txnID uuid.UUID
	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txnID = txn.ID()
		if err := txn.Put(ctx, key1, "range1-value"); err != nil {
			return err
		}
		return txn.Put(ctx, key2, "range2-value")
	})
	require.NoError(t, err)

	// Wait for the committed event.
	var committed *kvpb.TxnFeedCommitted
	testutils.SucceedsSoon(t, func() error {
		for _, ev := range stream.events() {
			if ev.Committed != nil && ev.Committed.TxnID == txnID {
				committed = ev.Committed
				return nil
			}
		}
		return errors.New("waiting for committed event")
	})
	require.NotEmpty(t, committed.WriteSpans, "expected write spans")

	// Send GetTxnDetailsRequest spanning both ranges. The DistSender
	// should split this across the two ranges and combine the results.
	resp, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(),
		&kvpb.GetTxnDetailsRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    scratchKey,
				EndKey: scratchKey.PrefixEnd(),
			},
			TxnID:           committed.TxnID,
			CommitTimestamp: committed.CommitTimestamp,
			WriteSpans:      committed.WriteSpans,
		})
	require.Nil(t, pErr)
	details := resp.(*kvpb.GetTxnDetailsResponse)

	verifyWriteSet(t, details.Writes, []expectedWrite{
		{key: key1, value: "range1-value"},
		{key: key2, value: "range2-value", prevVal: "range2-old"},
	})
}

// expectedWrite describes the expected state of a single key in a
// GetTxnDetailsResponse.
type expectedWrite struct {
	key     roachpb.Key
	value   string // empty = tombstone
	prevVal string // empty = no previous value
}

// verifyWriteSet asserts that the response contains exactly the expected
// writes, matched by key.
func verifyWriteSet(t *testing.T, writes []kvpb.TxnDetailKV, expected []expectedWrite) {
	t.Helper()
	require.Len(t, writes, len(expected), "wrong number of writes")

	byKey := make(map[string]kvpb.TxnDetailKV, len(writes))
	for _, w := range writes {
		byKey[string(w.KeyValue.Key)] = w
	}

	for _, exp := range expected {
		w, ok := byKey[string(exp.key)]
		require.True(t, ok, "missing write for key %s", exp.key)

		if exp.value == "" {
			require.Len(t, w.KeyValue.Value.RawBytes, 0,
				"key %s: expected tombstone", exp.key)
		} else {
			v, err := w.KeyValue.Value.GetBytes()
			require.NoError(t, err)
			require.Equal(t, exp.value, string(v),
				"key %s: wrong value", exp.key)
		}

		if exp.prevVal == "" {
			require.False(t, w.PrevValue.IsPresent(),
				"key %s: expected no prev_value", exp.key)
		} else {
			require.True(t, w.PrevValue.IsPresent(),
				"key %s: expected prev_value", exp.key)
			pv, err := w.PrevValue.GetBytes()
			require.NoError(t, err)
			require.Equal(t, exp.prevVal, string(pv),
				"key %s: wrong prev_value", exp.key)
		}
	}
}

// TestGetTxnDetailsDependencies verifies the full dependency tracking
// pipeline: CommitIndex population during Raft apply, followed by
// GetTxnDetails resolving read dependencies via the CommitIndex.
//
// The test uses two ranges and six transactions:
//
//   - Txn A (1PC): writes keyA on range 1. No dependencies.
//   - Txn B (2PC): reads keyA on range 1, writes keyB1 on range 1 and
//     keyB2 on range 2. Depends on txn A.
//   - Txn C (1PC): reads keyB1 on range 1, writes keyC on range 1.
//     Depends on txn B.
//   - Txn D (2PC): reads keyC on range 1 and keyB2 on range 2, writes
//     keyD1 on range 1 and keyD2 on range 2. Depends on txn C
//     (discovered on range 1) and txn B (discovered on range 2).
//   - Txn E (2PC): writes keyE1 on range 1 and keyE2 on range 2.
//     No dependencies. Txn record on range 1; intent on range 2
//     resolved asynchronously.
//   - Txn F (1PC): reads keyE2 on range 2, writes keyF on range 1.
//     Depends on txn E. The dependency is only discoverable on range 2
//     via the asynchronously resolved intent.
//
// This exercises 1PC and 2PC CommitIndex population, cross-range
// GetTxnDetails via DistSender, dependency chaining, multi-range
// dependency merging, and async intent resolution.
func TestGetTxnDetailsDependencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)

	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	ts := tc.Server(0)
	store, err := ts.GetStores().(*kvserver.Stores).GetStore(ts.GetFirstStoreID())
	require.NoError(t, err)
	db := ts.DB()

	// Two ranges: [scratchKey, midKey) and [midKey, ...).
	scratchKey := tc.ScratchRange(t)
	midKey := append(scratchKey.Clone(), 'm')
	tc.SplitRangeOrFatal(t, midKey)

	keyA := append(scratchKey.Clone(), 'a')
	keyB1 := append(scratchKey.Clone(), 'b')
	keyB2 := append(midKey.Clone(), 'b')
	keyC := append(scratchKey.Clone(), 'c')
	keyD1 := append(scratchKey.Clone(), 'd')
	keyD2 := append(midKey.Clone(), 'd')
	keyE1 := append(scratchKey.Clone(), 'e')
	keyE2 := append(midKey.Clone(), 'e')
	keyF := append(scratchKey.Clone(), 'f')

	// waitForCommittedEvent polls the stream until the given txnID appears.
	waitForCommittedEvent := func(
		t *testing.T, stream *txnFeedStream, txnID uuid.UUID,
	) *kvpb.TxnFeedCommitted {
		t.Helper()
		var committed *kvpb.TxnFeedCommitted
		testutils.SucceedsSoon(t, func() error {
			for _, ev := range stream.events() {
				if ev.Committed != nil && ev.Committed.TxnID == txnID {
					committed = ev.Committed
					return nil
				}
			}
			return errors.Newf("waiting for committed event for %s", txnID.Short())
		})
		return committed
	}

	fmtSpans := func(spans []roachpb.Span) string {
		var buf strings.Builder
		for i, s := range spans {
			if i > 0 {
				buf.WriteString(", ")
			}
			if len(s.EndKey) == 0 {
				fmt.Fprintf(&buf, "{%s}", s.Key)
			} else {
				fmt.Fprintf(&buf, "{%s-%s}", s.Key, s.EndKey)
			}
		}
		return buf.String()
	}

	// getTxnDetails issues a GetTxnDetails request spanning the full scratch
	// keyspace so DistSender fans it out across both ranges.
	getTxnDetails := func(
		t *testing.T, committed *kvpb.TxnFeedCommitted,
	) *kvpb.GetTxnDetailsResponse {
		t.Helper()
		resp, pErr := kv.SendWrapped(ctx, db.NonTransactionalSender(),
			&kvpb.GetTxnDetailsRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    scratchKey,
					EndKey: scratchKey.PrefixEnd(),
				},
				TxnID:            committed.TxnID,
				CommitTimestamp:  committed.CommitTimestamp,
				WriteSpans:       committed.WriteSpans,
				ReadSpans:        committed.ReadSpans,
				DependencyCutoff: hlc.Timestamp{WallTime: 1},
			})
		require.Nil(t, pErr)
		return resp.(*kvpb.GetTxnDetailsResponse)
	}

	logCommitted := func(t *testing.T, name string, c *kvpb.TxnFeedCommitted) {
		t.Helper()
		t.Logf("%s committed: id=%s ts=%s", name, c.TxnID.Short(), c.CommitTimestamp)
		t.Logf("  write_spans: [%s]", fmtSpans(c.WriteSpans))
		t.Logf("  read_spans:  [%s]", fmtSpans(c.ReadSpans))
	}

	logDetails := func(t *testing.T, name string, d *kvpb.GetTxnDetailsResponse) {
		t.Helper()
		t.Logf("%s details: %d writes, %d deps, event_horizon=%s",
			name, len(d.Writes), len(d.Dependencies), d.EventHorizon)
		for _, w := range d.Writes {
			t.Logf("  write: %s", w.KeyValue.Key)
		}
		for _, dep := range d.Dependencies {
			t.Logf("  depends on: %s", dep.Short())
		}
	}

	// Register a txn feed on range 1 before any txns so we capture all
	// commit events.
	feedSpan := roachpb.Span{Key: scratchKey, EndKey: midKey}
	rangeID := store.LookupReplica(roachpb.RKey(scratchKey)).RangeID
	startTS := db.Clock().Now()
	stream, disconnector := registerTxnFeed(t, store, rangeID, feedSpan, startTS)
	defer disconnector.Disconnect(nil)

	// --- Txn A: 1PC write to range 1 ---
	var txnAID uuid.UUID
	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txnAID = txn.ID()
		b := txn.NewBatch()
		b.Put(keyA, "from-txn-a")
		return txn.CommitInBatch(ctx, b)
	})
	require.NoError(t, err)

	committedA := waitForCommittedEvent(t, stream, txnAID)
	logCommitted(t, "txn A (1PC)", committedA)

	// --- Txn B: 2PC, reads keyA (range 1), writes keyB1 (range 1) + keyB2 (range 2) ---
	txnB := kv.NewTxn(ctx, db, 0)
	txnBID := txnB.ID()
	_, err = txnB.Get(ctx, keyA)
	require.NoError(t, err)
	require.NoError(t, txnB.Put(ctx, keyB1, "from-txn-b"))
	require.NoError(t, txnB.Put(ctx, keyB2, "from-txn-b-range2"))
	require.NoError(t, txnB.Commit(ctx))

	committedB := waitForCommittedEvent(t, stream, txnBID)
	logCommitted(t, "txn B (2PC)", committedB)
	require.NotEmpty(t, committedB.WriteSpans)
	require.NotEmpty(t, committedB.ReadSpans)

	// --- Txn C: 1PC, reads keyB1 (range 1), writes keyC (range 1) ---
	var txnCID uuid.UUID
	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txnCID = txn.ID()
		if _, err := txn.Get(ctx, keyB1); err != nil {
			return err
		}
		b := txn.NewBatch()
		b.Put(keyC, "from-txn-c")
		return txn.CommitInBatch(ctx, b)
	})
	require.NoError(t, err)
	committedC := waitForCommittedEvent(t, stream, txnCID)
	logCommitted(t, "txn C (1PC)", committedC)

	// --- Verify txn A has no dependencies ---
	detailsA := getTxnDetails(t, committedA)
	logDetails(t, "txn A", detailsA)
	require.Empty(t, detailsA.Dependencies, "txn A should have no dependencies")
	verifyWriteSet(t, detailsA.Writes, []expectedWrite{
		{key: keyA, value: "from-txn-a"},
	})

	// --- Verify txn B depends on txn A ---
	detailsB := getTxnDetails(t, committedB)
	logDetails(t, "txn B", detailsB)
	require.Contains(t, detailsB.Dependencies, txnAID,
		"txn B should depend on txn A; got deps=%v", detailsB.Dependencies)
	verifyWriteSet(t, detailsB.Writes, []expectedWrite{
		{key: keyB1, value: "from-txn-b"},
		{key: keyB2, value: "from-txn-b-range2"},
	})

	// --- Verify txn C depends on txn B ---
	detailsC := getTxnDetails(t, committedC)
	logDetails(t, "txn C", detailsC)
	require.Contains(t, detailsC.Dependencies, txnBID,
		"txn C should depend on txn B; got deps=%v", detailsC.Dependencies)
	verifyWriteSet(t, detailsC.Writes, []expectedWrite{
		{key: keyC, value: "from-txn-c"},
	})

	// --- Txn D: 2PC, reads keyC (range 1) + keyB2 (range 2) ---
	// Txn D's dependencies are discovered on different ranges: the
	// dependency on txn C comes from range 1 (keyC) and the dependency
	// on txn B comes from range 2 (keyB2). GetTxnDetails fans out via
	// DistSender, and the response.combine() merges the two dependency
	// sets.
	txnD := kv.NewTxn(ctx, db, 0)
	txnDID := txnD.ID()
	_, err = txnD.Get(ctx, keyC)
	require.NoError(t, err)
	_, err = txnD.Get(ctx, keyB2)
	require.NoError(t, err)
	require.NoError(t, txnD.Put(ctx, keyD1, "from-txn-d"))
	require.NoError(t, txnD.Put(ctx, keyD2, "from-txn-d-range2"))
	require.NoError(t, txnD.Commit(ctx))

	committedD := waitForCommittedEvent(t, stream, txnDID)
	logCommitted(t, "txn D (2PC)", committedD)
	require.NotEmpty(t, committedD.ReadSpans)

	// --- Verify txn D depends on both txn B and txn C ---
	detailsD := getTxnDetails(t, committedD)
	logDetails(t, "txn D", detailsD)
	require.Len(t, detailsD.Dependencies, 2,
		"txn D should depend on txn B and txn C; got deps=%v", detailsD.Dependencies)
	require.Contains(t, detailsD.Dependencies, txnCID,
		"txn D should depend on txn C (via keyC on range 1)")
	require.Contains(t, detailsD.Dependencies, txnBID,
		"txn D should depend on txn B (via keyB2 on range 2)")
	verifyWriteSet(t, detailsD.Writes, []expectedWrite{
		{key: keyD1, value: "from-txn-d"},
		{key: keyD2, value: "from-txn-d-range2"},
	})

	// --- Txn E: 2PC, writes keyE1 (range 1) + keyE2 (range 2) ---
	// Txn E's record lives on range 1 (first write is keyE1). The
	// intent on keyE2 (range 2) is resolved asynchronously after
	// EndTxn completes on range 1.
	txnE := kv.NewTxn(ctx, db, 0)
	txnEID := txnE.ID()
	require.NoError(t, txnE.Put(ctx, keyE1, "from-txn-e"))
	require.NoError(t, txnE.Put(ctx, keyE2, "from-txn-e-range2"))
	require.NoError(t, txnE.Commit(ctx))

	committedE := waitForCommittedEvent(t, stream, txnEID)
	logCommitted(t, "txn E (2PC)", committedE)

	// --- Txn F: reads keyE2 (range 2), writes keyF (range 1) ---
	// Txn F's only read is on range 2. The dependency on txn E is
	// exclusively discoverable via range 2's CommitIndex. We do NOT
	// wait for the CommitIndex to be populated: GetTxnDetails uses
	// MVCCIncrementalIterIntentPolicyError, so if txn E's intent on
	// keyE2 hasn't been resolved yet, the concurrency manager pushes
	// and resolves it — which fires MVCCCommitIntentOp and populates
	// the CommitIndex — before evaluation proceeds.
	var txnFID uuid.UUID
	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txnFID = txn.ID()
		if _, err := txn.Get(ctx, keyE2); err != nil {
			return err
		}
		b := txn.NewBatch()
		b.Put(keyF, "from-txn-f")
		return txn.CommitInBatch(ctx, b)
	})
	require.NoError(t, err)

	committedF := waitForCommittedEvent(t, stream, txnFID)
	logCommitted(t, "txn F (1PC)", committedF)

	// --- Verify txn F depends on txn E ---
	detailsF := getTxnDetails(t, committedF)
	logDetails(t, "txn F", detailsF)
	require.Contains(t, detailsF.Dependencies, txnEID,
		"txn F should depend on txn E (via keyE2 on range 2, async-resolved)")
	verifyWriteSet(t, detailsF.Writes, []expectedWrite{
		{key: keyF, value: "from-txn-f"},
	})
}

// TestGetTxnDetailsResolvesIntents verifies that GetTxnDetails correctly
// handles unresolved intents in its read spans. It disables async intent
// resolution so that a 2PC transaction's intent on range 2 remains
// unresolved after commit. GetTxnDetails must push and resolve the
// intent (populating the CommitIndex via MVCCCommitIntentOp) and then
// report the writer as a dependency.
func TestGetTxnDetailsResolvesIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
						DisableAsyncIntentResolution: true,
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	srv := tc.Server(0)
	db := srv.DB()

	// Two ranges: [scratchKey, midKey) and [midKey, ...).
	scratchKey := tc.ScratchRange(t)
	midKey := append(scratchKey.Clone(), 'm')
	tc.SplitRangeOrFatal(t, midKey)

	// Txn W: 2PC write to both ranges. keyW1 is in range 1, keyW2 in
	// range 2. With async intent resolution disabled, the intent on
	// keyW2 remains unresolved after commit.
	keyW1 := scratchKey.Next()
	keyW2 := midKey.Next()

	txnW := kv.NewTxn(ctx, db, 0)
	txnWID := txnW.ID()
	require.NoError(t, txnW.Put(ctx, keyW1, "from-txn-w"))
	require.NoError(t, txnW.Put(ctx, keyW2, "from-txn-w-range2"))
	require.NoError(t, txnW.Commit(ctx))
	t.Logf("txn W committed: %s", txnWID.Short())

	// Send GetTxnDetails with ReadSpans covering keyW2. The request
	// encounters the unresolved intent, pushes and resolves it, then
	// finds txn W in the CommitIndex as a dependency. Use verbose
	// tracing to confirm the resolve actually happened.
	readSpan := roachpb.Span{Key: keyW2, EndKey: keyW2.Next()}
	commitTS := db.Clock().Now()

	traceCtx, sp := tracing.EnsureChildSpan(ctx, srv.Tracer(),
		t.Name(), tracing.WithRecording(tracingpb.RecordingVerbose))
	defer sp.Finish()

	resp, pErr := kv.SendWrapped(traceCtx, db.NonTransactionalSender(),
		&kvpb.GetTxnDetailsRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    scratchKey,
				EndKey: scratchKey.PrefixEnd(),
			},
			TxnID:            uuid.MakeV4(),
			CommitTimestamp:  commitTS,
			ReadSpans:        []roachpb.Span{readSpan},
			DependencyCutoff: hlc.Timestamp{WallTime: 1},
		})
	require.Nil(t, pErr, "GetTxnDetails should resolve intents and succeed")
	details := resp.(*kvpb.GetTxnDetailsResponse)

	rec := sp.GetConfiguredRecording()
	msg, found := rec.FindLogMessage("resolving intent")
	require.True(t, found, "trace should contain intent resolution;\n%s", rec)
	t.Logf("trace confirmed intent resolution: %s", msg)

	require.Contains(t, details.Dependencies, txnWID,
		"expected dependency on txn W after intent resolution")
	t.Logf("found dependency on txn W (%s)", txnWID.Short())
}

type missingTxnFeedEventsError struct {
	has1PC, has2PC bool
}

func (e *missingTxnFeedEventsError) Error() string {
	switch {
	case !e.has1PC && !e.has2PC:
		return "waiting for 1PC and 2PC committed events"
	case !e.has1PC:
		return "waiting for 1PC committed event"
	default:
		return "waiting for 2PC committed event"
	}
}
