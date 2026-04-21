// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
