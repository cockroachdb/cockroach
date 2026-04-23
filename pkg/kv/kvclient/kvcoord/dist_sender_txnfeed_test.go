// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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

// txnFeedTestCluster wraps common setup for txnfeed tests.
type txnFeedTestCluster struct {
	tc         *testcluster.TestCluster
	db         *kv.DB
	ds         *kvcoord.DistSender
	scratchKey roachpb.Key
}

// setupTxnFeedTest creates a test cluster with txnfeed enabled.
// The caller must defer stop() to shut down the cluster — this must
// be deferred AFTER leaktest.AfterTest so the cluster stops before
// the goroutine leak check runs.
func setupTxnFeedTest(
	t *testing.T, ctx context.Context, numNodes int,
) (c *txnFeedTestCluster, stop func()) {
	t.Helper()
	settings := cluster.MakeTestingClusterSettings()
	txnfeed.Enabled.Override(ctx, &settings.SV, true)

	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings:          settings,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})

	_, err := tc.ServerConn(0).Exec(
		"SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'")
	require.NoError(t, err)

	return &txnFeedTestCluster{
		tc:         tc,
		db:         tc.Server(0).DB(),
		ds:         tc.Server(0).DistSenderI().(*kvcoord.DistSender),
		scratchKey: tc.ScratchRange(t),
	}, func() { tc.Stopper().Stop(ctx) }
}

type committedTxn struct {
	id    uuid.UUID
	label string
}

// runWorkload runs concurrent random transactions for the given
// duration. Returns the list of committed transaction IDs.
func runWorkload(
	t *testing.T,
	ctx context.Context,
	db *kv.DB,
	scratchKey roachpb.Key,
	numWorkers int,
	dur time.Duration,
) []committedTxn {
	t.Helper()
	const keyAlphabet = "abcdefghijklmnopqrstuvwxyz"

	var (
		mu   sync.Mutex
		txns []committedTxn
	)

	randomKey := func(rng *rand.Rand) roachpb.Key {
		k := scratchKey.Clone()
		k = append(k, keyAlphabet[rng.Intn(len(keyAlphabet))])
		k = append(k, byte('0'+rng.Intn(10)))
		return k
	}

	var wg sync.WaitGroup
	workloadCtx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(workerID)))
			for i := 0; workloadCtx.Err() == nil; i++ {
				label := fmt.Sprintf("w%d-t%d", workerID, i)
				if rng.Intn(2) == 0 {
					key := randomKey(rng)
					var id uuid.UUID
					err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						id = txn.ID()
						return txn.Put(ctx, key, label)
					})
					if err != nil {
						continue
					}
					mu.Lock()
					txns = append(txns, committedTxn{id: id, label: label})
					mu.Unlock()
				} else {
					key1 := randomKey(rng)
					key2 := randomKey(rng)
					for key1.Equal(key2) {
						key2 = randomKey(rng)
					}
					txn := kv.NewTxn(ctx, db, 0)
					id := txn.ID()
					if err := txn.Put(ctx, key1, label+"-a"); err != nil {
						continue
					}
					if err := txn.Put(ctx, key2, label+"-b"); err != nil {
						continue
					}
					if err := txn.Commit(ctx); err != nil {
						continue
					}
					mu.Lock()
					txns = append(txns, committedTxn{id: id, label: label})
					mu.Unlock()
				}
			}
		}(w)
	}

	wg.Wait()
	return txns
}

// startFeedAndDrain starts a TxnFeed and continuously drains events.
// Returns functions to get collected events and to cancel the feed.
func startFeedAndDrain(
	t *testing.T,
	ctx context.Context,
	ds *kvcoord.DistSender,
	span roachpb.Span,
	startAfter hlc.Timestamp,
) (getEvents func() []kvcoord.TxnFeedMessage, cancel func(), feedErrCh <-chan error) {
	t.Helper()
	eventCh := make(chan kvcoord.TxnFeedMessage, 1024)
	errCh := make(chan error, 1)
	feedCtx, feedCancel := context.WithCancel(ctx)

	go func() {
		errCh <- ds.TxnFeed(feedCtx, []kvcoord.SpanTimePair{
			{Span: span, StartAfter: startAfter},
		}, eventCh)
	}()

	var (
		mu     sync.Mutex
		events []kvcoord.TxnFeedMessage
	)
	go func() {
		for {
			select {
			case msg := <-eventCh:
				mu.Lock()
				events = append(events, msg)
				mu.Unlock()
			case <-feedCtx.Done():
				return
			}
		}
	}()

	return func() []kvcoord.TxnFeedMessage {
		mu.Lock()
		defer mu.Unlock()
		cp := make([]kvcoord.TxnFeedMessage, len(events))
		copy(cp, events)
		return cp
	}, feedCancel, errCh
}

// waitForTxns waits until all expected transactions appear in the feed.
func waitForTxns(
	t *testing.T,
	txns []committedTxn,
	getEvents func() []kvcoord.TxnFeedMessage,
	feedErrCh <-chan error,
) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		select {
		case feedErr := <-feedErrCh:
			t.Fatalf("TxnFeed exited with error: %v", feedErr)
		default:
		}
		evs := getEvents()
		seen := make(map[uuid.UUID]struct{})
		for _, ev := range evs {
			if ev.Committed != nil {
				seen[ev.Committed.TxnID] = struct{}{}
			}
		}
		missing := 0
		for _, ct := range txns {
			if _, ok := seen[ct.id]; !ok {
				missing++
			}
		}
		if missing == 0 {
			return nil
		}
		return fmt.Errorf("waiting for %d/%d committed txns (%d events so far)",
			missing, len(txns), len(evs))
	})
}

// validateEvents checks completeness, checkpoint ordering, and event
// validity on the collected events.
func validateEvents(
	t *testing.T,
	txns []committedTxn,
	collectedEvents []kvcoord.TxnFeedMessage,
	feedSpan roachpb.Span,
) {
	t.Helper()

	wantIDs := make(map[uuid.UUID]string)
	for _, ct := range txns {
		wantIDs[ct.id] = ct.label
	}

	committedEvents := make(map[uuid.UUID]*kvpb.TxnFeedCommitted)
	for _, ev := range collectedEvents {
		if ev.Committed != nil {
			committedEvents[ev.Committed.TxnID] = ev.Committed
		}
	}

	// 1. Completeness.
	for id, label := range wantIDs {
		_, ok := committedEvents[id]
		require.True(t, ok, "txn %s (%s) missing from feed", id.Short(), label)
	}
	t.Logf("completeness: all %d txns present", len(wantIDs))

	// 2. Checkpoint ordering: a committed event must not appear
	//    after a checkpoint that covers its anchor span.
	maxResolved := make(map[string]hlc.Timestamp)
	spanByKey := make(map[string]roachpb.Span)
	for _, ev := range collectedEvents {
		if ev.Checkpoint != nil {
			key := ev.Checkpoint.AnchorSpan.String()
			if ev.Checkpoint.ResolvedTS.Less(maxResolved[key]) {
				t.Errorf("checkpoint went backwards for %s: %s < %s",
					key, ev.Checkpoint.ResolvedTS, maxResolved[key])
			}
			maxResolved[key] = ev.Checkpoint.ResolvedTS
			spanByKey[key] = ev.Checkpoint.AnchorSpan
		}
		if ev.Committed != nil {
			anchorKey, err := keys.Addr(ev.Committed.AnchorKey)
			if err != nil {
				continue
			}
			for spanKey, resolved := range maxResolved {
				if !spanByKey[spanKey].ContainsKey(anchorKey.AsRawKey()) {
					continue
				}
				if ev.Committed.CommitTimestamp.Less(resolved) {
					t.Errorf("committed txn %s at %s after checkpoint %s for %s",
						ev.Committed.TxnID.Short(),
						ev.Committed.CommitTimestamp, resolved, spanKey)
				}
			}
		}
	}
	t.Logf("checkpoint ordering: validated across %d events", len(collectedEvents))

	// 3. Event validity.
	for _, c := range committedEvents {
		require.NotEqual(t, uuid.UUID{}, c.TxnID, "zero TxnID")
		require.False(t, c.CommitTimestamp.IsEmpty(), "txn %s: zero commit ts", c.TxnID.Short())
		addrKey, err := keys.Addr(c.AnchorKey)
		require.NoError(t, err, "txn %s: invalid anchor key", c.TxnID.Short())
		require.True(t, feedSpan.ContainsKey(addrKey.AsRawKey()),
			"anchor %s (addr %s) outside %s", c.AnchorKey, addrKey, feedSpan)
	}
	t.Logf("event validity: all %d committed events valid", len(committedEvents))
}

// TestDistSenderTxnFeed_Live runs a concurrent workload with a
// mid-workload range split and validates the live event stream.
func TestDistSenderTxnFeed_Live(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupTxnFeedTest(t, ctx, 1)
	defer stop()
	for _, split := range []byte{'c', 'f', 'i'} {
		c.tc.SplitRangeOrFatal(t, append(c.scratchKey.Clone(), split))
	}

	feedSpan := roachpb.Span{Key: c.scratchKey, EndKey: c.scratchKey.PrefixEnd()}
	startTS := c.db.Clock().Now()

	getEvents, cancelFeed, feedErrCh := startFeedAndDrain(t, ctx, c.ds, feedSpan, startTS)

	// Run workload with a mid-workload split.
	var splitWg sync.WaitGroup
	splitWg.Add(1)
	go func() {
		defer splitWg.Done()
		time.Sleep(time.Second)
		splitKey := append(c.scratchKey.Clone(), 'd', '5')
		t.Logf("splitting range at %s mid-workload", splitKey)
		c.tc.SplitRangeOrFatal(t, splitKey)
	}()

	txns := runWorkload(t, ctx, c.db, c.scratchKey, 8, 3*time.Second)
	splitWg.Wait()
	t.Logf("workload complete: %d txns committed", len(txns))

	waitForTxns(t, txns, getEvents, feedErrCh)
	cancelFeed()
	validateEvents(t, txns, getEvents(), feedSpan)
}

// TestDistSenderTxnFeed_CatchUpScan commits transactions before
// starting the feed with an earlier timestamp, verifying that the
// server's catch-up scan delivers historical events.
func TestDistSenderTxnFeed_CatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	c, stop := setupTxnFeedTest(t, ctx, 1)
	defer stop()
	for _, split := range []byte{'c', 'f', 'i'} {
		c.tc.SplitRangeOrFatal(t, append(c.scratchKey.Clone(), split))
	}

	feedSpan := roachpb.Span{Key: c.scratchKey, EndKey: c.scratchKey.PrefixEnd()}

	// Record a timestamp BEFORE any transactions.
	cursorTS := c.db.Clock().Now()

	// Run a workload — these transactions will be in the past
	// relative to when we start the feed.
	txns := runWorkload(t, ctx, c.db, c.scratchKey, 4, 1*time.Second)
	t.Logf("pre-feed workload: %d txns committed", len(txns))

	// Now start the feed at the old cursor. The server must deliver
	// all committed transactions via catch-up scan.
	getEvents, cancelFeed, feedErrCh := startFeedAndDrain(t, ctx, c.ds, feedSpan, cursorTS)

	waitForTxns(t, txns, getEvents, feedErrCh)
	cancelFeed()
	validateEvents(t, txns, getEvents(), feedSpan)
}
