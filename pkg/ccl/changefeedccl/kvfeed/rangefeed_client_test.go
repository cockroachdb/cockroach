// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvfeed

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/timers"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// noopWriter is a kvevent.Writer that drops everything. The SST test below
// only cares about runRangeFeed's terminating error, not the events it
// produces, so the sink can be inert.
type noopWriter struct{}

func (noopWriter) Add(context.Context, kvevent.Event) error     { return nil }
func (noopWriter) Drain(context.Context) error                  { return nil }
func (noopWriter) CloseWithReason(context.Context, error) error { return nil }

var _ kvevent.Writer = noopWriter{}

// TestRunRangeFeedSSTReturnsExplicitError verifies the explicit OnSSTable
// handler installed by runRangeFeed: an SST ingestion into a span with an
// active changefeed rangefeed should surface as a permanent error from
// runRangeFeed, not be silently absorbed.
//
// Without this handler, the rangefeed.Factory would treat the SST event as a
// transient processing error (it returns an AssertionFailedf when no handler
// is registered), retry the rangefeed, and re-emit the SST KVs as regular
// RangeFeedValue events via a catchup scan. The changefeed would then absorb
// the ingestion as if it were an ordinary set of writes, masking what is
// supposed to be an unexpected event.
func TestRunRangeFeedSSTReturnsExplicitError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	srv, _, db := serverutils.StartServer(t, base.TestServerArgs{Settings: settings})
	defer srv.Stopper().Stop(ctx)
	tsrv := srv.ApplicationLayer()

	scratchKey := append(tsrv.Codec().TenantPrefix(), keys.ScratchRangeMin...)
	_, _, err := srv.SplitRange(scratchKey)
	require.NoError(t, err)
	scratchKey = scratchKey[:len(scratchKey):len(scratchKey)]
	mkKey := func(k string) roachpb.Key {
		return encoding.EncodeStringAscending(scratchKey, k)
	}
	_, _, err = srv.SplitRange(mkKey("a"))
	require.NoError(t, err)

	for _, l := range []serverutils.ApplicationLayerInterface{tsrv, srv.SystemLayer()} {
		kvserver.RangefeedEnabled.Override(ctx, &l.ClusterSettings().SV, true)
	}

	factory, err := rangefeed.NewFactory(tsrv.AppStopper(), db, tsrv.ClusterSettings(), nil)
	require.NoError(t, err)

	span := roachpb.Span{Key: mkKey("c"), EndKey: mkKey("e")}
	startAfter := db.Clock().Now()

	// Use ShouldSkipCheckpoint as a "rangefeed established" signal: it fires
	// from the OnCheckpoint callback, which only runs after the server has
	// acknowledged the rangefeed and started delivering events.
	checkpointed := make(chan struct{})
	var once sync.Once
	f := &kvFeed{factory: factory}
	cfg := rangeFeedConfig{
		Spans:  []kvcoord.SpanTimePair{{Span: span, StartAfter: startAfter}},
		Timers: timers.NoopScopedTimers,
		Knobs: TestingKnobs{
			ShouldSkipCheckpoint: func(*kvpb.RangeFeedCheckpoint) bool {
				once.Do(func() { close(checkpointed) })
				return false
			},
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- f.runRangeFeed(ctx, noopWriter{}, cfg)
	}()

	// Wait for the rangefeed to be established before ingesting the SST;
	// otherwise the SST might land before the server registers our
	// rangefeed and we'd miss the event entirely.
	select {
	case <-checkpointed:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for initial checkpoint")
	}

	// Ingest an SST that overlaps the watched span.
	now := db.Clock().Now()
	now.Logical = 0
	ts := int(now.WallTime)
	sstKVs := storageutils.KVs{
		storageutils.PointKV(string(mkKey("c")), ts, "1"),
		storageutils.PointKV(string(mkKey("d")), ts, "2"),
	}
	sst, sstStart, sstEnd := storageutils.MakeSST(t, tsrv.ClusterSettings(), sstKVs)
	_, _, _, pErr := db.AddSSTableAtBatchTimestamp(ctx, sstStart, sstEnd, sst,
		false /* disallowConflicts */, hlc.Timestamp{},
		nil /* stats */, false /* ingestAsWrites */, now)
	require.Nil(t, pErr)

	// runRangeFeed should terminate with our explicit assertion error.
	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorContains(t, err, "unexpected SST ingestion")
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for runRangeFeed to terminate after SST ingestion")
	}
}
