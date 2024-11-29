// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestStreamEventBatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	seb := makeStreamEventBatcher(true)

	var runningSize int
	kv := roachpb.KeyValue{Key: roachpb.Key{'1'}}
	runningSize += (&streampb.StreamEvent_KV{KeyValue: kv}).Size()
	seb.addKV(streampb.StreamEvent_KV{KeyValue: kv})
	require.Equal(t, 1, len(seb.batch.KVs))
	require.Equal(t, runningSize, seb.getSize())

	delRange := kvpb.RangeFeedDeleteRange{Span: roachpb.Span{Key: roachpb.KeyMin}, Timestamp: hlc.Timestamp{}}
	runningSize += delRange.Size()
	seb.addDelRange(delRange)
	require.Equal(t, 1, len(seb.batch.DelRanges))
	require.Equal(t, runningSize, seb.getSize())

	sst := replicationtestutils.SSTMaker(t, []roachpb.KeyValue{kv})
	runningSize += sst.Size()
	seb.addSST(sst)
	require.Equal(t, 1, len(seb.batch.Ssts))
	require.Equal(t, runningSize, seb.getSize())

	splitKey := roachpb.Key("1")
	runningSize += len(splitKey)
	seb.addSplitPoint(splitKey)
	require.Equal(t, 1, len(seb.batch.SplitPoints))
	require.Equal(t, runningSize, seb.getSize())

	// Reset should clear the batch.
	seb.reset()
	require.Equal(t, 0, seb.getSize())
	require.Equal(t, 0, len(seb.batch.KVs))
	require.Equal(t, 0, len(seb.batch.Ssts))
	require.Equal(t, 0, len(seb.batch.DelRanges))
}

// TestSpanConfigsInStreamEventBatcher ensures that span config events are
// properly added to the stream event batcher.
func TestBatchSpanConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, seed := randutil.NewPseudoRand()
	t.Logf("Replication helper seed %d", seed)

	seb := makeStreamEventBatcher(true)
	codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(2))
	bufferedEvents := make([]streampb.StreamedSpanConfigEntry, 0)

	tableSpan := func(tableID uint32) roachpb.Span {
		syntheticTableSpanPrefix := codec.TablePrefix(tableID)
		return roachpb.Span{Key: syntheticTableSpanPrefix, EndKey: syntheticTableSpanPrefix.PrefixEnd()}
	}

	makeEntry := func(tableSpan roachpb.Span, ttl int, timestamp int64) streampb.StreamedSpanConfigEntry {
		record := replicationtestutils.MakeSpanConfigRecord(t, tableSpan, ttl)
		return streampb.StreamedSpanConfigEntry{
			SpanConfig: roachpb.SpanConfigEntry{
				Target: record.GetTarget().ToProto(),
				Config: record.GetConfig(),
			},
			Timestamp: hlc.Timestamp{WallTime: timestamp},
		}
	}

	events := func(events ...streampb.StreamedSpanConfigEntry) []streampb.StreamedSpanConfigEntry {
		return events
	}

	t1Span := tableSpan(1)
	t2Span := tableSpan(2)

	previousBatchIdx := 0

	for idx, toAdd := range []struct {
		updates          []streampb.StreamedSpanConfigEntry
		frontier         int64
		expected         []streampb.StreamedSpanConfigEntry
		shouldFlushAfter bool
	}{
		{
			// Both spans should buffer
			updates:  events(makeEntry(t1Span, 3, 1), makeEntry(t1Span, 4, 2)),
			frontier: 2,
			expected: events(makeEntry(t1Span, 3, 1), makeEntry(t1Span, 4, 2)),
		},
		{
			// An event at a lower timestamp should not get flushed
			updates:  events(makeEntry(t1Span, 3, 1)),
			frontier: 2,
			expected: []streampb.StreamedSpanConfigEntry{},
		},
		{
			// An empty update should not screw things up.
			updates:  []streampb.StreamedSpanConfigEntry{},
			frontier: 2,
			expected: []streampb.StreamedSpanConfigEntry{},
		},
		{
			// An event at the frontier should not get flushed
			updates:  events(makeEntry(t1Span, 4, 2)),
			frontier: 2,
			expected: []streampb.StreamedSpanConfigEntry{},
		},
		{
			// Only one of the duplicate events should flush
			updates:  events(makeEntry(t1Span, 3, 3), makeEntry(t1Span, 3, 3)),
			frontier: 3,
			expected: events(makeEntry(t1Span, 3, 3)),
		},
		{
			// Only one of the duplicate events should flush, even if there's an event in between the duplicate events.
			updates:  events(makeEntry(t1Span, 3, 4), makeEntry(t2Span, 4, 4), makeEntry(t1Span, 3, 4)),
			frontier: 4,
			expected: events(makeEntry(t1Span, 3, 4), makeEntry(t2Span, 4, 4)),
		},
		// Duplicate events from previous addition get elided.
		{
			updates:  events(makeEntry(t1Span, 3, 4), makeEntry(t2Span, 4, 4), makeEntry(t1Span, 3, 4), makeEntry(t2Span, 3, 5)),
			frontier: 5,
			expected: []streampb.StreamedSpanConfigEntry{makeEntry(t2Span, 3, 5)},
		},
	} {
		bufferedEvents = append(bufferedEvents, toAdd.updates...)
		seb.addSpanConfigs(bufferedEvents, hlc.Timestamp{WallTime: toAdd.frontier})
		require.Equal(t, toAdd.expected, seb.batch.SpanConfigs[previousBatchIdx:], fmt.Sprintf("failed on step %d (indexed by 0)", idx))
		bufferedEvents = bufferedEvents[:0]
		if rng.Intn(2) == 0 {
			// Flush half of the time.
			t.Logf("Flushing batcher on step %d (indexed by 0)", idx)
			seb.reset()
		}
		previousBatchIdx = len(seb.batch.SpanConfigs)
	}
}
