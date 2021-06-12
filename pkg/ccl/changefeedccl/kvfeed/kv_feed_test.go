// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed/schematestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKVFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// We want to inject fake table events and data into the buffer
	// and use that to assert that there are proper calls to the kvScanner and
	// what not.
	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}

	mkKey := func(tableID uint32, k string) roachpb.Key {
		vDatum := tree.DString(k)
		key, err := rowenc.EncodeTableKey(keys.SystemSQLCodec.TablePrefix(tableID), &vDatum, encoding.Ascending)
		require.NoError(t, err)
		return key
	}
	kv := func(tableID uint32, k, v string, ts hlc.Timestamp) roachpb.KeyValue {
		return roachpb.KeyValue{
			Key: mkKey(tableID, k),
			Value: roachpb.Value{
				RawBytes:  []byte(v),
				Timestamp: ts,
			},
		}
	}
	kvEvent := func(tableID uint32, k, v string, ts hlc.Timestamp) roachpb.RangeFeedEvent {
		keyVal := kv(tableID, k, v, ts)
		return roachpb.RangeFeedEvent{
			Val: &roachpb.RangeFeedValue{
				Key:   keyVal.Key,
				Value: keyVal.Value,
			},
			Checkpoint: nil,
			Error:      nil,
		}
	}
	checkpointEvent := func(span roachpb.Span, ts hlc.Timestamp) roachpb.RangeFeedEvent {
		return roachpb.RangeFeedEvent{
			Checkpoint: &roachpb.RangeFeedCheckpoint{
				Span:       span,
				ResolvedTS: ts,
			},
		}
	}
	type testCase struct {
		name               string
		needsInitialScan   bool
		withDiff           bool
		schemaChangeEvents changefeedbase.SchemaChangeEventClass
		schemaChangePolicy changefeedbase.SchemaChangePolicy
		initialHighWater   hlc.Timestamp
		spans              []roachpb.Span
		checkpoint         []roachpb.Span
		events             []roachpb.RangeFeedEvent

		descs []catalog.TableDescriptor

		expScans  []hlc.Timestamp
		expEvents int
		expErrRE  string
	}
	runTest := func(t *testing.T, tc testCase) {
		settings := cluster.MakeTestingClusterSettings()
		buf := kvevent.MakeChanBuffer()
		mm := mon.NewUnlimitedMonitor(
			context.Background(), "test", mon.MemoryResource,
			nil /* curCount */, nil /* maxHist */, math.MaxInt64, settings,
		)
		metrics := kvevent.MakeMetrics(time.Minute)
		bufferFactory := func() kvevent.Buffer {
			return kvevent.NewMemBuffer(mm.MakeBoundAccount(), &metrics)
		}
		scans := make(chan physicalConfig)
		sf := scannerFunc(func(ctx context.Context, sink kvevent.Writer, cfg physicalConfig) error {
			select {
			case scans <- cfg:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		ref := rawEventFeed(tc.events)
		tf := newRawTableFeed(tc.descs, tc.initialHighWater)
		f := newKVFeed(buf, tc.spans, tc.checkpoint,
			tc.schemaChangeEvents, tc.schemaChangePolicy,
			tc.needsInitialScan, tc.withDiff,
			tc.initialHighWater,
			keys.SystemSQLCodec,
			tf, sf, rangefeedFactory(ref.run), bufferFactory, TestingKnobs{})
		ctx, cancel := context.WithCancel(context.Background())
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			return f.run(ctx)
		})
		spansToScan := filterCheckpointSpans(tc.spans, tc.checkpoint)
		testG := ctxgroup.WithContext(ctx)
		testG.GoCtx(func(ctx context.Context) error {
			for expScans := tc.expScans; len(expScans) > 0; expScans = expScans[1:] {
				scan := <-scans
				assert.Equal(t, expScans[0], scan.Timestamp)
				assert.Equal(t, tc.withDiff, scan.WithDiff)
				assert.Equal(t, spansToScan, scan.Spans)
			}
			return nil
		})
		testG.GoCtx(func(ctx context.Context) error {
			for events := 0; events < tc.expEvents; events++ {
				_, err := buf.Get(ctx)
				assert.NoError(t, err)
			}
			return nil
		})
		// Wait for the feed to fail rather than canceling it.
		if tc.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyStop {
			testG.Go(func() error {
				_ = g.Wait()
				return nil
			})
		}
		require.NoError(t, testG.Wait())
		cancel()
		if runErr := g.Wait(); tc.expErrRE != "" {
			require.Regexp(t, tc.expErrRE, runErr)
		} else {
			require.Regexp(t, context.Canceled, runErr)
		}
	}
	makeTableDesc := schematestutils.MakeTableDesc
	addColumnDropBackfillMutation := schematestutils.AddColumnDropBackfillMutation

	makeSpan := func(tableID uint32, start, end string) (s roachpb.Span) {
		s.Key = mkKey(tableID, start)
		s.EndKey = mkKey(tableID, end)
		return s
	}

	for _, tc := range []testCase{
		{
			name:               "no events - backfill",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(42),
			},
			events: []roachpb.RangeFeedEvent{
				kvEvent(42, "a", "b", ts(3)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			expEvents: 1,
		},
		{
			name:               "no events -  full checkpoint",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(42),
			},
			checkpoint: []roachpb.Span{
				tableSpan(42),
			},
			events: []roachpb.RangeFeedEvent{
				kvEvent(42, "a", "b", ts(3)),
			},
			expScans:  []hlc.Timestamp{},
			expEvents: 1,
		},
		{
			name:               "no events - partial backfill",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(42),
			},
			checkpoint: []roachpb.Span{
				makeSpan(42, "a", "q"),
			},
			events: []roachpb.RangeFeedEvent{
				kvEvent(42, "a", "val", ts(3)),
				kvEvent(42, "d", "val", ts(3)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			expEvents: 1,
		},
		{
			name:               "one table event - backfill",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(42),
			},
			events: []roachpb.RangeFeedEvent{
				kvEvent(42, "a", "b", ts(3)),
				checkpointEvent(tableSpan(42), ts(4)),
				kvEvent(42, "a", "b", ts(5)),
				checkpointEvent(tableSpan(42), ts(2)), // ensure that events are filtered
				checkpointEvent(tableSpan(42), ts(5)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
				ts(3),
			},
			descs: []catalog.TableDescriptor{
				makeTableDesc(42, 1, ts(1), 2, 1),
				addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(3), 1, 1)),
			},
			expEvents: 2,
		},
		{
			name:               "one table event - skip",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyNoBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(42),
			},
			events: []roachpb.RangeFeedEvent{
				kvEvent(42, "a", "b", ts(3)),
				checkpointEvent(tableSpan(42), ts(4)),
				kvEvent(42, "a", "b", ts(5)),
				checkpointEvent(tableSpan(42), ts(6)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			descs: []catalog.TableDescriptor{
				makeTableDesc(42, 1, ts(1), 2, 1),
				addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(3), 1, 1)),
			},
			expEvents: 4,
		},
		{
			name:               "one table event - stop",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyStop,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(42),
			},
			events: []roachpb.RangeFeedEvent{
				kvEvent(42, "a", "b", ts(3)),
				checkpointEvent(tableSpan(42), ts(4)),
				kvEvent(42, "a", "b", ts(5)),
				checkpointEvent(tableSpan(42), ts(2)), // ensure that events are filtered
				checkpointEvent(tableSpan(42), ts(5)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			descs: []catalog.TableDescriptor{
				makeTableDesc(42, 1, ts(1), 2, 1),
				addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(4), 1, 1)),
			},
			expEvents: 2,
			expErrRE:  "schema change ...",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runTest(t, tc)
		})
	}
}

type scannerFunc func(ctx context.Context, sink kvevent.Writer, cfg physicalConfig) error

func (s scannerFunc) Scan(ctx context.Context, sink kvevent.Writer, cfg physicalConfig) error {
	return s(ctx, sink, cfg)
}

var _ kvScanner = (scannerFunc)(nil)

type rawTableFeed struct {
	events []schemafeed.TableEvent
}

func newRawTableFeed(
	descs []catalog.TableDescriptor, initialHighWater hlc.Timestamp,
) schemafeed.SchemaFeed {
	sort.Slice(descs, func(i, j int) bool {
		if descs[i].GetID() != descs[j].GetID() {
			return descs[i].GetID() < descs[j].GetID()
		}
		return descs[i].GetModificationTime().Less(descs[j].GetModificationTime())
	})
	f := rawTableFeed{}
	curID := descpb.ID(math.MaxUint32)
	for i, d := range descs {
		if d.GetID() != curID {
			curID = d.GetID()
			continue
		}
		if d.GetModificationTime().Less(initialHighWater) {
			continue
		}
		f.events = append(f.events, schemafeed.TableEvent{
			Before: descs[i-1],
			After:  d,
		})
	}
	return &f
}

func (r *rawTableFeed) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (r *rawTableFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []schemafeed.TableEvent, err error) {
	return r.peekOrPop(ctx, atOrBefore, false /* pop */)
}

func (r *rawTableFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []schemafeed.TableEvent, err error) {
	return r.peekOrPop(ctx, atOrBefore, true /* pop */)
}

func (r *rawTableFeed) peekOrPop(
	ctx context.Context, atOrBefore hlc.Timestamp, pop bool,
) (events []schemafeed.TableEvent, err error) {
	i := sort.Search(len(r.events), func(i int) bool {
		return !r.events[i].Timestamp().LessEq(atOrBefore)
	})
	if i == -1 {
		i = 0
	}
	events = r.events[:i]
	if pop {
		r.events = r.events[i:]
	}
	return events, nil
}

type rawEventFeed []roachpb.RangeFeedEvent

func (f rawEventFeed) run(
	ctx context.Context,
	span roachpb.Span,
	startFrom hlc.Timestamp,
	withDiff bool,
	eventC chan<- *roachpb.RangeFeedEvent,
) error {
	// We can't use binary search because the errors don't have timestamps.
	// Instead we just search for the first event which comes after the start time.
	var i int
	for i = range f {
		ev := f[i]
		if ev.Val != nil && startFrom.Less(ev.Val.Value.Timestamp) ||
			ev.Checkpoint != nil && startFrom.Less(ev.Checkpoint.ResolvedTS) {
			break
		}

	}
	f = f[i:]
	for i := range f {
		select {
		case eventC <- &f[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

var _ schemafeed.SchemaFeed = (*rawTableFeed)(nil)

func tableSpan(tableID uint32) roachpb.Span {
	return roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(tableID),
		EndKey: keys.SystemSQLCodec.TablePrefix(tableID).PrefixEnd(),
	}
}
