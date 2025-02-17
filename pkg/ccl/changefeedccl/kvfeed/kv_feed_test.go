// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvfeed

import (
	"context"
	"math"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed/schematestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/timers"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	kvserverrangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKVFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	codec := keys.SystemSQLCodec

	// We want to inject fake table events and data into the buffer
	// and use that to assert that there are proper calls to the kvScanner and
	// what not.
	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}

	// mkKey returns an encoded key of `/tableID/k`
	mkKey := func(codec keys.SQLCodec, tableID uint32, k string) roachpb.Key {
		vDatum := tree.DString(k)
		key, err := keyside.Encode(codec.TablePrefix(tableID), &vDatum, encoding.Ascending)
		require.NoError(t, err)
		return key
	}

	// kv returns a kv pair (key=/tableID/k, value=v,ts)
	kv := func(codec keys.SQLCodec, tableID uint32, k, v string, ts hlc.Timestamp) roachpb.KeyValue {
		return roachpb.KeyValue{
			Key: mkKey(codec, tableID, k),
			Value: roachpb.Value{
				RawBytes:  []byte(v),
				Timestamp: ts,
			},
		}
	}

	// kvEvent returns a RangeFeedEvent with 'Val = (key=/tableID/k, value=[v,ts])`
	kvEvent := func(codec keys.SQLCodec, tableID uint32, k, v string, ts hlc.Timestamp) kvpb.RangeFeedEvent {
		keyVal := kv(codec, tableID, k, v, ts)
		return kvpb.RangeFeedEvent{
			Val: &kvpb.RangeFeedValue{
				Key:   keyVal.Key,
				Value: keyVal.Value,
			},
			Checkpoint: nil,
			Error:      nil,
		}
	}

	// checkpointEvent returns a RangeFeedEvent with `Checkpoint=(span, ts)`
	checkpointEvent := func(span roachpb.Span, ts hlc.Timestamp) kvpb.RangeFeedEvent {
		return kvpb.RangeFeedEvent{
			Checkpoint: &kvpb.RangeFeedCheckpoint{
				Span:       span,
				ResolvedTS: ts,
			},
		}
	}

	type testCase struct {
		name                 string
		needsInitialScan     bool
		withDiff             bool
		withFrontierQuantize time.Duration
		schemaChangeEvents   changefeedbase.SchemaChangeEventClass
		schemaChangePolicy   changefeedbase.SchemaChangePolicy
		initialHighWater     hlc.Timestamp
		endTime              hlc.Timestamp
		spans                []roachpb.Span
		spanLevelCheckpoint  *jobspb.TimestampSpansMap
		events               []kvpb.RangeFeedEvent

		descs []catalog.TableDescriptor

		expScans       []hlc.Timestamp
		expEvents      []kvpb.RangeFeedEvent
		expEventsCount int
		expErrRE       string
	}
	st := cluster.MakeTestingClusterSettings()
	runTest := func(t *testing.T, tc testCase) {
		settings := cluster.MakeTestingClusterSettings()
		mm := mon.NewUnlimitedMonitor(context.Background(), mon.Options{
			Name:     mon.MakeMonitorName("test"),
			Settings: settings,
		})
		metrics := kvevent.MakeMetrics(time.Minute)
		buf := kvevent.NewMemBuffer(mm.MakeBoundAccount(), &st.SV, &metrics.AggregatorBufferMetricsWithCompat)

		// bufferFactory, when called, gives you a memory-monitored
		// in-memory "buffer" to write to and read from.
		bufferFactory := func() kvevent.Buffer {
			return kvevent.NewMemBuffer(mm.MakeBoundAccount(), &st.SV, &metrics.RangefeedBufferMetricsWithCompat)
		}
		scans := make(chan scanConfig)

		// `sf`, when called, attempts to push `cfg` onto the a channel `scans`.
		sf := scannerFunc(func(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error {
			select {
			case scans <- cfg:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		ref := rawEventFeed(tc.events)
		tf := newRawTableFeed(tc.descs, tc.initialHighWater)
		st := timers.New(time.Minute).GetOrCreateScopedTimers("")
		f := newKVFeed(buf, tc.spans, tc.spanLevelCheckpoint,
			tc.schemaChangeEvents, tc.schemaChangePolicy,
			tc.needsInitialScan, tc.withDiff, true /* withFiltering */, tc.withFrontierQuantize,
			0, /* consumerID */
			tc.initialHighWater, tc.endTime,
			codec,
			tf, sf, rangefeedFactory(ref.run), bufferFactory,
			changefeedbase.Targets{},
			st, TestingKnobs{})
		ctx, cancel := context.WithCancel(context.Background())
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			return f.run(ctx)
		})

		// Assert that each scanConfig pushed to the channel `scans` by `f.run()`
		// is what we expected (as specified in the test case).
		spansToScan := filterCheckpointSpans(tc.spans, tc.spanLevelCheckpoint)
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

		// Assert that number of events emitted from the kvfeed matches what we
		// specified in the testcase.
		testG.GoCtx(func(ctx context.Context) error {
			for eventIdx := 0; eventIdx < tc.expEventsCount; eventIdx++ {
				e, err := buf.Get(ctx)
				assert.NoError(t, err)
				if tc.expEvents != nil {
					assert.Equal(t, tc.expEvents[eventIdx], *e.Raw())
				}
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

		// Wait for all goroutines in `testG` (tc.ExpScans check and tc.ExpEvents check)
		// to finish, and then cancel the kvfeed (i.e. `f.run()`).
		// If the test case has OPTION SCHEMA_CHANGE_POLICY='stop', then testG has one
		// additional goroutine that waits for the finish of the kvfeed.
		require.NoError(t, testG.Wait())
		cancel()

		// Finally, assert that kvfeed is either cancelled, or is terminated with the
		// expected error.
		if runErr := g.Wait(); tc.expErrRE != "" {
			require.Regexp(t, tc.expErrRE, runErr)
		} else {
			require.Regexp(t, context.Canceled, runErr)
		}
	}
	makeTableDesc := schematestutils.MakeTableDesc
	addColumnDropBackfillMutation := schematestutils.AddColumnDropBackfillMutation

	// makeSpan returns a span (start=/tableID/start, end=/tableID/end)
	makeSpan := func(codec keys.SQLCodec, tableID uint32, start, end string) (s roachpb.Span) {
		s.Key = mkKey(codec, tableID, start)
		s.EndKey = mkKey(codec, tableID, end)
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
				tableSpan(codec, 42),
			},
			events: []kvpb.RangeFeedEvent{
				kvEvent(codec, 42, "a", "b", ts(3)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			expEventsCount: 1,
		},
		{
			name:               "no events -  full checkpoint",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts(2).Next(): {tableSpan(codec, 42)},
			}),
			events: []kvpb.RangeFeedEvent{
				kvEvent(codec, 42, "a", "b", ts(3)),
			},
			expScans:       []hlc.Timestamp{},
			expEventsCount: 1,
		},
		{
			name:               "no events - partial backfill",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			spanLevelCheckpoint: jobspb.NewTimestampSpansMap(map[hlc.Timestamp]roachpb.Spans{
				ts(2).Next(): {makeSpan(codec, 42, "a", "q")},
			}),
			events: []kvpb.RangeFeedEvent{
				kvEvent(codec, 42, "a", "val", ts(3)),
				kvEvent(codec, 42, "d", "val", ts(3)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			expEventsCount: 2,
		},
		{
			name:               "one table event - backfill",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			events: []kvpb.RangeFeedEvent{
				kvEvent(codec, 42, "a", "b", ts(3)),
				checkpointEvent(tableSpan(codec, 42), ts(4)),
				kvEvent(codec, 42, "a", "b", ts(5)),
				checkpointEvent(tableSpan(codec, 42), ts(2)), // ensure that events are filtered
				checkpointEvent(tableSpan(codec, 42), ts(5)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
				ts(3),
			},
			descs: []catalog.TableDescriptor{
				makeTableDesc(42, 1, ts(1), 2, 1),
				addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(3), 1, 1)),
			},
			expEventsCount: 5,
		},
		{
			name:               "one table event - skip",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyNoBackfill,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			events: []kvpb.RangeFeedEvent{
				kvEvent(codec, 42, "a", "b", ts(3).Next()),
				checkpointEvent(tableSpan(codec, 42), ts(4)),
				kvEvent(codec, 42, "a", "b", ts(5)),
				checkpointEvent(tableSpan(codec, 42), ts(6)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			descs: []catalog.TableDescriptor{
				makeTableDesc(42, 1, ts(1), 2, 1),
				addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(3), 1, 1)),
			},
			expEventsCount: 4,
		},
		{
			name:               "one table event - stop",
			schemaChangeEvents: changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy: changefeedbase.OptSchemaChangePolicyStop,
			needsInitialScan:   true,
			initialHighWater:   ts(2),
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			events: []kvpb.RangeFeedEvent{
				kvEvent(codec, 42, "a", "b", ts(3)),
				checkpointEvent(tableSpan(codec, 42), ts(4)),
				kvEvent(codec, 42, "a", "b", ts(5)),
				checkpointEvent(tableSpan(codec, 42), ts(2)), // ensure that events are filtered
				checkpointEvent(tableSpan(codec, 42), ts(5)),
			},
			expScans: []hlc.Timestamp{
				ts(2),
			},
			descs: []catalog.TableDescriptor{
				makeTableDesc(42, 1, ts(1), 2, 1),
				addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(4), 1, 1)),
			},
			expEventsCount: 2,
			expErrRE:       "schema change ...",
		},
		{
			name:                 "checkpoint events - with quantize",
			schemaChangeEvents:   changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy:   changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:     false,
			withFrontierQuantize: time.Duration(10),
			initialHighWater:     hlc.Timestamp{WallTime: 10},
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			events: []kvpb.RangeFeedEvent{
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20, Logical: 1}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 27, Logical: 1}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 43, Logical: 3}),
			},
			expEvents: []kvpb.RangeFeedEvent{
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20, Logical: 0}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20, Logical: 0}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 40, Logical: 0}),
			},
			expEventsCount: 4,
		},
		{
			name:                 "checkpoint events - without quantize",
			schemaChangeEvents:   changefeedbase.OptSchemaChangeEventClassDefault,
			schemaChangePolicy:   changefeedbase.OptSchemaChangePolicyBackfill,
			needsInitialScan:     false,
			withFrontierQuantize: time.Duration(0),
			initialHighWater:     hlc.Timestamp{WallTime: 10},
			spans: []roachpb.Span{
				tableSpan(codec, 42),
			},
			events: []kvpb.RangeFeedEvent{
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20, Logical: 1}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 27, Logical: 1}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 43, Logical: 3}),
			},
			expEvents: []kvpb.RangeFeedEvent{
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 20, Logical: 1}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 27, Logical: 1}),
				checkpointEvent(tableSpan(codec, 42), hlc.Timestamp{WallTime: 43, Logical: 3}),
			},
			expEventsCount: 4,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runTest(t, tc)
		})
	}
}

type scannerFunc func(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error

func (s scannerFunc) Scan(ctx context.Context, sink kvevent.Writer, cfg scanConfig) error {
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

type rawEventFeed []kvpb.RangeFeedEvent

func (f rawEventFeed) run(
	ctx context.Context,
	spans []kvcoord.SpanTimePair,
	eventC chan<- kvcoord.RangeFeedMessage,
	opts ...kvcoord.RangeFeedOption,
) error {
	var startAfter hlc.Timestamp
	for _, s := range spans {
		if startAfter.IsEmpty() || s.StartAfter.Less(startAfter) {
			startAfter = s.StartAfter
		}
	}

	// We can't use binary search because the errors don't have timestamps.
	// Instead we just search for the first event which comes after the start time.
	var i int
	for i = range f {
		ev := f[i]
		if ev.Val != nil && startAfter.LessEq(ev.Val.Value.Timestamp) ||
			ev.Checkpoint != nil && startAfter.LessEq(ev.Checkpoint.ResolvedTS) {
			break
		}
	}
	f = f[i:]
	for i := range f {
		select {
		case eventC <- kvcoord.RangeFeedMessage{RangeFeedEvent: &f[i]}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

var _ schemafeed.SchemaFeed = (*rawTableFeed)(nil)

// tableSpan returns a span that covers all keys under this tableID.
func tableSpan(codec keys.SQLCodec, tableID uint32) roachpb.Span {
	return roachpb.Span{
		Key:    codec.TablePrefix(tableID),
		EndKey: codec.TablePrefix(tableID).PrefixEnd(),
	}
}

// testKVEventWriter is a mock kvevent.Writer that appends to a slice of events.
type testKVEventWriter struct {
	events []kvevent.Event
}

func (w *testKVEventWriter) Add(ctx context.Context, event kvevent.Event) error {
	w.events = append(w.events, event)
	return nil
}

func (w *testKVEventWriter) Drain(ctx context.Context) error {
	return nil
}

func (w *testKVEventWriter) CloseWithReason(ctx context.Context, reason error) error {
	return nil
}

var _ kvevent.Writer = (*testKVEventWriter)(nil)

// testKVEventReader is a mock kvevent.Reader that pops and returns events
// from a queue of events.
type testKVEventReader struct {
	events []kvevent.Event
}

func (r *testKVEventReader) Get(ctx context.Context) (kvevent.Event, error) {
	if len(r.events) == 0 {
		return kvevent.Event{}, errors.New("out of events")
	}
	ev := r.events[0]
	r.events = r.events[1:]
	return ev, nil
}

var _ kvevent.Reader = (*testKVEventReader)(nil)

// testSchemaFeed is a mock SchemaFeed that operates on a slice of
// sorted table events.
type testSchemaFeed struct {
	tableEvents []schemafeed.TableEvent
}

func (t *testSchemaFeed) Run(ctx context.Context) error {
	return nil
}

func (t *testSchemaFeed) Peek(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []schemafeed.TableEvent, err error) {
	return t.peekOrPop(ctx, atOrBefore, false /* pop */)
}

func (t *testSchemaFeed) Pop(
	ctx context.Context, atOrBefore hlc.Timestamp,
) (events []schemafeed.TableEvent, err error) {
	return t.peekOrPop(ctx, atOrBefore, true /* pop */)
}

func (t *testSchemaFeed) peekOrPop(
	ctx context.Context, atOrBefore hlc.Timestamp, pop bool,
) (events []schemafeed.TableEvent, err error) {
	i, _ := slices.BinarySearchFunc(t.tableEvents, atOrBefore, func(event schemafeed.TableEvent, timestamp hlc.Timestamp) int {
		if event.Timestamp().LessEq(timestamp) {
			return -1
		} else {
			return 1
		}
	})
	events = t.tableEvents[:i]
	if pop {
		t.tableEvents = t.tableEvents[i:]
	}
	return events, nil
}

var _ schemafeed.SchemaFeed = (*testSchemaFeed)(nil)

// testTableDesc is a mock for catalog.TableDescriptor that only contains a
// modification time. It is used in lieu of a real table descriptor in
// test schemafeed.TableEvent structs.
type testTableDesc struct {
	catalog.TableDescriptor
	modTime hlc.Timestamp
}

func (d *testTableDesc) GetModificationTime() hlc.Timestamp {
	return d.modTime
}

func TestCopyFromSourceToDestUntilTableEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ts := func(ts int) hlc.Timestamp { return hlc.Timestamp{WallTime: int64(ts)} }
	makeSpan := func(key, endKey []byte) roachpb.Span { return roachpb.Span{Key: key, EndKey: endKey} }
	makeKVEvent := func(key, val []byte, ts hlc.Timestamp) kvevent.Event {
		return kvevent.NewBackfillKVEvent(key, ts, val, false /* withDiff */, ts)
	}
	makeResolvedEvent := func(span roachpb.Span, ts hlc.Timestamp) kvevent.Event {
		return kvevent.NewBackfillResolvedEvent(span, ts, jobspb.ResolvedSpan_NONE)
	}
	makeTableEvent := func(modTime hlc.Timestamp) schemafeed.TableEvent {
		return schemafeed.TableEvent{After: &testTableDesc{modTime: modTime}}
	}

	for name, tc := range map[string]struct {
		spans            []roachpb.Span
		events           []kvevent.Event
		endTime          hlc.Timestamp
		tableEvents      []schemafeed.TableEvent
		expectedErr      error
		expectedEvents   []kvevent.Event
		expectedFrontier hlc.Timestamp
	}{
		"end time reached": {
			spans: []roachpb.Span{makeSpan([]byte("a"), []byte("z"))},
			events: []kvevent.Event{
				makeKVEvent([]byte("a"), []byte("a_val"), ts(2)),
				makeResolvedEvent(makeSpan([]byte("a"), []byte("b")), ts(5)),
				makeKVEvent([]byte("b"), []byte("b_val"), ts(7)),
				makeResolvedEvent(makeSpan([]byte("b"), []byte("z")), ts(10)),
				makeResolvedEvent(makeSpan([]byte("a"), []byte("b")), ts(10)),
			},
			endTime:     ts(9),
			expectedErr: &errEndTimeReached{endTime: ts(9)},
			expectedEvents: []kvevent.Event{
				makeKVEvent([]byte("a"), []byte("a_val"), ts(2)),
				makeResolvedEvent(makeSpan([]byte("a"), []byte("b")), ts(5)),
				makeKVEvent([]byte("b"), []byte("b_val"), ts(7)),
				makeResolvedEvent(makeSpan([]byte("b"), []byte("z")), ts(9).Prev()),
			},
			expectedFrontier: ts(9).Prev(),
		},
		"table event reached": {
			spans: []roachpb.Span{makeSpan([]byte("a"), []byte("z"))},
			events: []kvevent.Event{
				makeKVEvent([]byte("a"), []byte("a_val"), ts(2)),
				makeResolvedEvent(makeSpan([]byte("a"), []byte("b")), ts(5)),
				makeKVEvent([]byte("b"), []byte("b_val"), ts(7)),
				makeResolvedEvent(makeSpan([]byte("b"), []byte("z")), ts(10)),
				makeResolvedEvent(makeSpan([]byte("a"), []byte("b")), ts(10)),
			},
			endTime: ts(9),
			tableEvents: []schemafeed.TableEvent{
				makeTableEvent(ts(8)),
			},
			expectedErr: &errTableEventReached{makeTableEvent(ts(8))},
			expectedEvents: []kvevent.Event{
				makeKVEvent([]byte("a"), []byte("a_val"), ts(2)),
				makeResolvedEvent(makeSpan([]byte("a"), []byte("b")), ts(5)),
				makeKVEvent([]byte("b"), []byte("b_val"), ts(7)),
			},
			expectedFrontier: ts(8).Prev(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			dest := &testKVEventWriter{}
			src := &testKVEventReader{events: tc.events}
			frontier, err := span.MakeFrontier(tc.spans...)
			require.NoError(t, err)
			schemaFeed := &testSchemaFeed{tableEvents: tc.tableEvents}
			endTime := tc.endTime

			err = copyFromSourceToDestUntilTableEvent(ctx, dest, src, frontier, schemaFeed, endTime, TestingKnobs{}, timers.New(1*time.Second).GetOrCreateScopedTimers(""))
			require.Equal(t, tc.expectedErr, err)
			require.Empty(t, src.events)
			require.Equal(t, tc.expectedEvents, dest.events)
			require.Equal(t, tc.expectedFrontier, frontier.Frontier())
		})
	}
}

func makeCheckpointEvent(key []byte, endKey []byte, ts int, logical int32) *kvpb.RangeFeedEvent {
	return &kvpb.RangeFeedEvent{
		Checkpoint: &kvpb.RangeFeedCheckpoint{
			Span:       roachpb.Span{Key: key, EndKey: endKey},
			ResolvedTS: hlc.Timestamp{WallTime: int64(ts), Logical: logical},
		},
	}
}

// TestFrontierQuantization tests that the frontier quantization works as
// expected. It should quantize timestamps and merge adjacent spans with same
// timestamp together.
func TestFrontierQuantization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for name, tc := range map[string]struct {
		spans                    []roachpb.Span
		withFrontierQuantization bool
		events                   []*kvpb.RangeFeedEvent
		expectedFrontierEntries  int
		expectedFrontier         hlc.Timestamp
	}{
		"overlapping spans with close ts with quantization": {
			withFrontierQuantization: true,
			spans:                    []roachpb.Span{{Key: []byte("a"), EndKey: []byte("d")}},
			events: []*kvpb.RangeFeedEvent{
				makeCheckpointEvent([]byte("a"), []byte("b"), 12, 2),
				makeCheckpointEvent([]byte("b"), []byte("c"), 13, 1),
				makeCheckpointEvent([]byte("c"), []byte("d"), 14, 0),
			},
			expectedFrontierEntries: 1,
			expectedFrontier:        hlc.Timestamp{WallTime: int64(10)},
		},
		"overlapping spans with close ts without quantization": {
			withFrontierQuantization: false,
			spans:                    []roachpb.Span{{Key: []byte("a"), EndKey: []byte("d")}},
			events: []*kvpb.RangeFeedEvent{
				makeCheckpointEvent([]byte("a"), []byte("b"), 12, 0),
				makeCheckpointEvent([]byte("b"), []byte("c"), 13, 0),
				makeCheckpointEvent([]byte("c"), []byte("d"), 14, 0),
			},
			expectedFrontierEntries: 3,
			expectedFrontier:        hlc.Timestamp{WallTime: int64(12)},
		},
		"non-overlapping spans with close ts with quantization": {
			withFrontierQuantization: true,
			spans:                    []roachpb.Span{{Key: []byte("a"), EndKey: []byte("z")}},
			events: []*kvpb.RangeFeedEvent{
				makeCheckpointEvent([]byte("a"), []byte("b"), 12, 3),
				makeCheckpointEvent([]byte("b"), []byte("c"), 13, 2),
				makeCheckpointEvent([]byte("c"), []byte("d"), 14, 1),
				makeCheckpointEvent([]byte("k"), []byte("m"), 12, 0),
			},
			expectedFrontierEntries: 4,
			expectedFrontier:        hlc.Timestamp{WallTime: int64(0)},
		},
		"non-overlapping spans with close ts without quantization": {
			withFrontierQuantization: false,
			spans:                    []roachpb.Span{{Key: []byte("a"), EndKey: []byte("z")}},
			events: []*kvpb.RangeFeedEvent{
				makeCheckpointEvent([]byte("a"), []byte("b"), 12, 3),
				makeCheckpointEvent([]byte("b"), []byte("c"), 13, 2),
				makeCheckpointEvent([]byte("c"), []byte("d"), 14, 1),
				makeCheckpointEvent([]byte("k"), []byte("m"), 12, 0),
			},
			expectedFrontierEntries: 6,
			expectedFrontier:        hlc.Timestamp{WallTime: int64(0)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			frontier, err := span.MakeFrontier(tc.spans...)
			require.NoError(t, err)
			const quantize = time.Duration(10)
			for _, e := range tc.events {
				if tc.withFrontierQuantization {
					e.Checkpoint.ResolvedTS = quantizeTS(e.Checkpoint.ResolvedTS, quantize)
				}
				_, err := frontier.Forward(e.Checkpoint.Span, e.Checkpoint.ResolvedTS)
				require.NoError(t, err)
			}
			frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) span.OpResult {
				t.Logf("span: %v, ts: %v\n", sp, ts)
				return false
			})
			require.Equal(t, tc.expectedFrontierEntries, frontier.Len())
			require.Equal(t, tc.expectedFrontier, frontier.Frontier())
		})
	}
}

// TestFrontierQuantizationRand makes two frontiers with the same set of spans
// and one with quantized ts and the other without. The test makes sure that the
// highwater tracked by the frontier with quantized ts is <= the other, and the
// number of spans being tracked is >= the other.
func TestFrontierQuantizationRand(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numOfSpans = 100
	events := make([]*kvpb.RangeFeedEvent, numOfSpans)
	rng, _ := randutil.NewTestRand()
	for i, s := range kvserverrangefeed.GenerateRandomizedSpans(rng, numOfSpans) {
		ts := kvserverrangefeed.GenerateRandomizedTs(rng, time.Minute.Nanoseconds())
		if s.EndKey.Equal(s.Key) {
			s.EndKey = s.Key.Next()
		}
		events[i] = makeCheckpointEvent(s.Key, s.EndKey, int(ts.WallTime), int32(rng.Intn(100)))
	}

	const quantize = time.Duration(10)
	quantizedFrontier, err := span.MakeFrontier([]roachpb.Span{{Key: keys.MinKey, EndKey: keys.MaxKey}}...)
	require.NoError(t, err)
	frontier, err := span.MakeFrontier([]roachpb.Span{{Key: keys.MinKey, EndKey: keys.MaxKey}}...)
	require.NoError(t, err)
	for _, e := range events {
		quantizedTs := quantizeTS(e.Checkpoint.ResolvedTS, quantize)
		_, err := quantizedFrontier.Forward(e.Checkpoint.Span, quantizedTs)
		require.NoError(t, err)
		_, err = frontier.Forward(e.Checkpoint.Span, e.Checkpoint.ResolvedTS)
		require.NoError(t, err)
	}
	quantizedEntries := quantizedFrontier.Len()
	quantizedHW := quantizedFrontier.Frontier()
	entries := frontier.Len()
	hw := frontier.Frontier()
	require.LessOrEqual(t, quantizedEntries, entries)
	require.True(t, quantizedHW.LessEq(hw))
}
