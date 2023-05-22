// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

type testOrderedSink struct {
	t *testing.T
	orderedSink
	pool testAllocPool
}

func (s *testOrderedSink) emitTs(wallTime int64) {
	require.NoError(s.t, s.EmitRow(
		context.Background(),
		nil,
		[]byte("[1001]"), []byte("{\"after\":{\"col1\":\"val1\",\"rowid\":1000},\"topic:\":\"foo\"}"),
		zeroTS,
		hlc.Timestamp{WallTime: wallTime},
		s.pool.alloc()),
	)
}

func (s *testOrderedSink) popPayload() jobspb.OrderedRows {
	row := s.forwardingBuf.Pop()
	require.True(s.t, row[0].IsNull())
	require.True(s.t, row[1].IsNull())
	require.True(s.t, row[2].IsNull())
	require.True(s.t, row[3].IsNull())
	require.False(s.t, row[4].IsNull())
	raw, ok := row[4].Datum.(*tree.DBytes)
	require.True(s.t, ok)

	var payload jobspb.OrderedRows
	require.NoError(s.t, protoutil.Unmarshal([]byte(*raw), &payload))

	return payload
}

func (s *testOrderedSink) flushToTs(wallTime int64) {
	_, err := s.frontier.Forward(makeSpan(s.t, "a", "f"), hlc.Timestamp{WallTime: wallTime})
	require.NoError(s.t, err)
	require.NoError(s.t, s.EmitUpToResolved(context.Background()))
}

func (s *testOrderedSink) flushAndVerify(wallTime int64) int {
	s.flushToTs(wallTime)

	if s.forwardingBuf.IsEmpty() {
		return 0
	}
	payload := s.popPayload()
	lastTs := hlc.Timestamp{}
	for _, orderedRow := range payload.Rows {
		tdebug(fmt.Sprintf("GOT %d", orderedRow.Mvcc.WallTime))
		require.False(s.t, orderedRow.Mvcc.Less(lastTs))
		lastTs = orderedRow.Mvcc
	}

	return len(payload.Rows)
}

func TestOrderedSink(t *testing.T) {
	sf, err := makeSchemaChangeFrontier(hlc.Timestamp{}, makeSpan(t, "a", "f"))
	require.NoError(t, err)

	sink := testOrderedSink{
		orderedSink: orderedSink{
			processorID: 42,
			metrics:     MakeMetrics(base.DefaultHistogramWindowInterval()).(*Metrics),
			frontier:    sf,
		}, t: t}

	sink.emitTs(1)
	sink.emitTs(3)
	sink.emitTs(200)
	sink.emitTs(2)
	sink.emitTs(8)
	sink.emitTs(5)
	sink.emitTs(190)

	require.Equal(t, sink.flushAndVerify(100), 5)
	require.Equal(t, sink.flushAndVerify(200), 2)
	require.Equal(t, sink.flushAndVerify(300), 0)

	for ts := 1000; ts > 500; ts-- {
		sink.emitTs(int64(ts))
	}
	require.Equal(t, sink.flushAndVerify(1001), 500)
}

func makeSpan(t *testing.T, start string, end string) (s roachpb.Span) {
	mkKey := func(k string) roachpb.Key {
		vDatum := tree.DString(k)
		key, err := keyside.Encode(keys.SystemSQLCodec.TablePrefix(42), &vDatum, encoding.Ascending)
		require.NoError(t, err)
		return key
	}
	s.Key = mkKey(start)
	s.EndKey = mkKey(end)
	return s
}

type mockSink struct {
	t        *testing.T
	frontier *schemaChangeFrontier
	emits    int
	lastMvcc hlc.Timestamp
}

func (ms *mockSink) Flush(ctx context.Context) error {
	return nil
}
func (ms *mockSink) getConcreteType() sinkType {
	return sinkTypeNull
}
func (ms *mockSink) Close() error {
	return nil
}
func (ms *mockSink) Dial() error {
	return nil
}
func (ms *mockSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	require.True(ms.t, mvcc.LessEq(ms.frontier.Frontier()) || mvcc.Equal(ms.frontier.BackfillTS()))
	require.False(ms.t, mvcc.Less(ms.lastMvcc), fmt.Sprintf("%s not less than %s", mvcc, ms.lastMvcc))
	ms.lastMvcc = mvcc
	ms.emits += 1
	return nil
}

var _ EventSink = (*mockSink)(nil)

func TestOrderedMerger(t *testing.T) {
	sf, err := makeSchemaChangeFrontier(hlc.Timestamp{}, makeSpan(t, "a", "f"))
	require.NoError(t, err)

	rng, _ := randutil.NewTestRand()

	orderedSinks := make([]testOrderedSink, 5)
	for i := 0; i < 5; i++ {
		orderedSinks[i] = testOrderedSink{
			orderedSink: orderedSink{
				processorID: int32(i),
				frontier:    sf,
				metrics:     MakeMetrics(base.DefaultHistogramWindowInterval()).(*Metrics),
			}, t: t}

		for j := 0; j < 500; j++ {
			ts := rng.Int63n(4999) + 1
			orderedSinks[i].emitTs(ts)
		}
		for j := 500; j < 1000; j++ {
			ts := rng.Int63n(4999) + 5001
			orderedSinks[i].emitTs(ts)
		}
	}

	sink := &mockSink{t: t, lastMvcc: hlc.Timestamp{}, frontier: sf}
	merger := &orderedRowMerger{
		orderedRows: make(map[int32][]jobspb.OrderedRows_Row),
		frontier:    sf,
		metrics:     MakeMetrics(base.DefaultHistogramWindowInterval()).(*Metrics),
		sink:        sink,
	}

	forwardFrontier := func(start string, end string, ts int64) {
		_, err := sf.Forward(makeSpan(t, start, end), hlc.Timestamp{WallTime: ts})
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			require.NoError(t, orderedSinks[i].EmitUpToResolved(context.Background()))
			for !orderedSinks[i].forwardingBuf.IsEmpty() {
				merger.Append(orderedSinks[i].popPayload())
			}
		}
	}

	require.NoError(t, merger.TryFlush(context.Background()))
	require.Equal(t, sink.emits, 0)

	forwardFrontier("a", "f", 2000)
	require.NoError(t, merger.TryFlush(context.Background()))
	prevEmits := sink.emits

	// Sholud not emit anything if only part of the frontier advanced
	forwardFrontier("a", "b", 10000)
	require.NoError(t, merger.TryFlush(context.Background()))
	require.Equal(t, sink.emits, prevEmits)

	// Sholud not emit anything if only part of the frontier advanced
	forwardFrontier("c", "f", 10000)
	require.NoError(t, merger.TryFlush(context.Background()))
	require.Equal(t, sink.emits, prevEmits)

	// Should finally be able to emit
	forwardFrontier("b", "c", 2500)
	require.NoError(t, merger.TryFlush(context.Background()))
	forwardFrontier("b", "c", 5000)
	require.NoError(t, merger.TryFlush(context.Background()))
	require.Greater(t, sink.emits, prevEmits)

	require.NoError(t, merger.TryFlush(context.Background()))
	forwardFrontier("a", "f", 10000)
	require.NoError(t, merger.TryFlush(context.Background()))
	require.Zero(t, merger.minHeap.Len())
	require.Equal(t, sink.emits, 5000)

	// Should handle a backfill where events are arriving at .Prev of the walltime
	_, err = sf.ForwardResolvedSpan(jobspb.ResolvedSpan{
		Span:         makeSpan(t, "a", "f"),
		Timestamp:    hlc.Timestamp{WallTime: 20000}.Prev(),
		BoundaryType: jobspb.ResolvedSpan_BACKFILL,
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		for j := 0; j < 100; j++ {
			orderedSinks[i].emitTs(20000)
		}
		orderedSinks[i].emitTs(20100)

		require.NoError(t, orderedSinks[i].EmitUpToResolved(context.Background()))
		for !orderedSinks[i].forwardingBuf.IsEmpty() {
			merger.Append(orderedSinks[i].popPayload())
		}
	}
	prevEmits = sink.emits
	require.NoError(t, merger.TryFlush(context.Background()))
	require.Equal(t, sink.emits, prevEmits+500)
}
