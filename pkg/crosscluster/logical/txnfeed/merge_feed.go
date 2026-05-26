// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// MergeFeed merges multiple ordered Subscriptions into a single
// globally-ordered stream. Each input subscription must emit events in MVCC
// timestamp order (e.g. via producer-side MVCC ordering). The MergeFeed uses a min-heap to
// merge them.
//
// KVs are accumulated into a scratch buffer and flushed as a single batch when
// the batch reaches targetBatchKVs and a transaction boundary is reached (the
// next KV has a different timestamp or a checkpoint/end-of-stream is
// encountered). This guarantees that a transaction is never split across
// batches.
//
// The merge feed only emits events with timestamp strictly less than endTime.
// After encountering the first event at or beyond endTime, it flushes any
// buffered KVs and emits a synthetic checkpoint at endTime.Prev(). For all
// remaining events, it continues normal heap processing but only ever emits
// the synthetic checkpoint (deduplicated by maybeEmitCheckpoint) until every
// subscription is exhausted and the loop exits naturally.
type MergeFeed struct {
	subs           []streamclient.Subscription
	events         chan crosscluster.Event
	targetBatchKVs int
	endTime        hlc.Timestamp

	// loop holds state owned exclusively by mergeLoop while it is running.
	// After Subscribe returns, err may be read via Err().
	loop struct {
		err          error
		coveringSpan roachpb.Span
		resolvedTime hlc.Timestamp
		scratch      []streampb.StreamEvent_KV
	}
}

func (m *MergeFeed) Events() <-chan crosscluster.Event {
	return m.events
}

func (m *MergeFeed) Err() error {
	return m.loop.err
}

var _ streamclient.Subscription = (*MergeFeed)(nil)

// NewMergeFeed creates a MergeFeed that merges events from the given
// subscriptions. The coveringSpan is used in emitted checkpoint events.
// targetBatchKVs controls the target number of KVs accumulated before
// flushing a batch (actual batches may be larger to avoid splitting a
// transaction).
func NewMergeFeed(
	subs []streamclient.Subscription,
	coveringSpan roachpb.Span,
	targetBatchKVs int,
	endTime hlc.Timestamp,
) *MergeFeed {
	m := &MergeFeed{
		subs:           subs,
		events:         make(chan crosscluster.Event),
		targetBatchKVs: targetBatchKVs,
		endTime:        endTime,
	}
	m.loop.coveringSpan = coveringSpan
	return m
}

// Subscribe starts all input subscriptions concurrently and runs the merge
// loop. Returns when all goroutines finish.
func (m *MergeFeed) Subscribe(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)
	for _, sub := range m.subs {
		group.GoCtx(func(ctx context.Context) error {
			return sub.Subscribe(ctx)
		})
	}
	group.GoCtx(func(ctx context.Context) error {
		defer close(m.events)
		return m.mergeLoop(ctx)
	})
	err := group.Wait()
	for _, sub := range m.subs {
		err = errors.CombineErrors(err, sub.Err())
	}
	m.loop.err = err
	return err
}

func (m *MergeFeed) mergeLoop(ctx context.Context) error {
	h, err := m.initHeap(ctx)
	if err != nil {
		return err
	}

	for h.len() != 0 {
		entry := h.pop()

		if m.endTime.LessEq(entry.peekTS) {
			if err := m.processPastEndTime(ctx, entry, h); err != nil {
				return err
			}
			continue
		}

		if !entry.isKV() {
			if err := m.flushScratch(ctx); err != nil {
				return err
			}
			cp := entry.peekedCheckpoint
			if err := m.maybeEmitCheckpoint(ctx, cp.ResolvedSpans[0].Timestamp); err != nil {
				return err
			}
			closed, err := entry.advanceFromChannel(ctx)
			if err != nil {
				return err
			}
			if !closed {
				h.push(entry)
			}
			continue
		}

		m.loop.scratch = append(m.loop.scratch, entry.currentKV())
		closed, err := entry.advanceKV(ctx)
		if err != nil {
			return err
		}
		if !closed {
			h.push(entry)
		}

		// Flush if we've reached the target batch size and we're at a
		// transaction boundary (the next item has a different timestamp or is
		// not a KV).
		if m.targetBatchKVs <= len(m.loop.scratch) {
			lastTS := m.loop.scratch[len(m.loop.scratch)-1].KeyValue.Value.Timestamp
			if !h.peekIsKVAt(lastTS) {
				if err := m.flushScratch(ctx); err != nil {
					return err
				}
			}
		}
	}

	// NOTE: The LLM's really like to point out the fact that we are not flushing
	// here. But one of the requirements for the input subscriptions is that the
	// final event is a checkpoint event which will trigger a flush when it is
	// processed.

	return nil
}

// flushScratch emits the accumulated scratch buffer as a single KV batch event
// and resets the buffer. If the scratch buffer is empty, it is a no-op.
func (m *MergeFeed) flushScratch(ctx context.Context) error {
	if len(m.loop.scratch) == 0 {
		return nil
	}
	batch := slices.Clone(m.loop.scratch)
	m.loop.scratch = m.loop.scratch[:0]
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.events <- crosscluster.MakeKVEvent(batch):
	}
	return nil
}

// initHeap reads the first event from each subscription to initialize the
// merge heap.
func (m *MergeFeed) initHeap(ctx context.Context) (*mergeHeap, error) {
	h := &mergeHeap{}
	h.init()

	for _, sub := range m.subs {
		entry := &mergeEntry{orderedSub: sub}
		exhausted, err := entry.advanceFromChannel(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read first event from subscription")
		}
		if !exhausted {
			h.push(entry)
		}
	}

	return h, nil
}

// processPastEndTime drains an entry whose timestamp is at or beyond endTime.
// It flushes any buffered KVs, emits a synthetic checkpoint at endTime.Prev(),
// and advances the entry without processing it.
func (m *MergeFeed) processPastEndTime(ctx context.Context, entry *mergeEntry, h *mergeHeap) error {
	if err := m.flushScratch(ctx); err != nil {
		return err
	}
	if err := m.emitCheckpoint(ctx, m.endTime.Prev()); err != nil {
		return err
	}
	var closed bool
	var err error
	if entry.isKV() {
		closed, err = entry.advanceKV(ctx)
	} else {
		closed, err = entry.advanceFromChannel(ctx)
	}
	if err != nil {
		return err
	}
	if !closed {
		h.push(entry)
	}
	return nil
}

// maybeEmitCheckpoint emits a merged checkpoint if ts advances the global
// frontier.
func (m *MergeFeed) maybeEmitCheckpoint(ctx context.Context, ts hlc.Timestamp) error {
	if ts.LessEq(m.loop.resolvedTime) {
		return nil
	}
	return m.emitCheckpoint(ctx, ts)
}

// emitCheckpoint unconditionally emits a checkpoint event at ts and updates
// the resolved timestamp.
func (m *MergeFeed) emitCheckpoint(ctx context.Context, ts hlc.Timestamp) error {
	m.loop.resolvedTime = ts

	cpEvent := crosscluster.MakeCheckpointEvent(
		&streampb.StreamEvent_StreamCheckpoint{
			ResolvedSpans: []jobspb.ResolvedSpan{
				{
					Span:      m.loop.coveringSpan,
					Timestamp: m.loop.resolvedTime,
				},
			},
		})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.events <- cpEvent:
	}
	return nil
}
