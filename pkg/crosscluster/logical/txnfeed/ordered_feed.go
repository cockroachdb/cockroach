// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"slices"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// NewOrderedFeed creates an OrderedFeed that buffers events from
// rawSubscription, sorts them by MVCC timestamp, and emits them in order as
// KVEvents followed by simplified checkpoint events. The coveringSpan is
// computed from the initial frontier entries and is used to emit a single
// resolved span per checkpoint.
func NewOrderedFeed(
	rawSubscription streamclient.Subscription, initialFrontier span.Frontier,
) (*OrderedFeed, error) {
	var coveringSpan roachpb.Span
	for s := range initialFrontier.Entries() {
		if coveringSpan.Key == nil {
			coveringSpan.Key = s.Key.Clone()
			coveringSpan.EndKey = s.EndKey.Clone()
		} else {
			if s.Key.Compare(coveringSpan.Key) < 0 {
				coveringSpan.Key = s.Key.Clone()
			}
			if 0 < s.EndKey.Compare(coveringSpan.EndKey) {
				coveringSpan.EndKey = s.EndKey.Clone()
			}
		}
	}

	return &OrderedFeed{
		rawSubscription: rawSubscription,
		frontier:        initialFrontier,
		readyFrontier:   initialFrontier.Frontier(),
		events:          make(chan crosscluster.Event),
		coveringSpan:    coveringSpan,
	}, nil
}

// OrderedFeed buffers KV events from a raw subscription and emits them in MVCC
// timestamp order.
type OrderedFeed struct {
	rawSubscription streamclient.Subscription
	frontier        span.Frontier
	coveringSpan    roachpb.Span

	events chan crosscluster.Event
	err    error

	// TODO(jeffswenson): this implementation of ordered feed is only suitable
	// for an initial prototype.
	// TODO(jeffswenson): add memory accounting.
	// TODO(jeffswenson): flush to disk if the buffer gets too large.

	// readyFrontier is an inclusive bound: all KV events with timestamps <=
	// readyFrontier have been emitted. Incoming KVs at or below readyFrontier
	// are treated as duplicates and dropped.
	readyFrontier hlc.Timestamp
	ready         []crosscluster.Event

	buffer []streampb.StreamEvent_KV
}

var _ streamclient.Subscription = (*OrderedFeed)(nil)

// Subscribe starts the ordered feed. It runs the raw subscription and the
// event processing loop concurrently, returning when both complete.
func (o *OrderedFeed) Subscribe(ctx context.Context) error {
	group := ctxgroup.WithContext(ctx)
	group.GoCtx(func(ctx context.Context) error {
		return o.rawSubscription.Subscribe(ctx)
	})
	group.GoCtx(func(ctx context.Context) error {
		defer close(o.events)
		return o.processEvents(ctx)
	})
	return group.Wait()
}

func (o *OrderedFeed) Events() <-chan crosscluster.Event {
	return o.events
}

func (o *OrderedFeed) Err() error {
	return o.err
}

// processEvents reads from the raw subscription's event channel, buffers KVs,
// and emits ordered write sets and checkpoints.
func (o *OrderedFeed) processEvents(ctx context.Context) error {
	for {
		var ev crosscluster.Event
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok = <-o.rawSubscription.Events():
			if !ok {
				for _, ready := range o.ready {
					if err := o.emitTimestamp(ctx, ready); err != nil {
						return err
					}
				}
				o.ready = nil
				o.err = o.rawSubscription.Err()
				return nil
			}
		}

		switch ev.Type() {
		case crosscluster.KVEvent:
			for _, kv := range ev.GetKVs() {
				kvTimestamp := kv.KeyValue.Value.Timestamp
				// Drop KVs at or below the ready frontier since they have
				// already been emitted (checkpoint timestamps are inclusive).
				if o.readyFrontier.Less(kvTimestamp) {
					o.buffer = append(o.buffer, kv)
				}
			}
		case crosscluster.CheckpointEvent:
			checkpoint := ev.GetCheckpoint()
			for _, ts := range checkpoint.ResolvedSpans {
				if _, _, err := o.frontier.Forward(ts.Span, ts.Timestamp); err != nil {
					return err
				}
			}
			if o.frontier.Frontier() != o.readyFrontier {
				o.advanceFrontier(o.frontier.Frontier())
				for _, ready := range o.ready {
					if err := o.emitTimestamp(ctx, ready); err != nil {
						return err
					}
				}
				o.ready = nil
				cpEvent := crosscluster.MakeCheckpointEvent(
					&streampb.StreamEvent_StreamCheckpoint{
						ResolvedSpans: []jobspb.ResolvedSpan{
							{
								Span:      o.coveringSpan,
								Timestamp: o.readyFrontier,
							},
						},
					})
				select {
				case <-ctx.Done():
					return ctx.Err()
				case o.events <- cpEvent:
				}
			}
		case crosscluster.SplitEvent:
			// ignore
		default:
			return errors.AssertionFailedf("unexpected event type %s", ev.Type())
		}
	}
}

func (o *OrderedFeed) emitTimestamp(ctx context.Context, ev crosscluster.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case o.events <- ev:
		return nil
	}
}

func (o *OrderedFeed) advanceFrontier(ts hlc.Timestamp) {
	o.readyFrontier = ts

	// Move all buffered KVs with timestamps <= ts into the ready list as a
	// single batch event, sorted by (timestamp, key) and deduplicated.
	if len(o.buffer) == 0 {
		return
	}

	slices.SortFunc(o.buffer, func(a, b streampb.StreamEvent_KV) int {
		hlcA := a.KeyValue.Value.Timestamp
		hlcB := b.KeyValue.Value.Timestamp
		if cmp := hlcA.Compare(hlcB); cmp != 0 {
			return cmp
		}
		return a.KeyValue.Key.Compare(b.KeyValue.Key)
	})

	afterCheckpoint := sort.Search(len(o.buffer), func(i int) bool {
		return o.buffer[i].KeyValue.Value.Timestamp.After(ts)
	})

	// The buffer is sorted by (timestamp, key) so duplicates are adjacent.
	flush := o.buffer[:afterCheckpoint]
	o.buffer = o.buffer[afterCheckpoint:]
	var batch []streampb.StreamEvent_KV
	for i, kv := range flush {
		if 0 < i && flush[i-1].KeyValue.Key.Equal(kv.KeyValue.Key) &&
			flush[i-1].KeyValue.Value.Timestamp == kv.KeyValue.Value.Timestamp {
			continue // skip duplicate
		}
		batch = append(batch, kv)
	}
	if len(batch) != 0 {
		o.ready = append(o.ready, crosscluster.MakeKVEvent(batch))
	}
}
