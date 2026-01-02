// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

type WriteSet struct {
	Timestamp hlc.Timestamp
	Rows      []streampb.StreamEvent_KV
}

// TODO(jeffswenson): consider refreshing the stream client API. The whole
// approach of creating a distsql plan on the source side seems awkward.
// Generally, the destination wants to control the plan so that it can
// optimize local ingestion and do thing like plan the number of processors
// based on the number of nodes in the destination.
//
// An alternative approach would be to simplify the stream client API to return:
// - A set of spans and nodes that can serve those spans (source topology).
// - An API for fetching/watching schema changes on the source cluster.
// - An API for streaming events for a given span that is closer to the API
// of a rangefeed.
func NewOrderedFeed(
	rawSubscription streamclient.Subscription, initialFrontier span.Frontier,
) (*OrderedFeed, error) {
	return &OrderedFeed{
		rawSubscription: rawSubscription,
		frontier:        initialFrontier,
		readyFrontier:   initialFrontier.Frontier(),
	}, nil
}

type OrderedFeed struct {
	rawSubscription streamclient.Subscription
	frontier        span.Frontier

	// TODO(jeffswenson): this implementation of ordered feed is only suitable
	// for an initial prototype.
	// TODO(jeffswenson): add memory accounting.
	// TODO(jeffswenson): flush to disk if the buffer gets too large.

	readyFrontier hlc.Timestamp
	ready         []WriteSet

	buffer []streampb.StreamEvent_KV
}

func (o *OrderedFeed) Run(ctx context.Context) error {
	return o.rawSubscription.Subscribe(ctx)
}

func (o *OrderedFeed) Next(ctx context.Context) (WriteSet, error) {
	for {
		if len(o.ready) != 0 {
			result := o.ready[0]
			o.ready = o.ready[1:]
			return result, nil
		}

		var ev crosscluster.Event
		select {
		case <-ctx.Done():
			return WriteSet{}, ctx.Err()
		case ev = <-o.rawSubscription.Events():
			// continue
		}

		switch ev.Type() {
		case crosscluster.KVEvent:
			for _, kv := range ev.GetKVs() {
				kvTimestamp := kv.KeyValue.Value.Timestamp
				// TODO how do we want to handle the eq case? Should eq be emitted or
				// excluded?
				if o.readyFrontier.Less(kvTimestamp) {
					o.buffer = append(o.buffer, kv)
				}
			}
		case crosscluster.CheckpointEvent:
			checkpoint := ev.GetCheckpoint()
			for _, ts := range checkpoint.ResolvedSpans {
				if _, err := o.frontier.Forward(ts.Span, ts.Timestamp); err != nil {
					return WriteSet{}, err
				}
			}
			if o.frontier.Frontier() != o.readyFrontier {
				o.advanceFrontier(o.frontier.Frontier())
			}
		case crosscluster.SplitEvent:
			// ignore
		default:
			return WriteSet{}, errors.AssertionFailedf("unexpected event type %s", ev.Type())
		}
	}
}

func (o *OrderedFeed) advanceFrontier(ts hlc.Timestamp) {
	o.readyFrontier = ts

	// Partition the buffer so we have all KVs up to the new frontier
	// timestamps.
	if len(o.buffer) == 0 {
		return
	}

	// Cut the buffer up into the writesets.
	slices.SortFunc(o.buffer, func(a, b streampb.StreamEvent_KV) int {
		// Sorty by time first in order to group by transaction.
		hlcA := a.KeyValue.Value.Timestamp
		hlcB := b.KeyValue.Value.Timestamp
		if cmp := hlcA.Compare(hlcB); cmp != 0 {
			return cmp
		}

		// Sort by key secondarily to allow for deduplication.
		keyA := a.KeyValue.Key
		keyB := b.KeyValue.Key
		return keyA.Compare(keyB)
	})

	// [txnStart, txnEnx] is the range of KVs for the current transaction.
	txnStart, txnEnd := 0, 0
	txnTimestamp := o.buffer[0].KeyValue.Value.Timestamp

	if txnTimestamp.After(ts) {
		return
	}

	for i, kv := range o.buffer {
		kvTimestamp := kv.KeyValue.Value.Timestamp
		if kvTimestamp == txnTimestamp {
			txnEnd = i
		} else if kvTimestamp.After(ts) {
			break
		} else {
			o.ready = append(o.ready, o.createWriteSet(o.buffer[txnStart:txnEnd+1]))
			txnStart = i
			txnEnd = i
			txnTimestamp = kvTimestamp
		}
	}
	o.ready = append(o.ready, o.createWriteSet(o.buffer[txnStart:txnEnd+1]))
	o.buffer = o.buffer[txnEnd+1:]
}

func (o *OrderedFeed) createWriteSet(kvs []streampb.StreamEvent_KV) WriteSet {
	writes := make([]streampb.StreamEvent_KV, 0, len(kvs))
	writes = append(writes, kvs[0])

	for _, kv := range kvs[1:] {
		prev := writes[len(writes)-1]
		if !prev.KeyValue.Key.Equal(kv.KeyValue.Key) {
			writes = append(writes, kv)
		}
	}

	return WriteSet{
		Timestamp: kvs[0].KeyValue.Value.Timestamp,
		Rows:      writes,
	}
}
