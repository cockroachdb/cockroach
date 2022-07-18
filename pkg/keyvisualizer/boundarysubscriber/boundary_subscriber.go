package boundarysubscriber

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type CollectionBoundarySubscriber struct {
	rfc     *rangefeedcache.Watcher
	handler func(context.Context, keyvisualizer.BoundaryUpdate)
}

// New is invoked by the server.
func New() *CollectionBoundarySubscriber {
	b := &CollectionBoundarySubscriber{}
	//b.rfc = rangefeedcache.NewWatcher()
	return b
}

func (b *CollectionBoundarySubscriber) handleUpdate(
	ctx context.Context,
	u rangefeedcache.Update,
) {
	for _, ev := range u.Events {
		update := ev.(*bufferEvent).BoundaryUpdate
		b.handler(ctx, update)
	}
}

func (b *CollectionBoundarySubscriber) Start(
	ctx context.Context, stopper *stop.Stopper,
) error {
	return rangefeedcache.Start(ctx, stopper, b.rfc, nil)
}

func (b *CollectionBoundarySubscriber) Subscribe(
	fn func(context.Context, keyvisualizer.BoundaryUpdate),
) {
	b.handler = fn
}

type bufferEvent struct {
	keyvisualizer.BoundaryUpdate
	ts hlc.Timestamp
}

// Timestamp implements the rangefeedbuffer.Event interface.
func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}
