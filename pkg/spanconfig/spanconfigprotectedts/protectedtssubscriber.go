// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigprotectedts

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type Subscriber struct {
	clock                       *hlc.Clock
	stopper                     *stop.Stopper
	rangefeedFactory            *rangefeed.Factory
	decoder                     *protectedTimestampDecoder
	protectedTimestampTableSpan roachpb.Span // typically system.pts_records, but overridable for tests

	mu struct {
		syncutil.RWMutex
		internal ProtectedTimestampStore
		handlers []handler
	}
}

// GetPTSRecordsForTableAsOf implements the ProtectedTimestampStoreReader interface.
func (p *Subscriber) GetPTSRecordsForTableAsOf(
	id descpb.ID, parentID descpb.ID, asOf hlc.Timestamp,
) []roachpb.ProtectedTimestampRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.internal.GetPTSRecordsForTableAsOf(id, parentID, asOf)
}

var _ ProtectedTimestampSubscriber = &Subscriber{}

// NewSubscriber constructs instantiates a Subscriber.
func NewSubscriber(
	stopper *stop.Stopper,
	rangeFeedFactory *rangefeed.Factory,
	protectedTimestampTableID uint32,
	codec keys.SQLCodec,
) *Subscriber {
	ptsTableStart := keys.SystemSQLCodec.IndexPrefix(
		protectedTimestampTableID,
		keys.ProtectedTimestampsRecordsTableID,
	)
	protectedTimestampTableSpan := roachpb.Span{
		Key:    ptsTableStart,
		EndKey: ptsTableStart.PrefixEnd(),
	}
	s := &Subscriber{
		stopper:                     stopper,
		rangefeedFactory:            rangeFeedFactory,
		protectedTimestampTableSpan: protectedTimestampTableSpan,
		decoder:                     newProtectedTimestampDecoder(codec),
	}
	s.mu.internal = NewStore()
	return s
}

func (p *Subscriber) Start(ctx context.Context) error {
	return p.stopper.RunAsyncTask(ctx, "protectedts-subscriber", func(ctx context.Context) {
		ctx, cancel := p.stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		const aWhile = 5 * time.Minute // arbitrary but much longer than a retry
		for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {

			started := timeutil.Now()
			if err := p.run(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return // we're done here
				}

				if timeutil.Since(started) > aWhile {
					r.Reset()
				}

				log.Warningf(ctx, "protectedts-subscriber failed with %v, retrying...", err)
				continue
			}

			return // we're done here (the stopper was stopped, run exited cleanly)
		}
	})
}

// Subscribe installs a callback that's invoked with whatever span may have seen
// a config update.
func (s *Subscriber) Subscribe(fn func(ProtectedTimestampUpdate)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.handlers = append(s.mu.handlers, handler{fn: fn})
}

func (p *Subscriber) run(ctx context.Context) error {
	buffer := rangefeedbuffer.New(10 /* TODO */)
	mu := struct { // serializes access between the rangefeed and the main thread here
		syncutil.Mutex
		frontierTS hlc.Timestamp
	}{}

	frontierBumpedCh, initialScanDoneCh, errCh := make(chan struct{}), make(chan struct{}), make(chan error)

	onValue := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		deleted := !ev.Value.IsPresent()
		var value roachpb.Value
		if deleted {
			if !ev.PrevValue.IsPresent() {
				// It's possible to write a KV tombstone on top of another KV
				// tombstone -- both the new and old value will be empty. We simply
				// ignore these events.
				return
			}
		} else {
			value = ev.Value
		}
		recordID, recordRawVal, err := p.decoder.DecodeRow(roachpb.KeyValue{
			Key:   ev.Key,
			Value: value,
		})
		if err != nil {
			log.Fatalf(ctx, "failed to decode row: %v", err) // non-retryable error; just fatal
		}

		if log.ExpensiveLogEnabled(ctx, 1) {
			log.Infof(ctx, "received pts record update for %d (deleted=%t)", recordID, deleted)
		}

		var update ProtectedTimestampUpdate
		if deleted {
			update = ProtectedTimestampDeletion(recordID, *recordRawVal.Target)
		} else {
			update = ProtectedTimestampAddition(recordID, *recordRawVal.Target, recordRawVal.Timestamp)
		}

		if err := buffer.Add(&bufferEvent{update, ev.Value.Timestamp}); err != nil {
			select {
			case <-ctx.Done():
				// The context is canceled when the rangefeed is closed by the
				// main handler goroutine. It's closed after we stop listening
				// to errCh.
			case errCh <- err:
			}
		}
	}

	initialScanTS := p.clock.Now()
	//if initialScanTS.Less(s.lastFrontierTS) {
	//	log.Fatalf(ctx, "initial scan timestamp (%s) regressed from last recorded frontier (%s)", initialScanTS, s.lastFrontierTS)
	//}

	rangeFeed := p.rangefeedFactory.New("protectedtimestamp-rangefeed", initialScanTS,
		onValue,
		rangefeed.WithInitialScan(func(ctx context.Context) {
			select {
			case <-ctx.Done():
				// The context is canceled when the rangefeed is closed by the
				// main handler goroutine. It's closed after we stop listening
				// to initialScanDoneCh.
			case initialScanDoneCh <- struct{}{}:
			}
		}),
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, frontierTS hlc.Timestamp) {
			mu.Lock()
			mu.frontierTS = frontierTS
			mu.Unlock()

			select {
			case <-ctx.Done():
			case frontierBumpedCh <- struct{}{}:
			}
		}),
		rangefeed.WithDiff(),
		rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
			// TODO(adityamaru): Consider if there are other errors which we
			// want to treat as permanent. This was cargo culted from the
			// settings watcher.
			if grpcutil.IsAuthError(err) ||
				strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
				return true
			}
			return false
		}),
	)
	if err := rangeFeed.Start(ctx, []roachpb.Span{p.protectedTimestampTableSpan}); err != nil {
		return err
	}
	defer rangeFeed.Close()

	log.Info(ctx, "established range feed over pts_records table")

	for {
		select {
		case <-p.stopper.ShouldQuiesce():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-frontierBumpedCh:
			mu.Lock()
			frontierTS := mu.frontierTS
			mu.Unlock()

			events := buffer.Flush(ctx, frontierTS)
			p.mu.Lock()
			for _, ev := range events {
				p.mu.internal.Apply(frontierTS, ev.(*bufferEvent).ProtectedTimestampUpdate)
			}
			handlers := p.mu.handlers
			p.mu.Unlock()

			for _, h := range handlers {
				for _, ev := range events {
					h.invoke(ev.(*bufferEvent).ProtectedTimestampUpdate)
				}
			}
		case <-initialScanDoneCh:
			events := buffer.Flush(ctx, initialScanTS)
			freshStore := NewStore()
			for _, ev := range events {
				freshStore.Apply(initialScanTS, ev.(*bufferEvent).ProtectedTimestampUpdate)
			}

			p.mu.Lock()
			p.mu.internal = freshStore
			handlers := p.mu.handlers
			p.mu.Unlock()

			for _, h := range handlers {
				for _, ev := range events {
					h.invoke(ev.(*bufferEvent).ProtectedTimestampUpdate)
				}
			}

		case err := <-errCh:
			return err
		}
	}
}

type bufferEvent struct {
	ProtectedTimestampUpdate
	ts hlc.Timestamp
}

// Timestamp implements the rangefeedbuffer.Event interface.
func (w *bufferEvent) Timestamp() hlc.Timestamp {
	return w.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}

type handler struct {
	fn func(update ProtectedTimestampUpdate)
}

func (h *handler) invoke(update ProtectedTimestampUpdate) {
	h.fn(update)
}
