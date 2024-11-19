// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	kvpb "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Watcher slurps all changes that happen to some span of kvs using RangeFeed.
type Watcher struct {
	env *Env
	mu  struct {
		syncutil.Mutex
		kvs             *Engine
		frontier        span.Frontier
		frontierWaiters map[hlc.Timestamp][]chan error
	}
	cancel func()
	g      ctxgroup.Group
}

// Watch starts a new Watcher over the given span of kvs. See Watcher.
func Watch(ctx context.Context, env *Env, dbs []*kv.DB, dataSpan roachpb.Span) (*Watcher, error) {
	if len(dbs) < 1 {
		return nil, errors.New(`at least one db must be given`)
	}
	firstDB := dbs[0]

	w := &Watcher{
		env: env,
	}
	var err error
	if w.mu.kvs, err = MakeEngine(); err != nil {
		return nil, err
	}
	w.mu.frontier, err = span.MakeFrontier(dataSpan)
	if err != nil {
		return nil, err
	}
	w.mu.frontierWaiters = make(map[hlc.Timestamp][]chan error)
	ctx, w.cancel = context.WithCancel(ctx)
	w.g = ctxgroup.WithContext(ctx)

	dss := make([]*kvcoord.DistSender, len(dbs))
	for i := range dbs {
		sender := dbs[i].NonTransactionalSender()
		dss[i] = sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
	}

	startTs := firstDB.Clock().Now()
	eventC := make(chan kvcoord.RangeFeedMessage, 128)
	w.g.GoCtx(func(ctx context.Context) error {
		ts := startTs
		for i := 0; ; i = (i + 1) % len(dbs) {
			w.mu.Lock()
			ts.Forward(w.mu.frontier.Frontier())
			w.mu.Unlock()

			ds := dss[i]
			err := ds.RangeFeed(ctx, []kvcoord.SpanTimePair{{Span: dataSpan, StartAfter: ts}}, eventC, kvcoord.WithDiff())
			if isRetryableRangeFeedErr(err) {
				log.Infof(ctx, "got retryable RangeFeed error: %+v", err)
				continue
			}
			return err
		}
	})
	w.g.GoCtx(func(ctx context.Context) error {
		return w.processEvents(ctx, eventC)
	})

	// Make sure the RangeFeed has started up, else we might lose some events.
	if err := w.WaitForFrontier(ctx, startTs); err != nil {
		_ = w.Finish()
		return nil, err
	}

	return w, nil
}

func isRetryableRangeFeedErr(err error) bool {
	switch {
	case errors.Is(err, context.Canceled):
		return false
	default:
		return true
	}
}

// Finish tears down the Watcher and returns all the kvs it has ingested. It may
// be called multiple times, though not concurrently.
func (w *Watcher) Finish() *Engine {
	if w.cancel == nil {
		// Finish was already called.
		return w.mu.kvs
	}
	defer w.mu.frontier.Release()

	w.cancel()
	w.cancel = nil
	// Only WaitForFrontier cares about errors.
	_ = w.g.Wait()
	return w.mu.kvs
}

// WaitForFrontier blocks until all kv changes <= the given timestamp are
// guaranteed to have been ingested.
func (w *Watcher) WaitForFrontier(ctx context.Context, ts hlc.Timestamp) (retErr error) {
	log.Infof(ctx, `watcher waiting for %s`, ts)
	if err := w.env.SetClosedTimestampInterval(ctx, 1*time.Millisecond); err != nil {
		return err
	}
	defer func() {
		if err := w.env.ResetClosedTimestampInterval(ctx); err != nil {
			retErr = errors.WithSecondaryError(retErr, err)
		}
	}()
	resultCh := make(chan error, 1)
	w.mu.Lock()
	w.mu.frontierWaiters[ts] = append(w.mu.frontierWaiters[ts], resultCh)
	w.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-resultCh:
		return err
	}
}

func (w *Watcher) processEvents(ctx context.Context, eventC chan kvcoord.RangeFeedMessage) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-eventC:
			switch e := event.GetValue().(type) {
			case *kvpb.RangeFeedError:
				return e.Error.GoError()
			case *kvpb.RangeFeedValue:
				if err := w.handleValue(ctx, roachpb.Span{Key: e.Key}, e.Value, &e.PrevValue); err != nil {
					return err
				}
			case *kvpb.RangeFeedDeleteRange:
				if err := w.handleValue(ctx, e.Span, roachpb.Value{Timestamp: e.Timestamp}, nil /* prevV */); err != nil {
					return err
				}
			case *kvpb.RangeFeedCheckpoint:
				if err := w.handleCheckpoint(ctx, e.Span, e.ResolvedTS); err != nil {
					return err
				}
			case *kvpb.RangeFeedSSTable:
				if err := w.handleSSTable(ctx, e.Data); err != nil {
					return err
				}
			default:
				return errors.Errorf("unknown event: %T", e)
			}
		}
	}
}

func (w *Watcher) handleValue(
	ctx context.Context, span roachpb.Span, v roachpb.Value, prevV *roachpb.Value,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.handleValueLocked(ctx, span, v, prevV)
}

func (w *Watcher) handleValueLocked(
	ctx context.Context, span roachpb.Span, v roachpb.Value, prevV *roachpb.Value,
) error {
	var buf strings.Builder
	fmt.Fprintf(&buf, `rangefeed %s %s -> %s`, span, v.Timestamp, v.PrettyPrint())
	if prevV != nil {
		fmt.Fprintf(&buf, ` (prev %s)`, prevV.PrettyPrint())
	}
	// TODO(dan): If the exact key+ts is put into kvs more than once, the
	// Engine will keep the last. This matches our txn semantics (if a key
	// is written in a transaction more than once, only the last is kept)
	// but it means that we'll won't catch it if we violate those semantics.
	// Consider first doing a Get and somehow failing if this exact key+ts
	// has previously been put with a different value.
	if len(span.EndKey) > 0 {
		// If we have two operations that are not atomic (i.e. aren't in a batch)
		// and they produce touching tombstones at the same timestamp, then
		// `.mu.kvs` will merge them but they wouldn't be merged in pebble, since
		// their MVCCValueHeader will contain different seqnos (and thus the value
		// isn't identical). To work around that, we put random stuff in here. This
		// is never interpreted - the seqno is only pulled out via an interceptor at
		// the rangefeed boundary, and handed to the tracker. This is merely our
		// local copy.
		//
		// See https://github.com/cockroachdb/cockroach/issues/92822.
		var vh enginepb.MVCCValueHeader
		vh.KVNemesisSeq.Set(kvnemesisutil.Seq(rand.Int63n(math.MaxUint32)))
		mvccV := storage.MVCCValue{
			MVCCValueHeader: vh,
		}

		sl, err := storage.EncodeMVCCValue(mvccV)
		if err != nil {
			return err
		}

		w.mu.kvs.DeleteRange(span.Key, span.EndKey, v.Timestamp, sl)
		return nil
	}

	// Handle a point write.
	w.mu.kvs.Put(storage.MVCCKey{Key: span.Key, Timestamp: v.Timestamp}, v.RawBytes)
	if prevV != nil {
		prevTs := v.Timestamp.Prev()
		getPrevV := w.mu.kvs.Get(span.Key, prevTs)

		// RangeFeed doesn't send the timestamps of the previous values back
		// because changefeeds don't need them. It would likely be easy to
		// implement, but would add unnecessary allocations in changefeeds,
		// which don't need them. This means we'd want to make it an option in
		// the request, which seems silly to do for only this test.
		getPrevV.Timestamp = hlc.Timestamp{}
		// Additionally, ensure that deletion tombstones and missing keys are
		// normalized as the nil slice, so that they can be matched properly
		// between the RangeFeed and the Engine.
		if len(prevV.RawBytes) == 0 {
			prevV.RawBytes = nil
		}
		prevValueMismatch := !reflect.DeepEqual(prevV, &getPrevV)
		var engineContents string
		if prevValueMismatch {
			engineContents = w.mu.kvs.DebugPrint("  ")
		}

		if prevValueMismatch {
			log.Infof(ctx, "rangefeed mismatch\n%s", engineContents)
			s := mustGetStringValue(getPrevV.RawBytes)
			fmt.Println(s)
			return errors.Errorf(
				`expected (%s, %s) has previous value %s in kvs, but rangefeed has: %s`,
				span, prevTs, mustGetStringValue(getPrevV.RawBytes), mustGetStringValue(prevV.RawBytes))
		}
	}
	return nil
}

func (w *Watcher) handleCheckpoint(
	ctx context.Context, span roachpb.Span, resolvedTS hlc.Timestamp,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	frontierAdvanced, err := w.mu.frontier.Forward(span, resolvedTS)
	if err != nil {
		return errors.Wrapf(err, "unexpected frontier error advancing to %s@%s", span, resolvedTS)
	}
	if frontierAdvanced {
		frontier := w.mu.frontier.Frontier()
		log.Infof(ctx, `watcher reached frontier %s lagging by %s`,
			frontier, timeutil.Since(frontier.GoTime()))
		for ts, chs := range w.mu.frontierWaiters {
			if frontier.Less(ts) {
				continue
			}
			log.Infof(ctx, `watcher notifying %s`, ts)
			delete(w.mu.frontierWaiters, ts)
			for _, ch := range chs {
				ch <- nil
			}
		}
	}
	return nil
}

func (w *Watcher) handleSSTable(ctx context.Context, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(data) == 0 {
		return errors.AssertionFailedf("no SST data found")
	}

	iter, err := storage.NewMemSSTIterator(data, false /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); !ok {
			return err
		}

		// Add range keys.
		if iter.RangeKeyChanged() {
			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange {
				rangeKeys := iter.RangeKeys().Clone()
				for _, v := range rangeKeys.Versions {
					mvccValue, err := storage.DecodeMVCCValue(v.Value)
					if err != nil {
						return err
					}
					mvccValue.Value.Timestamp = v.Timestamp
					if seq := mvccValue.KVNemesisSeq.Get(); seq > 0 {
						w.env.Tracker.Add(rangeKeys.Bounds.Key, rangeKeys.Bounds.EndKey, v.Timestamp, seq)
					}
					if err := w.handleValueLocked(ctx, rangeKeys.Bounds, mvccValue.Value, nil); err != nil {
						return err
					}
				}
			}
			if !hasPoint { // can only happen at range key start bounds
				continue
			}
		}

		// Add point keys.
		key := iter.UnsafeKey().Clone()
		rawValue, err := iter.Value()
		if err != nil {
			return err
		}
		mvccValue, err := storage.DecodeMVCCValue(rawValue)
		if err != nil {
			return err
		}
		mvccValue.Value.Timestamp = key.Timestamp
		if seq := mvccValue.KVNemesisSeq.Get(); seq > 0 {
			w.env.Tracker.Add(key.Key, nil, key.Timestamp, seq)
		}
		if err := w.handleValueLocked(ctx, roachpb.Span{Key: key.Key}, mvccValue.Value, nil); err != nil {
			return err
		}
		log.Infof(ctx, `rangefeed AddSSTable %s %s -> %s`,
			key.Key, key.Timestamp, mvccValue.Value.PrettyPrint())
	}
}
