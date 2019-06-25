// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// Stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	// Context returns the context for this stream.
	Context() context.Context
	// Send blocks until it sends m, the stream is done, or the stream breaks.
	// Send must be safe to call on the same stream in different goroutines.
	Send(*roachpb.RangeFeedEvent) error
}

// registration is an instance of a rangefeed subscriber who has
// registered to receive updates for a specific range of keys.
// Updates are delivered to its stream until one of the following
// conditions is met:
// 1. a Send to the Stream returns an error
// 2. the Stream's context is canceled
// 3. the registration is manually unregistered
//
// In all cases, when a registration is unregistered its error
// channel is sent an error to inform it that the registration
// has finished.
type registration struct {
	// Input.
	span             roachpb.Span
	catchupIter      engine.SimpleIterator
	catchupTimestamp hlc.Timestamp
	metrics          *Metrics

	// Output.
	stream Stream
	errC   chan<- *roachpb.Error

	// Internal.
	id   int64
	keys interval.Range
	buf  chan *roachpb.RangeFeedEvent

	mu struct {
		sync.Locker
		// True if this registration buffer has overflowed, dropping a live event.
		// This will cause the registration to exit with an error once the buffer
		// has been emptied.
		overflowed bool
		// Boolean indicating if all events have been output to stream. Used only
		// for testing.
		caughtUp bool
		// Management of the output loop goroutine, used to ensure proper teardown.
		outputLoopCancelFn func()
		disconnected       bool
	}
}

func newRegistration(
	span roachpb.Span,
	startTS hlc.Timestamp,
	catchupIter engine.SimpleIterator,
	bufferSz int,
	metrics *Metrics,
	stream Stream,
	errC chan<- *roachpb.Error,
) registration {
	r := registration{
		span:             span,
		catchupIter:      catchupIter,
		metrics:          metrics,
		stream:           stream,
		errC:             errC,
		buf:              make(chan *roachpb.RangeFeedEvent, bufferSz),
		catchupTimestamp: startTS,
	}
	r.mu.Locker = &syncutil.Mutex{}
	r.mu.caughtUp = true
	return r
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the overflowed flag is set,
// indicating that live events were lost and a catchup scan should be initiated.
// If overflowed is already set, events are ignored and not written to the
// buffer.
func (r *registration) publish(event *roachpb.RangeFeedEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.overflowed {
		return
	}
	select {
	case r.buf <- event:
		r.mu.caughtUp = false
	default:
		// Buffer exceeded and we are dropping this event. Registration will need
		// a catch-up scan.
		r.mu.overflowed = true
	}
}

// disconnect cancels the output loop context for the registration and passes an
// error to the output error stream for the registration. This also sets the
// disconnected flag on the registration, preventing it from being disconnected
// again.
func (r *registration) disconnect(pErr *roachpb.Error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.mu.disconnected {
		if r.mu.outputLoopCancelFn != nil {
			r.mu.outputLoopCancelFn()
		}
		r.mu.disconnected = true
		r.errC <- pErr
	}
}

// outputLoop is the operational loop for a single registration. The behavior
// is as thus:
//
// 1. If a catch-up scan is indicated, run one before beginning the proper
// output loop.
// 2. After catch-up is complete, begin reading from the registration buffer
// channel and writing to the output stream until the buffer is empty *and*
// the overflow flag has been set.
//
// The loop exits with any error encountered, if the provided context is
// canceled, or when the buffer has overflowed and all pre-overflow entries
// have been emitted.
func (r *registration) outputLoop(ctx context.Context) error {
	// If the registration has a catch-up scan,
	if r.catchupIter != nil {
		if err := r.runCatchupScan(); err != nil {
			err = errors.Wrap(err, "catch-up scan failed")
			log.Error(ctx, err)
			return err
		}
	}

	// Normal buffered output loop.
	for {
		overflowed := false
		r.mu.Lock()
		if len(r.buf) == 0 {
			overflowed = r.mu.overflowed
			r.mu.caughtUp = true
		}
		r.mu.Unlock()
		if overflowed {
			return newErrBufferCapacityExceeded().GoError()
		}

		select {
		case nextEvent := <-r.buf:
			if err := r.stream.Send(nextEvent); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stream.Context().Done():
			return r.stream.Context().Err()
		}
	}
}

func (r *registration) runOutputLoop(ctx context.Context) {
	r.mu.Lock()
	ctx, r.mu.outputLoopCancelFn = context.WithCancel(ctx)
	r.mu.Unlock()
	err := r.outputLoop(ctx)
	r.disconnect(roachpb.NewError(err))
}

// runCatchupScan starts a catchup scan which will output entries for all
// recorded changes in the replica that are newer than the catchupTimeStamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
func (r *registration) runCatchupScan() error {
	if r.catchupIter == nil {
		return nil
	}
	start := timeutil.Now()
	defer func() {
		r.catchupIter.Close()
		r.catchupIter = nil
		r.metrics.RangeFeedCatchupScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	var a bufalloc.ByteAllocator
	startKey := engine.MakeMVCCMetadataKey(r.span.Key)
	endKey := engine.MakeMVCCMetadataKey(r.span.EndKey)

	// Iterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse.
	reorderBuf := make([]roachpb.RangeFeedEvent, 0, 5)
	var lastKey []byte
	outputEvents := func() error {
		for i := len(reorderBuf) - 1; i >= 0; i-- {
			e := reorderBuf[i]
			if err := r.stream.Send(&e); err != nil {
				return err
			}
		}
		reorderBuf = reorderBuf[:0]
		return nil
	}

	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var meta enginepb.MVCCMetadata

	for r.catchupIter.Seek(startKey); ; r.catchupIter.Next() {
		if ok, err := r.catchupIter.Valid(); err != nil {
			return err
		} else if !ok || !r.catchupIter.UnsafeKey().Less(endKey) {
			break
		}

		unsafeKey := r.catchupIter.UnsafeKey()
		unsafeVal := r.catchupIter.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}
			if !meta.IsInline() {
				// This is an MVCCMetadata key for an intent. The catchup scan
				// only cares about committed values, so ignore this and skip
				// past the corresponding provisional key-value. To do this,
				// scan directly to the provisional key and let the loop Next
				// to the following key after it.
				r.catchupIter.Seek(engine.MVCCKey{
					Key:       unsafeKey.Key,
					Timestamp: hlc.Timestamp(meta.Timestamp),
				})
				continue
			}

			// If write is inline, it doesn't have a timestamp so we don't
			// filter on the registration's starting timestamp. Instead, we
			// return all inline writes.
			unsafeVal = meta.RawBytes
		} else if !r.catchupTimestamp.Less(unsafeKey.Timestamp) {
			// At or before the registration's exclusive starting timestamp.
			// Ignore.
			continue
		}

		var key, val []byte
		a, key = a.Copy(unsafeKey.Key, 0)
		a, val = a.Copy(unsafeVal, 0)
		ts := unsafeKey.Timestamp

		// Output values in order
		if !bytes.Equal(key, lastKey) {
			if err := outputEvents(); err != nil {
				return err
			}
			lastKey = key
		}

		var event roachpb.RangeFeedEvent
		event.MustSetValue(&roachpb.RangeFeedValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes:  val,
				Timestamp: ts,
			},
		})
		reorderBuf = append(reorderBuf, event)
	}

	// Output events for the last key encountered.
	return outputEvents()
}

// ID implements interval.Interface.
func (r *registration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration) Range() interval.Range {
	return r.keys
}

func (r registration) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.catchupTimestamp)
}

// registry holds a set of registrations and manages their lifecycle.
type registry struct {
	tree    interval.Tree // *registration items
	idAlloc int64
}

func makeRegistry() registry {
	return registry{
		tree: interval.NewTree(interval.ExclusiveOverlapper),
	}
}

// Len returns the number of registrations in the registry.
func (reg *registry) Len() int {
	return reg.tree.Len()
}

// Register adds the provided registration to the registry.
func (reg *registry) Register(r *registration) {
	r.id = reg.nextID()
	r.keys = r.span.AsRange()
	if err := reg.tree.Insert(r, false /* fast */); err != nil {
		panic(err)
	}
}

func (reg *registry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *registry) PublishToOverlapping(span roachpb.Span, event *roachpb.RangeFeedEvent) {
	// Determine the earliest starting timestamp that a registration
	// can have while still needing to hear about this event.
	var minTS hlc.Timestamp
	switch t := event.GetValue().(type) {
	case *roachpb.RangeFeedValue:
		// Only publish values to registrations with starting
		// timestamps equal to or greater than the value's timestamp.
		minTS = t.Value.Timestamp
	case *roachpb.RangeFeedCheckpoint:
		// Always publish checkpoint notifications, regardless of a registration's
		// starting timestamp.
		//
		// TODO(dan): It's unclear if this is the right contract, it's certainly
		// surprising. Revisit this once RangeFeed has more users.
		minTS = hlc.MaxTimestamp
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", event))
	}

	reg.forOverlappingRegs(span, func(r *registration) (bool, *roachpb.Error) {
		// Don't publish events if they are equal to or less
		// than the registration's starting timestamp.

		if r.catchupTimestamp.Less(minTS) {
			r.publish(event)
		}
		return false, nil
	})
}

// Unregister removes a registration from the registry. It is assumed that the
// registration has already been disconnected, this is intended only to clean
// up the registry.
func (reg *registry) Unregister(r *registration) {
	if err := reg.tree.Delete(r, false /* fast */); err != nil {
		panic(err)
	}
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *registry) Disconnect(span roachpb.Span) {
	reg.DisconnectWithErr(span, nil /* pErr */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry) DisconnectWithErr(span roachpb.Span, pErr *roachpb.Error) {
	reg.forOverlappingRegs(span, func(_ *registration) (bool, *roachpb.Error) {
		return true, pErr
	})
}

// all is a span that overlaps with all registrations.
var all = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

// forOverlappingRegs calls the provided function on each registration that
// overlaps the span. If the function returns true for a given registration
// then that registration is unregistered and the error returned by the
// function is send on its corresponding error channel.
func (reg *registry) forOverlappingRegs(
	span roachpb.Span, fn func(*registration) (disconnect bool, pErr *roachpb.Error),
) {
	var toDelete []interval.Interface
	matchFn := func(i interval.Interface) (done bool) {
		r := i.(*registration)
		dis, pErr := fn(r)
		if dis {
			r.disconnect(pErr)
			toDelete = append(toDelete, i)
		}
		return false
	}
	if span.EqualValue(all) {
		reg.tree.Do(matchFn)
	} else {
		reg.tree.DoMatching(matchFn, span.AsRange())
	}

	if len(toDelete) == reg.tree.Len() {
		reg.tree.Clear()
	} else if len(toDelete) == 1 {
		if err := reg.tree.Delete(toDelete[0], false /* fast */); err != nil {
			panic(err)
		}
	} else if len(toDelete) > 1 {
		for _, i := range toDelete {
			if err := reg.tree.Delete(i, true /* fast */); err != nil {
				panic(err)
			}
		}
		reg.tree.AdjustRanges()
	}
}

// Wait for this registration to completely process its internal buffer.
func (r *registration) waitForCaughtUp() error {
	opts := retry.Options{
		InitialBackoff: 5 * time.Millisecond,
		Multiplier:     2,
		MaxBackoff:     10 * time.Second,
		MaxRetries:     50,
	}
	for re := retry.Start(opts); re.Next(); {
		r.mu.Lock()
		caughtUp := len(r.buf) == 0 && r.mu.caughtUp
		r.mu.Unlock()
		if caughtUp {
			return nil
		}
	}
	return errors.Errorf("registration %v failed to empty in time", r.Range())
}

// waitForCaughtUp waits for all registrations overlapping the given span to
// completely process their internal buffers.
func (reg *registry) waitForCaughtUp(span roachpb.Span) error {
	var outerErr error
	reg.forOverlappingRegs(span, func(r *registration) (bool, *roachpb.Error) {
		if outerErr == nil {
			outerErr = r.waitForCaughtUp()
		}
		return false, nil
	})
	return outerErr
}
