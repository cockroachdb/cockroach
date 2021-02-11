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
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	span                   roachpb.Span
	catchupTimestamp       hlc.Timestamp
	catchupIterConstructor func() storage.SimpleMVCCIterator
	withDiff               bool
	metrics                *Metrics

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
	catchupIterConstructor func() storage.SimpleMVCCIterator,
	withDiff bool,
	bufferSz int,
	metrics *Metrics,
	stream Stream,
	errC chan<- *roachpb.Error,
) registration {
	r := registration{
		span:                   span,
		catchupTimestamp:       startTS,
		catchupIterConstructor: catchupIterConstructor,
		withDiff:               withDiff,
		metrics:                metrics,
		stream:                 stream,
		errC:                   errC,
		buf:                    make(chan *roachpb.RangeFeedEvent, bufferSz),
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
	r.validateEvent(event)
	event = r.maybeStripEvent(event)

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

// validateEvent checks that the event contains enough information for the
// registation.
func (r *registration) validateEvent(event *roachpb.RangeFeedEvent) {
	switch t := event.GetValue().(type) {
	case *roachpb.RangeFeedValue:
		if t.Key == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Key: %v", t))
		}
		if t.Value.RawBytes == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Value.RawBytes: %v", t))
		}
		if t.Value.Timestamp.IsEmpty() {
			panic(fmt.Sprintf("unexpected empty RangeFeedValue.Value.Timestamp: %v", t))
		}
	case *roachpb.RangeFeedCheckpoint:
		if t.Span.Key == nil {
			panic(fmt.Sprintf("unexpected empty RangeFeedCheckpoint.Span.Key: %v", t))
		}
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
}

// maybeStripEvent determines whether the event contains excess information not
// applicable to the current registration. If so, it makes a copy of the event
// and strips the incompatible information to match only what the registration
// requested.
func (r *registration) maybeStripEvent(event *roachpb.RangeFeedEvent) *roachpb.RangeFeedEvent {
	ret := event
	copyOnWrite := func() interface{} {
		if ret == event {
			ret = event.ShallowCopy()
		}
		return ret.GetValue()
	}

	switch t := ret.GetValue().(type) {
	case *roachpb.RangeFeedValue:
		if t.PrevValue.IsPresent() && !r.withDiff {
			// If no registrations for the current Range are requesting previous
			// values, then we won't even retrieve them on the Raft goroutine.
			// However, if any are and they overlap with an update then the
			// previous value on the corresponding events will be populated.
			// If we're in this case and any other registrations don't want
			// previous values then we'll need to strip them.
			t = copyOnWrite().(*roachpb.RangeFeedValue)
			t.PrevValue = roachpb.Value{}
		}
	case *roachpb.RangeFeedCheckpoint:
		if !t.Span.EqualValue(r.span) {
			// Checkpoint events are always created spanning the entire Range.
			// However, a registration might not be listening on updates over
			// the entire Range. If this is the case then we need to constrain
			// the checkpoint events published to that registration to just the
			// span that it's listening on. This is more than just a convenience
			// to consumers - it would be incorrect to say that a rangefeed has
			// observed all values up to the checkpoint timestamp over a given
			// key span if any updates to that span have been filtered out.
			if !t.Span.Contains(r.span) {
				panic(fmt.Sprintf("registration span %v larger than checkpoint span %v", r.span, t.Span))
			}
			t = copyOnWrite().(*roachpb.RangeFeedCheckpoint)
			t.Span = r.span
		}
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
	}
	return ret
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
	// If the registration has a catch-up scan, run it.
	if err := r.maybeRunCatchupScan(); err != nil {
		err = errors.Wrap(err, "catch-up scan failed")
		log.Errorf(ctx, "%v", err)
		return err
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

// maybeRunCatchupScan starts a catchup scan which will output entries for all
// recorded changes in the replica that are newer than the catchupTimestamp.
// This uses the iterator provided when the registration was originally created;
// after the scan completes, the iterator will be closed.
//
// If the registration does not have a catchUpIteratorConstructor, this method
// is a no-op.
func (r *registration) maybeRunCatchupScan() error {
	if r.catchupIterConstructor == nil {
		return nil
	}
	catchupIter := r.catchupIterConstructor()
	r.catchupIterConstructor = nil
	start := timeutil.Now()
	defer func() {
		catchupIter.Close()
		r.metrics.RangeFeedCatchupScanNanos.Inc(timeutil.Since(start).Nanoseconds())
	}()

	var a bufalloc.ByteAllocator
	startKey := storage.MakeMVCCMetadataKey(r.span.Key)
	endKey := storage.MakeMVCCMetadataKey(r.span.EndKey)

	// MVCCIterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse. This also allows us to buffer events
	// as we fill in previous values.
	var lastKey roachpb.Key
	reorderBuf := make([]roachpb.RangeFeedEvent, 0, 5)
	addPrevToLastEvent := func(val []byte) {
		if l := len(reorderBuf); l > 0 {
			if reorderBuf[l-1].Val.PrevValue.IsPresent() {
				panic("RangeFeedValue.PrevVal unexpectedly set")
			}
			reorderBuf[l-1].Val.PrevValue.RawBytes = val
		}
	}
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
	catchupIter.SeekGE(startKey)
	for {
		if ok, err := catchupIter.Valid(); err != nil {
			return err
		} else if !ok || !catchupIter.UnsafeKey().Less(endKey) {
			break
		}

		unsafeKey := catchupIter.UnsafeKey()
		unsafeVal := catchupIter.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}
			if !meta.IsInline() {
				// This is an MVCCMetadata key for an intent. The catchup scan
				// only cares about committed values, so ignore this and skip
				// past the corresponding provisional key-value. To do this,
				// scan to the timestamp immediately before (i.e. the key
				// immediately after) the provisional key.
				//
				// Make a copy since should not pass an unsafe key from the iterator
				// that provided it, when asking it to seek.
				a, unsafeKey.Key = a.Copy(unsafeKey.Key, 0)
				catchupIter.SeekGE(storage.MVCCKey{
					Key:       unsafeKey.Key,
					Timestamp: meta.Timestamp.ToTimestamp().Prev(),
				})
				continue
			}

			// If write is inline, it doesn't have a timestamp so we don't
			// filter on the registration's starting timestamp. Instead, we
			// return all inline writes.
			unsafeVal = meta.RawBytes
		}

		// Determine whether the iterator moved to a new key.
		sameKey := bytes.Equal(unsafeKey.Key, lastKey)
		if !sameKey {
			// If so, output events for the last key encountered.
			if err := outputEvents(); err != nil {
				return err
			}
			a, lastKey = a.Copy(unsafeKey.Key, 0)
		}
		key := lastKey
		ts := unsafeKey.Timestamp

		// Ignore the version if it's not inline and its timestamp is at
		// or before the registration's (exclusive) starting timestamp.
		ignore := !(ts.IsEmpty() || r.catchupTimestamp.Less(ts))
		if ignore && !r.withDiff {
			// Skip all the way to the next key.
			// NB: fast-path to avoid value copy when !r.withDiff.
			catchupIter.NextKey()
			continue
		}

		var val []byte
		a, val = a.Copy(unsafeVal, 0)
		if r.withDiff {
			// Update the last version with its previous value (this version).
			addPrevToLastEvent(val)
		}

		if ignore {
			// Skip all the way to the next key.
			catchupIter.NextKey()
		} else {
			// Move to the next version of this key.
			catchupIter.Next()

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

// NewFilter returns a operation filter reflecting the registrations
// in the registry.
func (reg *registry) NewFilter() *Filter {
	return newFilterFromRegistry(reg)
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
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", t))
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
