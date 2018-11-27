// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rangefeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
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
	// Internal.
	id               int64
	keys             interval.Range
	catchupTimestamp hlc.Timestamp

	mu struct {
		*syncutil.Mutex
		needsCatchup       bool
		outputLoopErr      error
		outputLoopCancelFn func()
		disconnected       bool
	}

	// Footprint.
	span roachpb.Span
	eng  engine.Engine

	// Buffer.
	buf chan *roachpb.RangeFeedEvent

	// Output.
	stream Stream
	errC   chan<- *roachpb.Error
}

func newRegistration(
	span roachpb.RSpan,
	startTS hlc.Timestamp,
	eng engine.Engine,
	bufferSz int,
	stream Stream,
	errC chan<- *roachpb.Error,
) registration {
	r := registration{
		span:             span.AsRawSpanWithNoLocals(),
		eng:              eng,
		stream:           stream,
		errC:             errC,
		buf:              make(chan *roachpb.RangeFeedEvent, bufferSz),
		catchupTimestamp: startTS,
	}
	r.mu.Mutex = &syncutil.Mutex{}
	if !startTS.IsEmpty() {
		r.mu.needsCatchup = true
	}
	return r
}

// publish attempts to send a single event to the output buffer for this
// registration. If the output buffer is full, the needsCatchup flag is set,
// indicating that live events were lost and a catchup scan should be initiated.
// If needsCatchup is already set, events are ignored and not written to the
// buffer.
func (r *registration) publish(event *roachpb.RangeFeedEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.needsCatchup {
		return
	}
	select {
	case r.buf <- event:
	default:
		r.mu.needsCatchup = true
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
// 1. If a catch-up scan is indicated, run one. This can occur at start-up, or
// if the buffer has been overfilled and the "needsCatchup" flag has been set.
// 2. After catch-up is complete, Begin reading from the registration buffer
// channel and writing to the output stream until the buffer is empty *and*
// the needsCatchup flag has been set.
//
// The loop exits with any error encountered, or if the provided context is
// canceled.
func (r *registration) outputLoop(ctx context.Context) error {
	for {
		r.mu.Lock()
		needsCatchup := r.mu.needsCatchup
		r.mu.needsCatchup = false
		r.mu.Unlock()

		// Run a catch-up scan from the current start time.
		if needsCatchup {
			if err := r.runCatchupScan(); err != nil {
				err = errors.Wrap(err, "catch-up scan failed")
				log.Error(ctx, err)
				return err
			}
		}

		// Normal buffered output loop.
		for {
			if len(r.buf) == 0 {
				r.mu.Lock()
				needsCatchup := r.mu.needsCatchup
				r.mu.Unlock()
				if needsCatchup {
					break
				}
			}

			var nextEvent *roachpb.RangeFeedEvent
			select {
			case nextEvent = <-r.buf:
			case <-ctx.Done():
				return ctx.Err()
			case <-r.stream.Context().Done():
				return r.stream.Context().Err()
			}

			if nextEvent.Checkpoint != nil {
				r.catchupTimestamp = nextEvent.Checkpoint.ResolvedTS
			}
			if err := r.stream.Send(nextEvent); err != nil {
				return err
			}
		}
	}
}

// runCatchupScan starts a catchup scan which will output entries for all
// recorded changes in the replica that are newer than the catchupTimeStamp.
// This creates a single engine iterator immediately when this method
// is called, and outputs records directly to the registration stream.
func (r *registration) runCatchupScan() error {
	it := r.eng.NewIterator(engine.IterOptions{
		UpperBound:       r.span.EndKey,
		MinTimestampHint: r.catchupTimestamp,
	})
	defer it.Close()

	var a bufalloc.ByteAllocator
	startKey := engine.MakeMVCCMetadataKey(r.span.Key)
	endKey := engine.MakeMVCCMetadataKey(r.span.EndKey)

	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var meta enginepb.MVCCMetadata
	for it.Seek(startKey); ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok || !it.UnsafeKey().Less(endKey) {
			break
		}

		unsafeKey := it.UnsafeKey()
		unsafeVal := it.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}
			if !meta.IsInline() {
				// Not an inline value. Ignore.
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

		var event roachpb.RangeFeedEvent
		event.MustSetValue(&roachpb.RangeFeedValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes:  val,
				Timestamp: ts,
			},
		})
		if err := r.stream.Send(&event); err != nil {
			return err
		}
	}
	return nil
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
		// Always publish checkpoint notifications, regardless
		// of a registration's starting timestamp.
		minTS = hlc.MaxTimestamp
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", event))
	}

	reg.forOverlappingRegs(span, func(r *registration) (bool, *roachpb.Error) {
		if !r.catchupTimestamp.Less(minTS) {
			// Don't publish events if they are equal to or less
			// than the registration's starting timestamp.
			return false, nil
		}

		r.publish(event)
		return false, nil
	})
}

// DisconnectRegWithError disconnects a specific registration with a provided error.
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
