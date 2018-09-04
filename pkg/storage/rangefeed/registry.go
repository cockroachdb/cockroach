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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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
	id   int64
	keys interval.Range

	// Footprint.
	span    roachpb.Span
	startTS hlc.Timestamp // exclusive

	// Catch-up state.
	catchUpIter engine.SimpleIterator
	caughtUp    bool

	// Output.
	stream Stream
	errC   chan<- *roachpb.Error
}

// ID implements interval.Interface.
func (r *registration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration) Range() interval.Range {
	return r.keys
}

func (r *registration) SetCaughtUp() {
	r.caughtUp = true
}

func (r registration) String() string {
	return fmt.Sprintf("[%s @ %s+]", r.span, r.startTS)
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

// PublishToReg publishes the provided event to the given registration. No
// validation of whether the registration state is compatible with the event
// is performed.
func (reg *registry) PublishToReg(r *registration, event *roachpb.RangeFeedEvent) {
	if err := r.stream.Send(event); err != nil {
		reg.DisconnectRegWithError(r, roachpb.NewError(err))
	}
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *registry) PublishToOverlapping(span roachpb.Span, event *roachpb.RangeFeedEvent) {
	// Determine the earliest starting timestamp that a registration
	// can have while still needing to hear about this event.
	var minTS hlc.Timestamp
	var requireCaughtUp bool
	switch t := event.GetValue().(type) {
	case *roachpb.RangeFeedValue:
		// Only publish values to registrations with starting
		// timestamps equal to or greater than the value's timestamp.
		minTS = t.Value.Timestamp
	case *roachpb.RangeFeedCheckpoint:
		// Always publish checkpoint notifications, regardless
		// of a registration's starting timestamp.
		minTS = hlc.MaxTimestamp
		requireCaughtUp = true
	default:
		panic(fmt.Sprintf("unexpected RangeFeedEvent variant: %v", event))
	}

	reg.forOverlappingRegs(span, func(r *registration) (bool, *roachpb.Error) {
		if !r.startTS.Less(minTS) {
			// Don't publish events if they are equal to or less
			// than the registration's starting timestamp.
			return false, nil
		}
		if requireCaughtUp && !r.caughtUp {
			// Don't publish event if it requires the registration
			// to be caught up and this one is not.
			return false, nil
		}

		err := r.stream.Send(event)
		return err != nil, roachpb.NewError(err)
	})
}

// DisconnectRegWithError disconnects a specific registration with a provided error.
func (reg *registry) DisconnectRegWithError(r *registration, pErr *roachpb.Error) {
	r.errC <- pErr
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

// CheckStreams checks the context of all streams in the registry and
// unregisters any that have already disconnected.
func (reg *registry) CheckStreams() {
	reg.forOverlappingRegs(all, func(reg *registration) (bool, *roachpb.Error) {
		err := errors.Wrap(reg.stream.Context().Err(), "check streams")
		return err != nil, roachpb.NewError(err)
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
			r.errC <- pErr
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
