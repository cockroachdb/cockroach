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

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

// Stream is a object capable of transmitting RangeFeedEvents.
type Stream interface {
	Context() context.Context
	Send(*roachpb.RangeFeedEvent) error
}

// registration is an instance of a rangefeed subscriber who has registered to
// receive updates for a specified range of keys.
type registration struct {
	id     int64
	keys   interval.Range
	stream Stream
	errC   chan<- error
}

// ID implements interval.Interface.
func (r *registration) ID() uintptr {
	return uintptr(r.id)
}

// Range implements interval.Interface.
func (r *registration) Range() interval.Range {
	return r.keys
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
func (reg *registry) Register(r registration) {
	r.id = reg.nextID()
	reg.tree.Insert(&r, false /* fast */)
}

func (reg *registry) nextID() int64 {
	reg.idAlloc++
	return reg.idAlloc
}

// PublishToOverlapping publishes the provided event to all registrations whose
// range overlaps the specified span.
func (reg *registry) PublishToOverlapping(span roachpb.Span, event *roachpb.RangeFeedEvent) {
	reg.forOverlappingRegs(span, func(r *registration) error {
		return r.stream.Send(event)
	})
}

// Disconnect disconnects all registrations that overlap the specified span with
// a nil error.
func (reg *registry) Disconnect(span roachpb.Span) {
	reg.DisconnectWithErr(span, nil /* err */)
}

// DisconnectWithErr disconnects all registrations that overlap the specified
// span with the provided error.
func (reg *registry) DisconnectWithErr(span roachpb.Span, err error) {
	reg.forOverlappingRegs(span, func(_ *registration) error {
		return err
	})
}

// CheckStreams checks the context of all streams in the registry and
// unregisters any that have already disconnected.
func (reg *registry) CheckStreams() {
	reg.forOverlappingRegs(all, func(reg *registration) error {
		return errors.Wrap(reg.stream.Context().Err(), "check streams")
	})
}

// all is a span that overlaps with all registrations.
var all = roachpb.Span{Key: roachpb.KeyMin, EndKey: roachpb.KeyMax}

func (reg *registry) forOverlappingRegs(span roachpb.Span, fn func(*registration) error) {
	var toDelete []interval.Interface
	reg.tree.DoMatching(
		func(i interval.Interface) (done bool) {
			r := i.(*registration)
			err := fn(r)
			if err != nil {
				r.errC <- err
				toDelete = append(toDelete, i)
			}
			return false
		},
		span.AsRange(),
	)
	if len(toDelete) > 0 {
		for _, i := range toDelete {
			reg.tree.Delete(i, true /* fast */)
		}
		reg.tree.AdjustRanges()
	}
}
