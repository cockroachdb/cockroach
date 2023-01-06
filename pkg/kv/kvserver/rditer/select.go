// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rditer

import "github.com/cockroachdb/cockroach/pkg/roachpb"

type StateMachineSelectionOptions struct {
	// ReplicatedBySpan selects all replicated key Spans that are keyed by a user
	// key. This includes user keys, range descriptors, and locks (separated
	// intents).
	ReplicatedBySpan roachpb.RSpan
	// ReplicatedByRangeID selects all RangeID-keyed replicated keys. An example
	// of a key that falls into this Span is the GCThresholdKey.
	ReplicatedByRangeID bool
}

// SelectionOptions configures which spans for a Replica to return from Select.
// A Replica comprises replicated (i.e. belonging to the state machine) spans
// and unreplicated spans, and depending on external circumstances one may want
// to read or erase only certain components of a Replica.
type SelectionOptions struct {
	StateMachineSelectionOptions
	// ReplicatedByRangeID selects all RangeID-keyed unreplicated keys. Examples
	// of keys that fall into this Span are the HardStateKey (and generally all
	// Raft state) and the RangeTombstoneKey.
	UnreplicatedByRangeID bool
}

// A Selection is a collection of Select describing a part of a Replica.
// Which fields are populated depends on the SelectionOptions passed to
// Select.
//
// TODO(during review): write a comprehensive test for this. There was some method
// in the tests for this package that attempts to create an interesting data set,
// but we should "somehow" make sure this data set is always complete, i.e. contains
// all possible kinds of keys even as new ones are being added.
type Selection struct {
	// The following slices may share backing memory.
	statemachine []roachpb.Span
	other        []roachpb.Span
	all          []roachpb.Span
}

// StateMachineSpans returns the Spans in the Selection that refer to the state
// machine (i.e. replicated keys).
func (s Selection) StateMachineSpans() []roachpb.Span {
	return s.statemachine
}

// Spans returns the StateMachineSpans and NonStateMachineSpans in one slice,
// making sure to order them correctly.
func (s Selection) Spans() []roachpb.Span {
	return s.all
}

// NonStateMachineSpans returns all spans in the Selection that are not StateMachineSpans().
func (s Selection) NonStateMachineSpans() []roachpb.Span {
	return s.other
}

// Select creates a Selection according to the SelectionOptions.
func Select(rangeID roachpb.RangeID, opts SelectionOptions) Selection {
	var s Selection

	if opts.ReplicatedByRangeID {
		sp := makeRangeIDReplicatedSpan(rangeID)
		s.all = append(s.all, sp)
		s.statemachine = append(s.statemachine, sp)
	}

	if opts.UnreplicatedByRangeID {
		sp := makeRangeIDUnreplicatedSpan(rangeID)
		s.all = append(s.all, sp)
		s.other = append(s.other, sp)
	}

	if in := opts.ReplicatedBySpan; !in.Equal(roachpb.RSpan{}) {
		d := &roachpb.RangeDescriptor{StartKey: in.Key, EndKey: in.EndKey}
		{
			sp := makeRangeLocalKeySpan(d)
			s.all = append(s.all, sp)
			s.statemachine = append(s.statemachine, sp)
		}
		{
			sps := makeRangeLockTableKeySpans(d)
			s.all = append(s.all, sps[:]...)
			s.statemachine = append(s.all, sps[:]...)
		}
		{
			sp := MakeUserKeySpan(d)
			s.all = append(s.all, sp)
			s.statemachine = append(s.statemachine, sp)
		}
	}
	return s
}
