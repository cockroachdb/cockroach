// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracker

import "github.com/cockroachdb/cockroach/pkg/util/container/ring"

// inflight describes an in-flight MsgApp message.
type inflight struct {
	index uint64 // the index of the last entry inside the message
	bytes uint64 // the total byte size of the entries in the message
}

// Inflights limits the number of MsgApp (represented by the largest index
// contained within) sent to followers but not yet acknowledged by them. Callers
// use Full() to check whether more messages can be sent, call Add() whenever
// they are sending a new append, and release "quota" via FreeLE() whenever an
// ack is received.
type Inflights struct {
	bytes uint64 // number of inflight bytes

	// TODO(pav-kv): do not store the limits here, pass them to methods. For flow
	// control, we need to support dynamic limits.
	size     int    // the max number of inflight messages
	maxBytes uint64 // the max total byte size of inflight messages

	// buffer is a ring buffer containing info about all in-flight messages.
	buffer ring.Buffer[inflight]
}

// NewInflights sets up an Inflights that allows up to size inflight messages,
// with the total byte size up to maxBytes. If maxBytes is 0 then there is no
// byte size limit. The maxBytes limit is soft, i.e. we accept a single message
// that brings it from size < maxBytes to size >= maxBytes.
func NewInflights(size int, maxBytes uint64) *Inflights {
	return &Inflights{
		size:     size,
		maxBytes: maxBytes,
	}
}

// Clone returns an *Inflights that is identical to but shares no memory with
// the receiver.
func (in *Inflights) Clone() *Inflights {
	ins := *in
	ins.buffer = in.buffer.Clone()
	return &ins
}

// Add notifies the Inflights that a new message with the given index and byte
// size is being dispatched. Consecutive calls to Add() must provide a monotonic
// sequence of log indices.
//
// The caller should check that the tracker is not Full(), before calling Add().
// If the tracker is full, the caller should hold off sending entries to the
// peer. However, Add() is still valid and allowed, for cases when this pacing
// is implemented at the higher app level. The tracker correctly tracks all the
// in-flight entries.
func (in *Inflights) Add(index, bytes uint64) {
	in.buffer.Push(inflight{index: index, bytes: bytes})
	in.bytes += bytes
}

// FreeLE frees the inflights smaller or equal to the given `to` flight.
func (in *Inflights) FreeLE(to uint64) {
	n := in.buffer.Length()
	if n == 0 || to < in.buffer.At(0).index {
		// out of the left side of the window
		return
	}

	var i int
	var bytes uint64
	for i = 0; i < n; i++ {
		e := in.buffer.At(i)
		if to < e.index { // found the first large inflight
			break
		}
		bytes += e.bytes
	}
	// free i inflights and set new start index
	in.buffer.Pop(i)
	in.bytes -= bytes
}

// Full returns true if no more messages can be sent at the moment.
func (in *Inflights) Full() bool {
	return in.buffer.Length() >= in.size || (in.maxBytes != 0 && in.bytes >= in.maxBytes)
}

// Count returns the number of inflight messages.
func (in *Inflights) Count() int { return in.buffer.Length() }

// reset frees all inflights.
func (in *Inflights) reset() {
	in.buffer.ShrinkToPrefix(0)
	in.bytes = 0
}
