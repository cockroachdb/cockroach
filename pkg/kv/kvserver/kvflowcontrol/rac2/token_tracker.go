// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Tracker tracks flow token deductions for a replicaSendStream. Tokens are
// deducted for an in-flight log entry identified by its raft log index and
// term with a given RaftPriority.
type Tracker struct {
	// tracked entries are stored in increasing order of (term, index), per
	// priority.
	tracked [raftpb.NumPriorities]CircularBuffer[tracked]

	stream kvflowcontrol.Stream // used for logging only
}

// tracked represents tracked flow tokens; they're tracked with respect to a
// raft log index and term.
type tracked struct {
	tokens      kvflowcontrol.Tokens
	index, term uint64
}

func (t *Tracker) Init(stream kvflowcontrol.Stream) {
	*t = Tracker{
		tracked: [raftpb.NumPriorities]CircularBuffer[tracked]{},
		stream:  stream,
	}
}

func (t *Tracker) Empty() bool {
	// TODO(pav-kv): can optimize this loop out if needed. We can maintain the
	// total number of tokens held, and return whether it's zero. It's also
	// possible to make it atomic and avoid locking the mutex in replicaSendStream
	// when calling this.
	for pri := range t.tracked {
		if t.tracked[pri].Length() != 0 {
			return false
		}
	}
	return true
}

// Track token deductions of the given priority with the given raft log index and term.
func (t *Tracker) Track(
	ctx context.Context, term uint64, index uint64, pri raftpb.Priority, tokens kvflowcontrol.Tokens,
) bool {
	if length := t.tracked[pri].Length(); length >= 1 {
		last := t.tracked[pri].At(length - 1)
		// Tracker exists in the context of a single replicaSendStream, which cannot
		// span the leader losing leadership and regaining it. So the indices must
		// advance.
		if last.index >= index {
			log.Fatalf(ctx, "expected in order tracked log indexes (%d < %d)",
				last.index, index)
			return false
		}
		if last.term > term {
			log.Fatalf(ctx, "expected in order tracked leader terms (%d < %d)",
				last.term, term)
			return false
		}
	}

	t.tracked[pri].Push(tracked{
		tokens: tokens,
		index:  index,
		term:   term,
	})

	if log.V(1) {
		log.Infof(ctx, "tracking %v flow control tokens for pri=%s stream=%s log-position=%d/%d",
			tokens, pri, t.stream, term, index)
	}
	return true
}

// Untrack all token deductions of the given priority that have indexes less
// than or equal to the one provided, per priority, and terms less than or
// equal to the leader term. evalTokensGEIndex is used to separately count the
// untracked (eval) tokens that are for indices >= evalTokensGEIndex.
func (t *Tracker) Untrack(
	term uint64, admitted [raftpb.NumPriorities]uint64, evalTokensGEIndex uint64,
) (returnedSend, returnedEval [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	for pri := range admitted {
		uptoIndex := admitted[pri]
		var untracked int
		for n := t.tracked[pri].Length(); untracked < n; untracked++ {
			deduction := t.tracked[pri].At(untracked)
			if deduction.term > term || (deduction.term == term && deduction.index > uptoIndex) {
				break
			}
			returnedSend[pri] += deduction.tokens
			if deduction.index >= evalTokensGEIndex {
				returnedEval[pri] += deduction.tokens
			}
		}
		if untracked > 0 {
			t.tracked[pri].Pop(untracked)
		}
	}

	return returnedSend, returnedEval
}

// UntrackAll iterates through all tracked token deductions, untracking all of them
// and returning the sum of tokens for each priority.
func (t *Tracker) UntrackAll() (returned [raftpb.NumPriorities]kvflowcontrol.Tokens) {
	for pri := range t.tracked {
		n := t.tracked[pri].Length()
		for i := 0; i < n; i++ {
			returned[pri] += t.tracked[pri].At(i).tokens
		}
	}
	t.tracked = [raftpb.NumPriorities]CircularBuffer[tracked]{}

	return returned
}

// Inspect returns a snapshot of all tracked token deductions. It's used to
// power /inspectz-style debugging pages.
func (t *Tracker) Inspect() []kvflowinspectpb.TrackedDeduction {
	var res []kvflowinspectpb.TrackedDeduction
	for pri := range t.tracked {
		n := t.tracked[pri].Length()
		for i := 0; i < n; i++ {
			deduction := t.tracked[pri].At(i)
			res = append(res, kvflowinspectpb.TrackedDeduction{
				Tokens: int64(deduction.tokens),
				RaftLogPosition: kvflowcontrolpb.RaftLogPosition{
					Index: deduction.index,
					Term:  deduction.term,
				},
				Priority: int32(RaftToAdmissionPriority(raftpb.Priority(pri))),
			})
		}
	}
	return res
}

// CircularBuffer provides functionality akin to a []T, for cases that usually
// push to the back and pop from the front, and would like to reduce
// allocations. The assumption made here is that the capacity needed for
// holding the live entries is somewhat stable. The circular buffer will grow
// as needed, and over time will shrink. Liveness of shrinking depends on new
// entries being pushed.
type CircularBuffer[T any] struct {
	// len(buf) == cap(buf) == cap.
	buf []T
	cap int
	// first is in [0, cap).
	first int
	// len is in [0, cap].
	len int
	// pushesSinceCheck counts the number of a push calls since the last time we
	// considered shrinking the buffer.
	pushesSinceCheck int
	// maxObservedLen is the maximum observed len value since the last time we
	// considered shrinking the buffer.
	maxObservedLen int
}

// Push is used to push an entry onto the end.
func (cb *CircularBuffer[T]) Push(a T) {
	needed := cb.len + 1
	const minSize = 40
	if needed > cb.cap {
		size := cb.cap*2 + 1
		if size < minSize {
			size = minSize
		}
		cb.replace(size)
	} else if cb.pushesSinceCheck > 10*cb.cap {
		cb.pushesSinceCheck = 0
		cb.maxObservedLen = 0
		if cb.cap > 2*minSize && cb.cap > 3*cb.maxObservedLen && cb.cap/2 > needed {
			// Shrink.
			size := cb.cap / 2
			cb.replace(size)
		}
	}
	cb.buf[(cb.first+cb.len)%cb.cap] = a
	cb.len++
	cb.pushesSinceCheck++
	if cb.maxObservedLen < cb.len {
		cb.maxObservedLen = cb.len
	}
}

// Pop removes the first num entries.
func (cb *CircularBuffer[T]) Pop(num int) {
	if num == 0 {
		return
	}
	cb.len -= num
	cb.first = (cb.first + num) % cb.cap
}

// Prefix shrinks the buffer to retain the first num entries.
func (cb *CircularBuffer[T]) Prefix(num int) {
	cb.len = num
}

// At returns the entry at index.
func (cb *CircularBuffer[T]) At(index int) T {
	return cb.buf[(cb.first+index)%cb.cap]
}

// SetLast overwrites the last entry.
func (cb *CircularBuffer[T]) SetLast(a T) {
	cb.buf[(cb.first+cb.len-1)%cb.cap] = a
}

// SetFirst overwrites the first entry.
func (cb *CircularBuffer[T]) SetFirst(a T) {
	cb.buf[cb.first] = a
}

// Length returns the current length.
func (cb *CircularBuffer[T]) Length() int {
	return cb.len
}

func (cb *CircularBuffer[T]) replace(size int) {
	buf := make([]T, size)
	for i := 0; i < cb.len; i++ {
		buf[i] = cb.buf[(cb.first+i)%cb.cap]
	}
	cb.first = 0
	cb.cap = size
	cb.buf = buf
}
