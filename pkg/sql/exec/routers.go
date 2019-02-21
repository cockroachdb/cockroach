// Copyright 2019 The Cockroach Authors.
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

package exec

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type routerOutput interface {
	addBatch(coldata.Batch, []uint16)
	cancel()
}

// defaultRouterOutputBlockedThreshold is the number of unread values bufferred
// by the routerOutputOp after which the output is considered blocked.
const defaultRouterOutputBlockedThreshold = coldata.BatchSize * 2

type routerOutputOp struct {
	types []types.T
	// zeroBatch is used to return a 0 length batch in some cases.
	zeroBatch coldata.Batch

	// blockedStateChangeChan is signalled when a routerOuput changes state from
	// to blocked or back again. true will be sent in the former case and false in
	// the latter case.

	blockedStateChangeChan chan<- bool

	mu struct {
		syncutil.Mutex
		cond *sync.Cond
		done bool
		// TODO(asubiotto): Use a ring buffer once we have disk spilling.
		data      []coldata.Batch
		numUnread int
		blocked   bool
	}

	// These fields default to defaultRouterOutputBlockedThreshold and
	// coldata.BatchSize but are modified by tests to test edge cases.
	// blockedThreshold is the number of buffered values above which we consider
	// a router output to be blocked.
	blockedThreshold int
	outputBatchSize  int
}

var _ Operator = &routerOutputOp{}

// newRouterOutputOp creates a new router output. The caller must ensure that
// blockedStateChangeChan is a buffered channel, as the router output will
// write to it.
func newRouterOutputOp(types []types.T, blockedStateChangeChan chan<- bool) *routerOutputOp {
	return newRouterOutputOpWithBlockedThresholdAndBatchSize(
		types, blockedStateChangeChan, defaultRouterOutputBlockedThreshold, coldata.BatchSize,
	)
}

func newRouterOutputOpWithBlockedThresholdAndBatchSize(
	types []types.T, blockedStateChangeChan chan<- bool, blockedThreshold int, outputBatchSize int,
) *routerOutputOp {
	o := &routerOutputOp{
		types:                  types,
		zeroBatch:              coldata.NewMemBatchWithSize(types, 0 /* size */),
		blockedStateChangeChan: blockedStateChangeChan,
		blockedThreshold:       blockedThreshold,
		outputBatchSize:        outputBatchSize,
	}
	o.zeroBatch.SetLength(0)
	o.mu.cond = sync.NewCond(&o.mu)
	o.mu.data = make([]coldata.Batch, 0, o.blockedThreshold/o.outputBatchSize)
	return o
}

func (o *routerOutputOp) Init() {}

// Do we really want to implement cancellation by

// Next gets the next coldata.Batch. Next blocks indefinitely until data is ready or
// the parent router cancels Next.

// Next returns the next coldata.Batch from the routerOutputOp. Note that Next is
// designed for only one concurrent caller.
func (o *routerOutputOp) Next() coldata.Batch {
	o.mu.Lock()
	if o.mu.done {
		o.mu.Unlock()
		return o.zeroBatch
	}
	for len(o.mu.data) == 0 && !o.mu.done {
		o.mu.cond.Wait()
	}
	if o.mu.done {
		o.mu.Unlock()
		return o.zeroBatch
	}
	// Get the first batch and advance the start of the buffer.
	b := o.mu.data[0]
	o.mu.data = o.mu.data[1:]
	o.mu.numUnread -= int(b.Length())
	if o.mu.numUnread <= o.blockedThreshold {
		o.maybeSetBlockedLocked(false)
	}
	if b.Length() == 0 {
		// This is the last batch. Set done to protect against further calls to
		// Next since this is allowed by the interface.
		o.mu.done = true
	}
	o.mu.Unlock()
	return b
}

func (o *routerOutputOp) cancel() {
	o.mu.Lock()
	o.mu.done = true
	// Some goroutine might be waiting on the condition variable, so wake it up.
	// Note that read goroutines check o.mu.done, so won't wait on the condition
	// variable after we unlock the mutex.
	o.mu.cond.Signal()
	o.mu.Unlock()
}

// addBatch copies the columns in batch according to selection into an internal
// buffer. Note that an empty selection slice will add nothing. The selection
// slice is assumed to be at most as big as the batch.
func (o *routerOutputOp) addBatch(batch coldata.Batch, selection []uint16) {
	o.mu.Lock()
	if batch.Length() == 0 {
		// End of data. o.mu.done will be set in Next.
		o.mu.data = append(o.mu.data, o.zeroBatch)
		o.mu.cond.Signal()
		o.mu.Unlock()
		return
	}

	if len(selection) == 0 {
		// Non-zero batch with no selection vector. Nothing to do.
		o.mu.Unlock()
		return
	}

	writeIdx := 0
	if len(o.mu.data) == 0 {
		// New output coldata.Batch.
		o.mu.data = append(o.mu.data, coldata.NewMemBatchWithSize(o.types, o.outputBatchSize))
	} else {
		if int(o.mu.data[len(o.mu.data)-1].Length()) == o.outputBatchSize {
			// No space in last coldata.Batch, append new output ColBatch.
			o.mu.data = append(o.mu.data, coldata.NewMemBatchWithSize(o.types, o.outputBatchSize))
		}
		writeIdx = len(o.mu.data) - 1
	}

	// Increment o.mu.numUnread before going into the loop, as we will consume
	// selection.
	o.mu.numUnread += len(selection)

	// Append the batch to o.mu.data. Each index will have a batch of at most
	// o.outputBatchSize-1, so if the batch at writeIdx is too large to
	// accommodate all of the elements, spill over to the following batch.
	for toAppend := uint16(len(selection)); toAppend > 0; writeIdx++ {
		dst := o.mu.data[writeIdx]

		available := uint16(o.outputBatchSize) - dst.Length()
		numAppended := toAppend
		if toAppend > available {
			numAppended = available
			// Need to create a new batch to append to in the next o.mu.data slot.
			// This will be used in the next iteration.
			o.mu.data = append(o.mu.data, coldata.NewMemBatchWithSize(o.types, o.outputBatchSize))
		}

		for i, t := range o.types {
			dst.ColVec(i).AppendWithSel(batch.ColVec(i), selection, numAppended, t, uint64(dst.Length()))
		}

		selection = selection[numAppended:]
		dst.SetLength(dst.Length() + numAppended)
		toAppend -= numAppended
	}

	if o.mu.numUnread > o.blockedThreshold {
		o.maybeSetBlockedLocked(true)
	}
	o.mu.cond.Signal()
	o.mu.Unlock()
}

func (o *routerOutputOp) maybeSetBlockedLocked(blocked bool) {
	if o.mu.blocked != blocked {
		o.mu.blocked = blocked
		o.blockedStateChangeChan <- blocked
	}
}

type hashRouter struct {
	input Operator
	// types are the input types.
	types []types.T
	// ht is not fully initialized to a hashTable, only the utility methods are
	// used.
	ht hashTable
	// hashCols is a slice of indices of the columns used for hashing.
	hashCols []int

	// One output for each stream.
	outputs []routerOutput
	// TODO(asubiotto): Comment semantics of channel, including maximum size.
	blockedStateChangeChan <-chan bool
	numBlockedOutputs      int

	scratch struct {
		// buckets is scratch space for the computed hash value of a group of columns
		// with the same index in the current coldata.Batch.
		buckets []uint64
		// selections is scratch space for selection vectors used by router outputs.
		selections [][]uint16
	}
}

// NewHashRouter creates a new hash router that consumes coldata.Batches from input
// and hashes each row according to hashCols to one of numOutputs outputs. These
// outputs are exposed as Operators.
// TODO(asubiotto): Rephrase this comment?
func NewHashRouter(
	input Operator, types []types.T, hashCols []int, numOutputs int,
) (*hashRouter, []Operator) {
	outputs := make([]routerOutput, numOutputs)
	outputsAsOps := make([]Operator, numOutputs)
	// TODO(asubiotto): Explain buffer size.
	ch := make(chan bool, 2*numOutputs)
	for i := 0; i < numOutputs; i++ {
		op := newRouterOutputOp(types, ch)
		outputs[i] = op
		outputsAsOps[i] = op
	}
	return newHashRouterWithOutputs(input, types, hashCols, ch, outputs), outputsAsOps
}

func newHashRouterWithOutputs(
	input Operator, types []types.T, hashCols []int, ch <-chan bool, outputs []routerOutput,
) *hashRouter {
	r := &hashRouter{
		input:                  input,
		types:                  types,
		hashCols:               hashCols,
		outputs:                outputs,
		blockedStateChangeChan: ch,
	}
	r.scratch.buckets = make([]uint64, coldata.BatchSize)
	r.scratch.selections = make([][]uint16, len(outputs))
	for i := range r.scratch.selections {
		// Save some space by only preallocating coldata.BatchSize/numOutputs, since we
		// should be getting even distribution.
		r.scratch.selections[i] = make([]uint16, 0, coldata.BatchSize/len(outputs))
	}
	return r
}

func (r *hashRouter) run(ctx context.Context) {
	cancelOutputs := func() {
		for _, o := range r.outputs {
			o.cancel()
		}
	}
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			cancelOutputs()
			return
		default:
		}

		// Read all the routerOutput state changes that have happened since the
		// last iteration.
		moreToRead := true
		for moreToRead {
			select {
			case blocked := <-r.blockedStateChangeChan:
				if blocked {
					r.numBlockedOutputs++
				} else {
					r.numBlockedOutputs--
				}
			default:
				// No more routerOutput state changes to read without blocking.
				moreToRead = false
			}
		}

		if r.numBlockedOutputs == len(r.outputs) {
			// All outputs are blocked, wait until at least one output is unblocked.
			select {
			case <-r.blockedStateChangeChan:
				// Note that the only value that can be sent over the channel is when
				// a read has happened that unblocks the channel, as this goroutine
				// is the only producer.
				r.numBlockedOutputs--
			case <-ctx.Done():
				cancelOutputs()
				return
			}
		}

		if done := r.processNextBatch(); done {
			// The input was done and we have notified the routerOutputs that there
			// is no more data.
			return
		}
	}
}

// processNextBatch reads the next batch from its input, hashes it and adds each
// column to its corresponding output, returning whether the input has any more
// batches to process.
func (r *hashRouter) processNextBatch() bool {
	r.ht.initHash(r.scratch.buckets, uint64(len(r.scratch.buckets)))
	b := r.input.Next()
	if b.Length() == 0 {
		// Done.
		return true
	}

	for _, i := range r.hashCols {
		r.ht.rehash(r.scratch.buckets, i, r.types[i], b.ColVec(i), uint64(len(r.scratch.buckets)), b.Selection())
	}

	// Reset selections.
	for i := 0; i < len(r.outputs); i++ {
		r.scratch.selections[i] = r.scratch.selections[i][:0]
	}

	// finalizeHash has an assumption that bucketSize is a power of 2, so
	// finalize the hash in our own way. While doing this, we will build a
	// selection vector for each output.
	// TODO(asubiotto): I think we need to care here about the selection on the
	// batch and reflect what it has wrt e.g. reordering. Test to see what to do.
	for i, hash := range r.scratch.buckets {
		outputIdx := hash % uint64(len(r.outputs))
		r.scratch.selections[outputIdx] = append(r.scratch.selections[outputIdx], uint16(i))
	}

	for i, o := range r.outputs {
		o.addBatch(b, r.scratch.selections[i])
	}
	return false
}
