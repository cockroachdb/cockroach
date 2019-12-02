// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// routerOutput is an interface implemented by router outputs. It exists for
// easier test mocking of outputs.
type routerOutput interface {
	execinfra.OpNode
	// addBatch adds the elements specified by the selection vector from batch to
	// the output. It returns whether or not the output changed its state to
	// blocked (see implementations).
	addBatch(coldata.Batch, []uint16) bool
	// cancel tells the output to stop producing batches.
	cancel()
}

// defaultRouterOutputBlockedThreshold is the number of unread values buffered
// by the routerOutputOp after which the output is considered blocked.
var defaultRouterOutputBlockedThreshold = int(coldata.BatchSize() * 2)

type routerOutputOp struct {
	// input is a reference to our router.
	input execinfra.OpNode

	types []coltypes.T

	// unblockedEventsChan is signaled when a routerOutput changes state from
	// blocked to unblocked.
	unblockedEventsChan chan<- struct{}

	mu struct {
		syncutil.Mutex
		allocator *Allocator
		cond      *sync.Cond
		done      bool
		// TODO(asubiotto): Use a ring buffer once we have disk spilling.
		data      []coldata.Batch
		numUnread int
		blocked   bool
	}

	// These fields default to defaultRouterOutputBlockedThreshold and
	// coldata.BatchSize() but are modified by tests to test edge cases.
	// blockedThreshold is the number of buffered values above which we consider
	// a router output to be blocked.
	blockedThreshold int
	outputBatchSize  int
}

func (o *routerOutputOp) ChildCount() int {
	return 1
}

func (o *routerOutputOp) Child(nth int) execinfra.OpNode {
	if nth == 0 {
		return o.input
	}
	execerror.VectorizedInternalPanic(fmt.Sprintf("invalid index %d", nth))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

var _ Operator = &routerOutputOp{}

// newRouterOutputOp creates a new router output. The caller must ensure that
// unblockedEventsChan is a buffered channel, as the router output will write to
// it.
func newRouterOutputOp(
	allocator *Allocator, types []coltypes.T, unblockedEventsChan chan<- struct{},
) *routerOutputOp {
	return newRouterOutputOpWithBlockedThresholdAndBatchSize(
		allocator, types, unblockedEventsChan, defaultRouterOutputBlockedThreshold, int(coldata.BatchSize()),
	)
}

func newRouterOutputOpWithBlockedThresholdAndBatchSize(
	allocator *Allocator,
	types []coltypes.T,
	unblockedEventsChan chan<- struct{},
	blockedThreshold int,
	outputBatchSize int,
) *routerOutputOp {
	o := &routerOutputOp{
		types:               types,
		unblockedEventsChan: unblockedEventsChan,
		blockedThreshold:    blockedThreshold,
		outputBatchSize:     outputBatchSize,
	}
	o.mu.allocator = allocator
	o.mu.cond = sync.NewCond(&o.mu)
	o.mu.data = make([]coldata.Batch, 0, o.blockedThreshold/o.outputBatchSize)
	return o
}

func (o *routerOutputOp) Init() {}

// Next returns the next coldata.Batch from the routerOutputOp. Note that Next
// is designed for only one concurrent caller and will block until data is
// ready.
func (o *routerOutputOp) Next(context.Context) coldata.Batch {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.done {
		return coldata.ZeroBatch
	}
	for len(o.mu.data) == 0 && !o.mu.done {
		o.mu.cond.Wait()
	}
	if o.mu.done {
		return coldata.ZeroBatch
	}
	// Get the first batch and advance the start of the buffer.
	b := o.mu.data[0]
	o.mu.data = o.mu.data[1:]
	o.mu.numUnread -= int(b.Length())
	if o.mu.numUnread <= o.blockedThreshold {
		o.maybeUnblockLocked()
	}
	if b.Length() == 0 {
		// This is the last batch. Set done to protect against further calls to
		// Next since this is allowed by the interface.
		o.mu.done = true
	}
	return b
}

// cancel wakes up a reader in Next if there is one and results in the output
// returning zero length batches for every Next call after cancel. Note that
// all accumulated data that hasn't been read will not be returned.
func (o *routerOutputOp) cancel() {
	o.mu.Lock()
	o.mu.done = true
	// Release o.mu.data to GC.
	o.mu.data = nil
	// Some goroutine might be waiting on the condition variable, so wake it up.
	// Note that read goroutines check o.mu.done, so won't wait on the condition
	// variable after we unlock the mutex.
	o.mu.cond.Signal()
	o.mu.Unlock()
}

// addBatch copies the columns in batch according to selection into an internal
// buffer.
// The routerOutputOp only adds the elements specified by selection. Therefore,
// an empty selection slice will add no elements. Note that the selection vector
// on the batch is ignored. This is so that callers of addBatch can push the
// same batch with different selection vectors to many different outputs.
// True is returned if the the output changes state to blocked (note: if the
// output is already blocked, false is returned).
func (o *routerOutputOp) addBatch(batch coldata.Batch, selection []uint16) bool {
	if len(selection) > int(batch.Length()) {
		selection = selection[:batch.Length()]
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if batch.Length() == 0 {
		// End of data. o.mu.done will be set in Next.
		o.mu.data = append(o.mu.data, coldata.ZeroBatch)
		o.mu.cond.Signal()
		return false
	}

	if len(selection) == 0 {
		// Non-zero batch with no selection vector. Nothing to do.
		return false
	}

	writeIdx := 0
	if len(o.mu.data) == 0 {
		// New output batch.
		o.mu.data = append(o.mu.data, o.mu.allocator.NewMemBatchWithSize(o.types, o.outputBatchSize))
	} else {
		if int(o.mu.data[len(o.mu.data)-1].Length()) == o.outputBatchSize {
			// No space in last batch, append new output batch.
			o.mu.data = append(o.mu.data, o.mu.allocator.NewMemBatchWithSize(o.types, o.outputBatchSize))
		}
		writeIdx = len(o.mu.data) - 1
	}

	// Increment o.mu.numUnread before going into the loop, as we will consume
	// selection.
	o.mu.numUnread += len(selection)

	// Append the batch to o.mu.data. The batch at writeIdx is at most
	// o.outputBatchSize-1, so if all of the elements cannot be accommodated at
	// that index, spill over to the following batch.
	for toAppend := uint16(len(selection)); toAppend > 0; writeIdx++ {
		dst := o.mu.data[writeIdx]

		available := uint16(o.outputBatchSize) - dst.Length()
		numAppended := toAppend
		if toAppend > available {
			numAppended = available
			// Need to create a new batch to append to in the next o.mu.data slot.
			// This will be used in the next iteration.
			o.mu.data = append(o.mu.data, o.mu.allocator.NewMemBatchWithSize(o.types, o.outputBatchSize))
		}

		for i, t := range o.types {
			o.mu.allocator.Append(
				dst.ColVec(i),
				coldata.SliceArgs{
					ColType:   t,
					Src:       batch.ColVec(i),
					Sel:       selection,
					DestIdx:   uint64(dst.Length()),
					SrcEndIdx: uint64(len(selection)),
				},
			)
		}

		selection = selection[numAppended:]
		dst.SetLength(dst.Length() + numAppended)
		toAppend -= numAppended
	}

	stateChanged := false
	if o.mu.numUnread > o.blockedThreshold && !o.mu.blocked {
		// The output is now blocked.
		o.mu.blocked = true
		stateChanged = true
	}
	o.mu.cond.Signal()
	return stateChanged
}

// maybeUnblockLocked unblocks the router output if it is in a blocked state. If the
// output was previously in a blocked state, an event will be sent on
// routerOutputOp.unblockedEventsChan.
func (o *routerOutputOp) maybeUnblockLocked() {
	if o.mu.blocked {
		o.mu.blocked = false
		o.unblockedEventsChan <- struct{}{}
	}
}

// reset resets the routerOutputOp for a benchmark run.
func (o *routerOutputOp) reset() {
	o.mu.Lock()
	o.mu.done = false
	o.mu.data = o.mu.data[:0]
	o.mu.numUnread = 0
	o.mu.blocked = false
	o.mu.Unlock()
}

// HashRouter hashes values according to provided hash columns and computes a
// destination for each row. These destinations are exposed as Operators
// returned by the constructor.
type HashRouter struct {
	OneInputNode
	// types are the input coltypes.
	types []coltypes.T
	// ht is not fully initialized to a hashTable, only the utility methods are
	// used.
	ht hashTable
	// hashCols is a slice of indices of the columns used for hashing.
	hashCols []int

	// One output for each stream.
	outputs []routerOutput

	// unblockedEventsChan is a channel shared between the HashRouter and its
	// outputs. outputs send events on this channel when they are unblocked by a
	// read.
	unblockedEventsChan <-chan struct{}
	numBlockedOutputs   int

	mu struct {
		syncutil.Mutex
		bufferedMeta []execinfrapb.ProducerMetadata
	}

	scratch struct {
		// buckets is scratch space for the computed hash value of a group of columns
		// with the same index in the current coldata.Batch.
		buckets []uint64
		// selections is scratch space for selection vectors used by router outputs.
		selections [][]uint16
	}
}

// NewHashRouter creates a new hash router that consumes coldata.Batches from
// input and hashes each row according to hashCols to one of numOutputs outputs.
// These outputs are exposed as Operators.
func NewHashRouter(
	allocator *Allocator, input Operator, types []coltypes.T, hashCols []int, numOutputs int,
) (*HashRouter, []Operator) {
	outputs := make([]routerOutput, numOutputs)
	outputsAsOps := make([]Operator, numOutputs)
	// unblockEventsChan is buffered to 2*numOutputs as we don't want the outputs
	// writing to it to block.
	// Unblock events only happen after a corresponding block event. Since these
	// are state changes and are done under lock (including the output sending
	// on the channel, which is why we want the channel to be buffered in the
	// first place), every time the HashRouter blocks an output, it *must* read
	// all unblock events preceding it since these *must* be on the channel.
	unblockEventsChan := make(chan struct{}, 2*numOutputs)
	for i := 0; i < numOutputs; i++ {
		op := newRouterOutputOp(allocator, types, unblockEventsChan)
		outputs[i] = op
		outputsAsOps[i] = op
	}
	router := newHashRouterWithOutputs(input, types, hashCols, unblockEventsChan, outputs)
	for i := range outputs {
		outputs[i].(*routerOutputOp).input = router
	}
	return router, outputsAsOps
}

func newHashRouterWithOutputs(
	input Operator,
	types []coltypes.T,
	hashCols []int,
	unblockEventsChan <-chan struct{},
	outputs []routerOutput,
) *HashRouter {
	r := &HashRouter{
		OneInputNode:        NewOneInputNode(input),
		types:               types,
		hashCols:            hashCols,
		outputs:             outputs,
		unblockedEventsChan: unblockEventsChan,
	}
	r.scratch.buckets = make([]uint64, coldata.BatchSize())
	r.scratch.selections = make([][]uint16, len(outputs))
	for i := range r.scratch.selections {
		r.scratch.selections[i] = make([]uint16, 0, coldata.BatchSize())
	}
	return r
}

// Run runs the HashRouter. Batches are read from the input and pushed to an
// output calculated by hashing columns. Cancel the given context to terminate
// early.
func (r *HashRouter) Run(ctx context.Context) {
	r.input.Init()
	cancelOutputs := func(err error) {
		if err != nil {
			r.mu.Lock()
			r.mu.bufferedMeta = append(r.mu.bufferedMeta, execinfrapb.ProducerMetadata{Err: err})
			r.mu.Unlock()
		}
		for _, o := range r.outputs {
			o.cancel()
		}
	}
	var done bool
	processNextBatch := func() {
		done = r.processNextBatch(ctx)
	}
	for {
		// Check for cancellation.
		select {
		case <-ctx.Done():
			cancelOutputs(ctx.Err())
			return
		default:
		}

		// Read all the routerOutput state changes that have happened since the
		// last iteration.
		for moreToRead := true; moreToRead; {
			select {
			case <-r.unblockedEventsChan:
				r.numBlockedOutputs--
			default:
				// No more routerOutput state changes to read without blocking.
				moreToRead = false
			}
		}

		if r.numBlockedOutputs == len(r.outputs) {
			// All outputs are blocked, wait until at least one output is unblocked.
			select {
			case <-r.unblockedEventsChan:
				r.numBlockedOutputs--
			case <-ctx.Done():
				cancelOutputs(ctx.Err())
				return
			}
		}

		if err := execerror.CatchVectorizedRuntimeError(processNextBatch); err != nil {
			cancelOutputs(err)
			return
		}
		if done {
			// The input was done and we have notified the routerOutputs that there
			// is no more data.
			return
		}
	}
}

// processNextBatch reads the next batch from its input, hashes it and adds
// each column to its corresponding output, returning whether the input is
// done.
func (r *HashRouter) processNextBatch(ctx context.Context) bool {
	r.ht.initHash(r.scratch.buckets, uint64(len(r.scratch.buckets)))
	b := r.input.Next(ctx)
	if b.Length() == 0 {
		// Done. Push an empty batch to outputs to tell them the data is done as
		// well.
		for _, o := range r.outputs {
			o.addBatch(b, nil)
		}
		return true
	}

	for _, i := range r.hashCols {
		r.ht.rehash(ctx, r.scratch.buckets, i, r.types[i], b.ColVec(i), uint64(b.Length()), b.Selection())
	}

	// Reset selections.
	for i := 0; i < len(r.outputs); i++ {
		r.scratch.selections[i] = r.scratch.selections[i][:0]
	}

	// finalizeHash has an assumption that bucketSize is a power of 2, so
	// finalize the hash in our own way. While doing this, we will build a
	// selection vector for each output.
	selection := b.Selection()
	if selection != nil {
		selection = selection[:b.Length()]
		for i, selIdx := range selection {
			outputIdx := r.scratch.buckets[i] % uint64(len(r.outputs))
			r.scratch.selections[outputIdx] = append(r.scratch.selections[outputIdx], selIdx)
		}
	} else {
		for i, hash := range r.scratch.buckets[:b.Length()] {
			outputIdx := hash % uint64(len(r.outputs))
			r.scratch.selections[outputIdx] = append(r.scratch.selections[outputIdx], uint16(i))
		}
	}

	for i, o := range r.outputs {
		if o.addBatch(b, r.scratch.selections[i]) {
			// This batch blocked the output.
			r.numBlockedOutputs++
		}
	}
	return false
}

// reset resets the HashRouter for a benchmark run.
func (r *HashRouter) reset() {
	if i, ok := r.input.(resetter); ok {
		i.reset()
	}
	r.numBlockedOutputs = 0
	for moreToRead := true; moreToRead; {
		select {
		case <-r.unblockedEventsChan:
		default:
			moreToRead = false
		}
	}
	for _, o := range r.outputs {
		o.(resetter).reset()
	}
}

// DrainMeta is part of the MetadataGenerator interface.
func (r *HashRouter) DrainMeta(ctx context.Context) []execinfrapb.ProducerMetadata {
	r.mu.Lock()
	defer r.mu.Unlock()
	meta := r.mu.bufferedMeta
	r.mu.bufferedMeta = r.mu.bufferedMeta[:0]
	return meta
}
