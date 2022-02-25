// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// resultsBuffer encapsulates the logic of handling the Results created by the
// asynchronous requests. The implementations are concurrency-safe.
type resultsBuffer interface {
	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//    Methods that should be called by the Streamer's user goroutine.    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// init prepares the buffer for the next batch of responses.
	// numExpectedResponses specifies how many complete responses are expected
	// to be added to the buffer.
	//
	// It will return an error if:
	// - the buffer hasn't seen all of the complete responses since the previous
	// init() call (i.e. no pipelining is allowed);
	// - the buffer is not empty (i.e. not all results have been retrieved);
	// - there are some unreleased results.
	init(_ context.Context, numExpectedResponses int) error

	// get returns all the Results that the buffer can send to the client at the
	// moment. The boolean indicates whether all expected Results have been
	// returned.
	get(context.Context) (_ []Result, allComplete bool, _ error)

	// wait blocks until there is at least one Result available to be returned
	// to the client.
	wait()

	// releaseOne decrements the number of unreleased Results by one.
	releaseOne()

	// close releases all of the resources associated with the buffer.
	close(context.Context)

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//            Methods that should be called by the goroutines            //
	//            evaluating the requests asynchronously.                    //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// add adds the provided Results into the buffer. If any Results are
	// available to be returned to the client and there is a goroutine blocked
	// in wait(), the goroutine is woken up.
	add([]Result)

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//   Methods that should be called by the worker coordinator goroutine.  //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// error returns the first error encountered by the buffer.
	error() error

	///////////////////////////////////////////////////////////////////////////
	//                                                                       //
	//              Methods that can be called by any goroutine.             //
	//                                                                       //
	///////////////////////////////////////////////////////////////////////////

	// numUnreleased returns the number of unreleased Results.
	numUnreleased() int
	// setError sets the error on the buffer (if it hasn't been previously set)
	// and wakes up a goroutine if there is one blocked in wait().
	setError(error)
}

type resultsBufferBase struct {
	budget *budget
	syncutil.Mutex
	// numExpectedResponses tracks the number of complete responses that the
	// results buffer expects to process until the next call to init().
	numExpectedResponses int
	// numCompleteResponses tracks the number of complete Results added into the
	// buffer since the last init().
	numCompleteResponses int
	// numUnreleasedResults tracks the number of Results that have already been
	// created but haven't been Release()'d yet.
	numUnreleasedResults int
	// hasResults is used in wait() to block until there are some results to be
	// picked up.
	hasResults chan struct{}
	err        error
}

func (b *resultsBufferBase) initLocked(isEmpty bool, numExpectedResponses int) error {
	b.Mutex.AssertHeld()
	if b.numExpectedResponses != b.numCompleteResponses {
		b.setErrorLocked(errors.AssertionFailedf("Enqueue is called before the previous requests have been completed"))
		return b.err
	}
	if !isEmpty {
		b.setErrorLocked(errors.AssertionFailedf("Enqueue is called before the results of the previous requests have been retrieved"))
		return b.err
	}
	if b.numUnreleasedResults > 0 {
		b.setErrorLocked(errors.AssertionFailedf("unexpectedly there are some unreleased Results"))
		return b.err
	}
	b.numExpectedResponses = numExpectedResponses
	b.numCompleteResponses = 0
	return nil
}

// signal non-blockingly sends on hasResults channel.
func (b *resultsBufferBase) signal() {
	select {
	case b.hasResults <- struct{}{}:
	default:
	}
}

func (b *resultsBufferBase) wait() {
	<-b.hasResults
}

func (b *resultsBufferBase) numUnreleased() int {
	b.Lock()
	defer b.Unlock()
	return b.numUnreleasedResults
}

func (b *resultsBufferBase) releaseOne() {
	b.Lock()
	defer b.Unlock()
	b.numUnreleasedResults--
}

func (b *resultsBufferBase) setError(err error) {
	b.Lock()
	defer b.Unlock()
	b.setErrorLocked(err)
}

func (b *resultsBufferBase) setErrorLocked(err error) {
	b.Mutex.AssertHeld()
	if b.err == nil {
		b.err = err
	}
	b.signal()
}

func (b *resultsBufferBase) error() error {
	b.Lock()
	defer b.Unlock()
	return b.err
}

func resultsToString(results []Result) string {
	result := "results for positions "
	for i, r := range results {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%d", r.position)
	}
	return result
}

// outOfOrderResultsBuffer is a resultsBuffer that returns the Results in an
// arbitrary order (namely in the same order as the Results are added).
type outOfOrderResultsBuffer struct {
	*resultsBufferBase
	results []Result
}

var _ resultsBuffer = &outOfOrderResultsBuffer{}

func (b *outOfOrderResultsBuffer) init(_ context.Context, numExpectedResponses int) error {
	b.Lock()
	defer b.Unlock()
	if err := b.initLocked(len(b.results) == 0 /* isEmpty */, numExpectedResponses); err != nil {
		b.setErrorLocked(err)
		return err
	}
	return nil
}

func (b *outOfOrderResultsBuffer) add(results []Result) {
	b.Lock()
	defer b.Unlock()
	b.results = append(b.results, results...)
	for i := range results {
		if results[i].GetResp != nil || results[i].ScanResp.Complete {
			b.numCompleteResponses++
		}
	}
	b.numUnreleasedResults += len(results)
	b.signal()
}

func (b *outOfOrderResultsBuffer) get(context.Context) ([]Result, bool, error) {
	b.Lock()
	defer b.Unlock()
	results := b.results
	b.results = nil
	allComplete := b.numCompleteResponses == b.numExpectedResponses
	return results, allComplete, b.err
}

func (b *outOfOrderResultsBuffer) close(context.Context) {
	// Note that only the client's goroutine can be blocked waiting for the
	// results, and close() is called only by the same goroutine, so signaling
	// isn't necessary. However, we choose to be safe and do it anyway.
	b.signal()
}
