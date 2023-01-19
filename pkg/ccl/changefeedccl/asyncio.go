package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// Batch represents an abstract unit of IO.
// Batch identifies the set of keys contained inside the batch.
type Batch interface {
	Keys() intsets.Fast
}

// IOHandler receives a batch to perform blocking IO.
type IOHandler func(context.Context, Batch) error

// asyncIO facilitates asynchronous IO, while maintaining changefeed ordering
// guarantees. It receives series of Batch requests upon which IO operation
// runs.
type asyncIO struct {
	// Do receives IO requests.
	Do chan<- Batch

	// Done signaled when async IO terminates.
	// After Done, Err() method may be used to retrieve the error.
	Done <-chan struct{}

	handler  IOHandler
	ioGroup  ctxgroup.Group
	doChan   chan Batch    // channel for submitting flush requests.
	termChan chan struct{} // channel closed by async flusher to indicate an error
	ioErr    error         // set by IO dispatcher, prior to closing termChan
}

func newAsyncIO(handler IOHandler) *asyncIO {
	doChan := make(chan Batch, flushQueueDepth)
	termChan := make(chan struct{})
	return &asyncIO{
		Do:       doChan,
		Done:     termChan,
		handler:  handler,
		doChan:   doChan,
		termChan: termChan,
	}
}

// Start begins execution of async IO dispatcher.
// numIOWorker go routines are created to handle synchronous IO.
func (io *asyncIO) Start(ctx context.Context, numIOWorkers int) {
	io.ioGroup = ctxgroup.WithContext(ctx)
	io.ioGroup.GoCtx(func(ctx context.Context) error {
		return io.dispatch(ctx, numIOWorkers)
	})
}

// Err returns error encountered while perfomring async IO.
// May only be accessed after Done channel signaled (similar to ctx.Done())
func (io *asyncIO) Err() error {
	return io.ioErr
}

// Flush submits flush request which awaits for all of the outstanding IO
// requests to complete.
func (io *asyncIO) Flush(ctx context.Context) error {
	flushDone := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-io.Done:
		return io.ioErr
	case io.Do <- flushReq(flushDone):
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-io.Done:
		return io.ioErr
	case <-flushDone:
		return nil
	}
}

// Close terminates execution of asyncIO.
func (io *asyncIO) Close() error {
	close(io.doChan) // signal flusher to exit.
	return io.ioGroup.Wait()
}

func (io *asyncIO) dispatch(ctx context.Context, numIOWorkers int) error {
	defer close(io.termChan)

	// IO workers read this channel to receive the next IO request.
	ioWorkerCh := make(chan Batch, numIOWorkers)
	defer close(ioWorkerCh)

	type ioResult struct {
		err error
		req Batch
	}

	// ioResultCh used by workers to communicate IO completion back to this dispatcher.
	ioResultCh := make(chan ioResult)

	// Start numIOWorkers worker go routines: each will read from
	// ioWorkerCh, and send results to the ioResultCh.
	for i := 0; i < numIOWorkers; i++ {
		io.ioGroup.GoCtx(func(ctx context.Context) error {
			for req := range ioWorkerCh {
				res := ioResult{err: io.handler(ctx, req), req: req}
				select {
				case ioResultCh <- res:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	// inflight keeps track of outstanding IO requests.
	var inflight struct {
		intsets.Fast         // All keys currently in flight.
		n            int     // Number of IO operations outstanding.
		q            []Batch // Queue of IO requests that should be started later.
	}

	// Only single flush request can be outstanding at a time.
	// This channel is closed when all outstanding IO completes.
	var pendingFlush chan struct{}

	var finishIO func(res ioResult) error
	// startIO submits IO request to one of the workers.
	startIO := func(req Batch) error {
		// Submit request to the ioWorkerCh.
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ioWorkerCh <- req:
				return nil
			case res := <-ioResultCh:
				// Note: all IO workers may be busy performing IO, so write to
				// ioWorkerCh may block. We therefore must also read from ioResultCh to
				// process results of completed IO requests.
				if err := finishIO(res); err != nil {
					return err
				}
			}
		}
	}

	// finishIO executes when IO completes execution, with or without an err.r
	finishIO = func(res ioResult) error {
		if res.err != nil {
			return res.err
		}

		inflight.n--
		inflight.DifferenceWith(res.req.Keys())

		// Attempt to start single deferred request.
		if len(inflight.q) > 0 {
			for i, req := range inflight.q {
				if !inflight.Intersects(req.Keys()) {
					inflight.n++
					inflight.UnionWith(req.Keys())
					inflight.q = append(inflight.q[:i], inflight.q[i+1:]...)
					if err := startIO(req); err != nil {
						return err
					}
					break
				}
			}
		}

		if pendingFlush != nil && inflight.n == 0 {
			close(pendingFlush)
			pendingFlush = nil
		}
		return nil
	}

	// anyKeysInFlight returns true if any of the keys are currently
	// in flight.
	anyKeysInflight := func(keys intsets.Fast) bool {
		if inflight.Intersects(keys) {
			return true
		}
		// We must check queue in order to make sure that we don't have
		// any previously added batches that contain keys.
		for _, queued := range inflight.q {
			if inflight.Intersects(queued.Keys()) {
				return true
			}
		}
		return false
	}

	setErrorAndReturn := func(err error) error {
		io.ioErr = err
		return err
	}

	// Main dispatch loop processes new requests, and handles IO completion notifications.
	for {
		select {
		case <-ctx.Done():
			return setErrorAndReturn(ctx.Err())
		case resp := <-ioResultCh:
			// Process IO completion.
			if err := finishIO(resp); err != nil {
				return setErrorAndReturn(err)
			}
		case req, ok := <-io.doChan:
			// Process new IO request.
			if !ok {
				return nil // We're done.
			}

			if flush, isFlush := req.(flushReq); isFlush {
				if pendingFlush != nil {
					return setErrorAndReturn(errors.AssertionFailedf("unexpected concurrent flush"))
				}
				if inflight.n == 0 {
					close(flush)
				} else {
					pendingFlush = flush
				}
				continue
			}

			if anyKeysInflight(req.Keys()) {
				// Can't start this batch right now since some keys are in flight.
				// We must wait for the completion of previous batch(es) in order to maintain
				// ordering guarantees.
				inflight.q = append(inflight.q, req)
				continue
			}

			// Mark keys in this request in flight, and submit IO request to the worker.
			inflight.n++
			inflight.UnionWith(req.Keys())
			if err := startIO(req); err != nil {
				return setErrorAndReturn(err)
			}
		}
	}
}

type flushReq chan struct{}

func (f flushReq) Keys() (empty intsets.Fast) {
	return empty
}

var _ Batch = (flushReq)(nil)
