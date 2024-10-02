// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package reduce implements a reducer core for reducing the size of test
// failure cases.
//
// See: https://blog.regehr.org/archives/1678.
package reduce

import (
	"context"
	"log"
	"math/rand"
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Pass defines a reduce pass.
type Pass interface {
	// New creates a new opaque state object for the input string.
	New(string) State
	// Transform applies this transformation pass to the input string using
	// State to determine which occurrence to transform. It returns the
	// transformed string, a Result indicating whether to proceed or not, and
	// an error if the transformation could not be performed.
	Transform(string, State) (string, Result, error)
	// Advance moves State to the next occurrence of a transformation in
	// the given input string and returns the new State.
	Advance(string, State) State
	// Name returns the name of the Pass.
	Name() string
}

// Result is returned by a Transform func.
type Result int

const (
	// OK indicates there are more transforms in the current Pass.
	OK Result = iota
	// STOP indicates there are no more transforms.
	STOP
)

// State is opaque state for a Pass.
type State interface{}

// InterestingFn returns true if the string triggers the target test failure. It
// should be context-aware and stop work if the context is canceled. It can
// return a function which will be called if the original test case is not
// interesting. The function should log a hint that will help the user
// understand why the original test case is not interesting.
type InterestingFn func(context.Context, string) (_ bool, logOriginalHint func())

// Mode is an enum specifying how to determine if forward progress was made.
type Mode int

const (
	// ModeSize instructs Reduce to use filesize as the progress indicator.
	ModeSize Mode = iota
	// ModeInteresting instructs Reduce to use the interestingness as the
	// progress indicator. That is, if any pass generates an interesting
	// result (even if the file size increases), that is considered
	// progress.
	ModeInteresting
)

// Reduce executes the test case reduction algorithm. logger, if not nil, will
// log progress output. numGoroutines is the number of parallel workers, or 0
// for GOMAXPROCS.
func Reduce(
	logger *log.Logger,
	originalTestCase string,
	isInteresting InterestingFn,
	numGoroutines int,
	mode Mode,
	chunkReducer ChunkReducer,
	passList ...Pass,
) (string, error) {
	if numGoroutines < 1 {
		numGoroutines = runtime.GOMAXPROCS(0)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if interesting, logHint := isInteresting(ctx, originalTestCase); !interesting {
		if logHint != nil {
			logHint()
		}
		return "", errors.New("original test case not interesting")
	}

	chunkReducedTestCase, err := attemptChunkReduction(logger, originalTestCase, isInteresting, chunkReducer)
	if err != nil {
		return "", err
	}

	// findNextInteresting finds the next interesting result. It does this
	// by starting some worker goroutines and running the interestingness
	// test on different variants of each of them. To preserve determinism,
	// if an interesting variant is found, it is only reported if all tests
	// before it were uninteresting. This is tracked by giving each worker
	// a done chan from the previous worker.
	// See https://blog.regehr.org/archives/1679.
	findNextInteresting := func(vs varState) (*varState, error) {
		ctx := context.Background()
		g := ctxgroup.WithContext(ctx)
		variants := make(chan varState)
		g.GoCtx(func(ctx context.Context) error {
			// This goroutine generates all variants from passList and sends
			// them on a chan for testing. It closes the variants chan when
			// there are no more. Since numGoroutines are working at one time,
			// this goroutine will block until one is available. If an
			// interesting variant is found, ctx will close and this goroutine
			// will shut down.
			defer close(variants)
			current := vs.file
			state := vs.s
			var done, prev chan struct{}
			// Pre-populate the first prev.
			prev = make(chan struct{}, 1)
			prev <- struct{}{}
			for pi := vs.pi; pi < len(passList); pi++ {
				p := passList[pi]
				if state == nil {
					state = p.New(current)
				}
				for {
					variant, result, err := p.Transform(current, state)
					if err != nil || result != OK {
						state = nil
						break
					}
					// Done must be buffered because it will only be received
					// from if the following variant was interesting, and in
					// other cases the send must not block.
					done = make(chan struct{}, 1)
					select {
					case variants <- varState{
						pi:   pi,
						file: variant,
						s:    state,
						done: done,
						prev: prev,
					}:
						prev = done
					case <-ctx.Done():
						return nil
					}
					state = p.Advance(current, state)
				}
			}
			return nil
		})
		// Start the workers.
		for i := 0; i < numGoroutines; i++ {
			g.GoCtx(func(ctx context.Context) error {
				for vs := range variants {
					if interesting, _ := isInteresting(ctx, vs.file); interesting {
						// Wait for the previous test to finish.
						select {
						case <-ctx.Done():
							return nil
						case <-vs.prev:
							// Since the send on vs.done is below this next return,
							// vs.prev will only send if the previous (and thus all previous)
							// interestingness tests failed, so we know if we got here
							// we're the first interesting variant.

							// Return a non-nil error to shut down all the other go routines.
							return errInteresting(vs)
						}
					}
					vs.done <- struct{}{}
				}
				return nil
			})
		}
		// Wait for the errgroup to shut down. If an error is produced,
		// it could be a normal error in which case return it. An error
		// could also be the sentinel errInteresting type (i.e., a
		// varState), which means an interesting variant was found and
		// we should return that varState. If no error is returned it
		// means there were no more interesting variants found starting
		// from the passed varState.
		if err := g.Wait(); err != nil {
			var ierr errInteresting
			if errors.As(err, &ierr) {
				vs := varState(ierr)
				if logger != nil {
					logger.Printf("\tpass %d of %d (%s): %d bytes\n", vs.pi+1, len(passList),
						passList[vs.pi].Name(), len(vs.file))
				}
				return &vs, nil
			}
			return nil, err
		}
		return nil, nil
	}

	start := timeutil.Now()
	vs := varState{
		file: chunkReducedTestCase,
	}
	if logger != nil {
		logger.Printf("size: %d\n", len(vs.file))
	}
	for {
		sizeAtStart := len(vs.file)
		foundInteresting := false
		for {
			next, err := findNextInteresting(vs)
			if err != nil {
				if logger != nil {
					logger.Printf("unexpected error: %s", err)
				}
				//nolint:returnerrcheck
				return "", nil
			}
			if next == nil {
				break
			}
			foundInteresting = true
			vs = *next
		}
		done := false
		switch mode {
		case ModeSize:
			if len(vs.file) >= sizeAtStart {
				done = true
			}
		case ModeInteresting:
			done = !foundInteresting
		default:
			panic("unknown mode")
		}
		if done {
			break
		}
		// Need to do another round. Clear pi and state.
		vs = varState{
			file: vs.file,
		}
	}
	if logger != nil {
		logger.Printf("total time: %v\n", timeutil.Since(start))
		logger.Printf("original size: %v\n", len(originalTestCase))
		if chunkReducer != nil {
			logger.Printf("chunk-reduced size: %v\n", len(chunkReducedTestCase))
		}
		logger.Printf("final size: %v\n", len(vs.file))
		logger.Printf("reduction: %v%%\n", 100-int(100*float64(len(vs.file))/float64(len(originalTestCase))))
	}
	return vs.file, nil
}

// errInteresting is an error version of varState that is a special sentinel
// error. It is used to shutdown the other goroutines in the errgroup while
// also transmitting the new varState to resume from.
type errInteresting varState

func (e errInteresting) Error() string {
	return "interesting"
}

// varState tracks the current variant state, which is a tuple of the current
// pass, file, and state.
type varState struct {
	pi   int
	file string
	s    State

	// done and prev are used to synchronize work between variant
	// testing. A variant sends on done when it has verified its test is
	// uninteresting. If its test was interesting, it receives on prev,
	// which thus guarantees that it was the first interesting variant.
	done, prev chan struct{}
}

// A ChunkReducer can eliminate large chunks of a test case before performing
// the more granular and expensive reduction algorithm. It breaks a test case
// into segments. Segments can be grouped into chunks than can be removed
// entirely from the test case if they aren't required to produce an interesting
// result.
type ChunkReducer interface {
	// HaltAfter returns the number of consecutive failed reduction attempts
	// allowed before chunk reduction is halted.
	HaltAfter() int
	// Init the ChunkReducer with the given string.
	Init(string) error
	// NumSegments returns the total number of segments that are eligible to be
	// reduced en masse.
	NumSegments() int
	// DeleteSegments returns a string with segments [start, end) removed from
	// the original string.
	DeleteSegments(start, end int) string
}

// attemptChunkReduction attempts to reduce chunks of originalTestCase en masse
// using the provided ChunkReducer. It randomly deletes a range of segments and
// tests if the remaining segments satisfy isInteresting. It will continually
// reduce segments until it fails to reduce chunkReducer.HaltAfter() times in a
// row.
func attemptChunkReduction(
	logger *log.Logger,
	originalTestCase string,
	isInteresting InterestingFn,
	chunkReducer ChunkReducer,
) (string, error) {
	if chunkReducer == nil {
		return originalTestCase, nil
	}

	ctx := context.Background()
	reduced := originalTestCase

	failedAttempts := 0
	for failedAttempts < chunkReducer.HaltAfter() {
		err := chunkReducer.Init(reduced)
		if err != nil {
			return "", err
		}

		if chunkReducer.NumSegments() == 0 {
			return reduced, nil
		}

		// Pick two random indexes and remove all statements between them.
		start := rand.Intn(chunkReducer.NumSegments())
		end := rand.Intn(chunkReducer.NumSegments()-start) + start + 1

		localReduced := chunkReducer.DeleteSegments(start, end)
		if interesting, _ := isInteresting(ctx, localReduced); interesting {
			reduced = localReduced
			if logger != nil {
				logger.Printf("\tchunk reduction: %d bytes\n", len(reduced))
			}
			failedAttempts = 0
		} else {
			failedAttempts++
		}
	}

	return reduced, nil
}
