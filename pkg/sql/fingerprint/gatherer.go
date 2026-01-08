// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprint

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type spanFingerprintResult struct {
	span        roachpb.Span
	fingerprint uint64
}

type spanFingerprinter func(ctx context.Context, span roachpb.Span) (spanFingerprintResult, error)

type checkpointState struct {
	fingerprint uint64
	totalParts  int
	done        span.Frontier
}

type persister interface {
	load(ctx context.Context) (checkpointState, bool, error)
	store(ctx context.Context, state checkpointState, frac float64) error
}

type partitioner interface {
	partition(ctx context.Context, spans []roachpb.Span) ([]sql.SpanPartition, error)
}

// gatherer represents the high-level orchestration involved in fingerprinting
// a collection of spans in terms of abstract components for computing the
// fingerprint for any individual span or for persisting and retrieving progress
// information from durable (e.g. job_info) storage. The gatherer is responsible
// for tying these components together while managing concurrency and
// coordination acoss the workers doing the fingerprinting, aggregating their
// partial results, and periodically the aggregation, as well as resuming this
// process from a previously persisted checkpoint.
type gatherer struct {
	spans       []roachpb.Span
	persister   persister
	partitioner partitioner
	fn          spanFingerprinter
	chkptFreq   func() time.Duration

	mu struct {
		syncutil.Mutex
		checkpointState
		prog []struct{ total, remaining int }
	}
}

// Run executes the fingerprinting job.
// Spans should be the initial target spans.
// Returns the final fingerprint.
func (g *gatherer) run(ctx context.Context) (uint64, error) {
	loaded, ok, err := g.persister.load(ctx)
	if err != nil {
		return 0, err
	}
	if ok {
		g.mu.checkpointState = loaded
	} else {
		g.mu.checkpointState.done, err = span.MakeFrontier(g.spans...)
		if err != nil {
			return 0, err
		}
	}

	todo := g.remaining()
	if len(todo) == 0 {
		return g.finish(ctx)
	}

	partitions, err := g.partitioner.partition(ctx, todo)
	if err != nil {
		return 0, errors.Wrap(err, "failed to partition spans")
	}
	g.mu.prog = make([]struct{ total, remaining int }, len(partitions))

	stopCheckpoint := make(chan struct{})
	results := make(chan spanResult, len(partitions))
	grp := ctxgroup.WithContext(ctx)

	// Gather the individual span fingerprints.
	grp.GoCtx(func(ctx context.Context) error {
		defer close(results)
		return ctxgroup.GroupWorkers(ctx, len(partitions), func(ctx context.Context, i int) error {
			return worker(ctx, i, partitions[i].Spans, g.fn, results)
		})
	})

	// Accumulate results from the workers into the gatherer state.
	grp.GoCtx(func(ctx context.Context) error {
		defer close(stopCheckpoint)
		for res := range results {
			if err := g.record(res); err != nil {
				return err
			}
		}
		return nil
	})

	// Periodically checkpoint progress to the persister.
	grp.GoCtx(func(ctx context.Context) error {
		freq := g.chkptFreq()
		ticker := time.NewTicker(freq)
		for {
			select {
			case <-ticker.C:
				if err := g.checkpoint(ctx); err != nil {
					return errors.Wrap(err, "failed to checkpoint progress")
				}
				if f := g.chkptFreq(); f != freq {
					freq = f
					ticker.Reset(freq)
				}
			case <-stopCheckpoint:
				return nil
			}
		}
	})

	if err := grp.Wait(); err != nil {
		return 0, err
	}

	return g.finish(ctx)
}

func (g *gatherer) remaining() []roachpb.Span {
	var remaining []roachpb.Span
	for span, ts := range g.mu.done.Entries() {
		if ts.Less(hlc.Timestamp{WallTime: 1}) {
			remaining = append(remaining, span)
		}
	}
	return remaining
}

func (g *gatherer) record(result spanResult) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.mu.fingerprint ^= result.fingerprint
	if _, err := g.mu.done.Forward(result.span, hlc.Timestamp{WallTime: 1}); err != nil {
		return err
	}
	g.mu.prog[result.idx].total, g.mu.prog[result.idx].remaining = result.total, result.remaining

	return nil
}

func (g *gatherer) checkpoint(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	var overallRemaining, overallTotal int
	for _, p := range g.mu.prog {
		overallRemaining += p.remaining
		overallTotal += p.total
	}

	// Update the totalPart count that gets persisted if needed, and use it as the
	// overall total for progress fraction reporting.
	g.mu.checkpointState.totalParts = max(overallTotal, g.mu.checkpointState.totalParts)
	overallTotal = g.mu.checkpointState.totalParts

	// Nothing to save yet.
	if overallRemaining == overallTotal {
		return nil
	}
	return g.persister.store(ctx, g.mu.checkpointState, float64(overallTotal-overallRemaining)/float64(overallTotal))
}

func (g *gatherer) finish(ctx context.Context) (uint64, error) {
	return g.mu.fingerprint, nil
}

type spanResult struct {
	spanFingerprintResult
	idx, total, remaining int
}

func worker(
	ctx context.Context, idx int, todo []roachpb.Span, fn spanFingerprinter, res chan<- spanResult,
) error {
	done := ctx.Done()
	i := 0
	for _, sp := range todo {
		select {
		case <-done:
			return ctx.Err()
		default:
			r, err := fn(ctx, sp)
			if err != nil {
				return err
			}
			i++
			res <- spanResult{r, idx, len(todo), len(todo) - i}
		}
	}
	return nil
}
