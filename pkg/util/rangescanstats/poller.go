// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangescanstats

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/rangescanstats/rangescanstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// RangeStatsPoller manages a goroutine that polls the total number of ranges
// and their scanning status. Close must be called to avoid leaking the
// goroutine.
type RangeStatsPoller struct {
	cancel func()
	g      ctxgroup.Group
	stats  atomic.Pointer[rangescanstatspb.RangeStats]
}

func StartStatsPoller(
	ctx context.Context,
	interval time.Duration,
	spans []roachpb.Span,
	frontier span.Frontier,
	ranges rangedesc.IteratorFactory,
	laggingSpanThreshold time.Duration,
) *RangeStatsPoller {
	ctx, cancel := context.WithCancel(ctx)
	poller := &RangeStatsPoller{
		cancel: cancel,
		g:      ctxgroup.WithContext(ctx),
	}
	poller.g.GoCtx(func(ctx context.Context) error {
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			stats, err := computeRangeStats(ctx, spans, frontier, ranges, laggingSpanThreshold)
			if err != nil {
				log.Dev.Warningf(ctx, "unable to calculate range scan stats: %v", err)
			} else {
				poller.stats.Store(&stats)
			}

			log.VEventf(ctx, 1, "publishing range scan stats: %+v", stats)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
				//continue
			}
		}
	})
	return poller
}

// Close cancels the internal context and waits for the goroutine to exit.
func (r *RangeStatsPoller) Close() {
	r.cancel()
	_ = r.g.Wait()
}

// MaybeStats returns the most recent stats if they are available or null if
// the initial stats calculation is not ready.
func (r *RangeStatsPoller) MaybeStats() *rangescanstatspb.RangeStats {
	return r.stats.Load()
}

func computeRangeStats(
	ctx context.Context,
	spans []roachpb.Span,
	frontier span.Frontier,
	ranges rangedesc.IteratorFactory,
	laggingSpanThreshold time.Duration,
) (rangescanstatspb.RangeStats, error) {
	var stats rangescanstatspb.RangeStats
	for _, initialSpan := range spans {
		lazyIterator, err := ranges.NewLazyIterator(ctx, initialSpan, 100)
		if err != nil {
			return rangescanstatspb.RangeStats{}, err
		}
		for ; lazyIterator.Valid(); lazyIterator.Next() {
			now := timeutil.Now()
			rangeSpan := roachpb.Span{
				Key:    lazyIterator.CurRangeDescriptor().StartKey.AsRawKey(),
				EndKey: lazyIterator.CurRangeDescriptor().EndKey.AsRawKey(),
			}
			stats.RangeCount += 1
			for _, timestamp := range frontier.SpanEntries(rangeSpan) {
				if timestamp.IsEmpty() {
					stats.ScanningRangeCount += 1
					break
				} else if now.Sub(timestamp.GoTime()) > laggingSpanThreshold {
					stats.LaggingRangeCount += 1
					break
				}
			}
		}
		if lazyIterator.Error() != nil {
			return rangescanstatspb.RangeStats{}, lazyIterator.Error()
		}
	}
	return stats, nil
}
