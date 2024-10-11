package producer

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

type rangeStatsPoller struct {
	cancel func()
	g      ctxgroup.Group
	stats  atomic.Pointer[streampb.StreamEvent_RangeStats]
}

func NewPoller(
	ctx context.Context,
	interval time.Duration,
	spans []roachpb.Span,
	frontier span.Frontier,
	ranges rangedesc.IteratorFactory,
) *rangeStatsPoller {
	ctx, cancel := context.WithCancel(ctx)
	poller := &rangeStatsPoller{
		cancel: cancel,
		g:      ctxgroup.WithContext(ctx),
	}
	poller.g.GoCtx(func(ctx context.Context) error {
		// TODO add a jitter to the interval
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		for {
			// TODO get interval from the setting
			// TODO replace "lagging" with "scanning"
			stats, err := computeRangeStats(ctx, spans, frontier, ranges)
			if err != nil {
				log.Warningf(ctx, "event stream unable to calculate range stats: %v", err)
			} else {
				poller.stats.Store(&stats)
			}

			log.VEventf(ctx, 1, "publishing range stats: %+v", stats)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				//continue
			}
		}
	})
	return poller
}

func (r *rangeStatsPoller) Close() {
	r.cancel()
	_ = r.g.Wait()
}

func (r *rangeStatsPoller) MaybeStats() *streampb.StreamEvent_RangeStats {
	return r.stats.Load()
}

func computeRangeStats(
	ctx context.Context,
	spans []roachpb.Span,
	frontier span.Frontier,
	ranges rangedesc.IteratorFactory,
) (streampb.StreamEvent_RangeStats, error) {
	var stats streampb.StreamEvent_RangeStats
	for _, initialSpan := range spans {
		lazyIterator, err := ranges.NewLazyIterator(ctx, initialSpan, 100)
		if err != nil {
			return streampb.StreamEvent_RangeStats{}, err
		}
		for ; lazyIterator.Valid(); lazyIterator.Next() {
			// TODO: do I need to decode this with a codec?
			// TODO: why does the RKey type exist?
			rangeSpan := roachpb.Span{
				Key:    lazyIterator.CurRangeDescriptor().StartKey.AsRawKey(),
				EndKey: lazyIterator.CurRangeDescriptor().EndKey.AsRawKey(),
			}
			stats.RangeCount += 1
			frontier.SpanEntries(
				rangeSpan,
				func(_ roachpb.Span, timestamp hlc.Timestamp) span.OpResult {
					if timestamp.IsEmpty() {
						stats.ScanningRangeCount += 1
						return span.StopMatch
					}
					return span.ContinueMatch
				})
		}
		if lazyIterator.Error() != nil {
			return streampb.StreamEvent_RangeStats{}, err
		}
	}
	return stats, nil
}
