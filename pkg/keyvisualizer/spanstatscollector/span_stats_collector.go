package spanstatscollector

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"time"
)

type tenantStatsCollector struct {
	histogram interval.Tree
}

type SpanStatsCollector struct {
	tenants map[uint64]*tenantStatsCollector
}

func New(ctx context.Context, s *stop.Stopper) (*SpanStatsCollector, error) {

	collector := &SpanStatsCollector{}

	// what happens if this fails?
	// should it be restarted?
	// how are errors logged, if something bubbles up?
	err := s.RunAsyncTask(ctx, "span-stats-collector",
		func(ctx context.Context) {
			for {
				select {
				case <-s.ShouldQuiesce():
					return
				default:
					// Keep going.
				}

				// sleep for 15 minutes
				time.Sleep(time.Minute * 15)

				// roll over histograms
				collector.attemptRollOver()

				// install fresh histograms and start a new sample period
				collector.reset()

				// clear inactive tenants
				collector.clearInactiveTenants()

				// TODO(zachlite): log errors
			}
		})

	return collector, err
}

func (s *SpanStatsCollector) SaveBoundaries(
	boundaries keyvispb.Boundaries,
) error {
	return nil
}

func (s *SpanStatsCollector) Increment(
	id roachpb.TenantID, sp roachpb.Span,
) error {
	return nil
}

func (s *SpanStatsCollector) GetSamples(
	id roachpb.TenantID, start, end hlc.Timestamp,
) []keyvispb.Sample {
	return nil
}

// private API
func (s *SpanStatsCollector) attemptRollOver() {

}

func (s *SpanStatsCollector) reset() {

}

func (s *SpanStatsCollector) clearInactiveTenants() {

}
