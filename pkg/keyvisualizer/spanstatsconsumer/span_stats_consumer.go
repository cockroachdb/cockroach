package spanstatsconsumer

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type SpanStatsConsumer struct {
	ie sqlutil.InternalExecutor
}

func New() *SpanStatsConsumer {
	return nil
}

func (s *SpanStatsConsumer) FetchStats(start, end hlc.Timestamp) error {
	// fetch stats via GetSamplesFromAllNodes RPC
	// downsample
	// normalize
	// persist
	return nil
}

func (s *SpanStatsConsumer) DecideBoundaries() {
	// boundary decision scheme for now:
	// 1) Get this tenant's ranges
	// 2) consider N adjacent ranges to be a single boundary,
	// if len(ranges) > 10k
	// issue SaveBoundaries RPC
}
