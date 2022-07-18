package spanstatsmanager

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// SpanStatsManager ensures that there's only one SpanStatsConsumer job
// running per tenant.
type SpanStatsManager struct {
	db      *kv.DB
	jr      *jobs.Registry
	ie      sqlutil.InternalExecutor
	stopper *stop.Stopper
	keyvisualizer.SpanStatsConsumer
}

func New() *SpanStatsManager {
	return nil
}

// cargo-culting from spanconfigmanager.Manager
func (s *SpanStatsManager) Start(ctx context.Context) error {
	return nil
}

func (s *SpanStatsManager) run(ctx context.Context) {

}
