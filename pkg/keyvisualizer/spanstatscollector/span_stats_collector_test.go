package spanstatscollector

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSpanStatsCollectorInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	collector, err := NewSpanStatsCollector(ctx, stopper)
	require.NoError(t, err)
	_ = collector

	time.Sleep(time.Second * 5)
}

