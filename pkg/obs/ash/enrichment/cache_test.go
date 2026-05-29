// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enrichment_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/ash/enrichment"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// makeID returns a deterministic, non-zero clusterunique.ID derived from i.
func makeID(i uint64) clusterunique.ID {
	return clusterunique.GenerateID(
		hlc.Timestamp{WallTime: int64(i + 1)}, base.SQLInstanceID(1))
}

func startTestCache(
	t *testing.T, enabled bool, limit int64,
) (*enrichment.Cache, *enrichment.Metrics, func()) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	enrichment.Enabled.Override(ctx, &st.SV, enabled)
	enrichment.CacheLimit.Override(ctx, &st.SV, limit)
	metrics := enrichment.NewMetrics()
	stopper := stop.NewStopper()
	c := enrichment.NewCache(st, &metrics)
	c.Start(ctx, stopper)
	return c, &metrics, func() { stopper.Stop(ctx) }
}

// TestCacheRoundtrip exercises the basic write → drain → read path: an entry
// written with PutExecution and then flushed via DrainWriteBuffer must be
// visible to GetExecution with the same attributes.
func TestCacheRoundtrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c, _, cleanup := startTestCache(t, true /* enabled */, 1024 /* limit */)
	defer cleanup()

	id := makeID(1)
	attrs := enrichment.Attributes{
		AppName:     "myapp",
		Database:    "mydb",
		User:        "alice",
		PlanGist:    []byte("gist-abc"),
		CanaryStats: true,
		TxnID:       uuid.FromStringOrNil("00000000-0000-0000-0000-000000000001"),
		SessionID:   makeID(2),
	}

	c.PutExecution(id, attrs)
	c.DrainWriteBuffer()

	testutils.SucceedsWithin(t, func() error {
		got, ok := c.GetExecution(id)
		if !ok {
			return errors.New("expected entry, but it was not present")
		}
		require.Equal(t, attrs, got)
		return nil
	}, 3*time.Second)
}

// TestCacheGetMissing confirms that a Get for a key that was never written
// returns found=false.
func TestCacheGetMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c, _, cleanup := startTestCache(t, true /* enabled */, 1024 /* limit */)
	defer cleanup()

	_, ok := c.GetExecution(makeID(99))
	require.False(t, ok)
}

// TestCacheDisabledIsNoop confirms that when obs.ash.enrichment.enabled is
// false, both PutExecution and GetExecution behave as no-ops: nothing is
// stored and reads do not find anything even after a drain.
func TestCacheDisabledIsNoop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c, _, cleanup := startTestCache(t, false /* enabled */, 1024 /* limit */)
	defer cleanup()

	id := makeID(1)
	c.PutExecution(id, enrichment.Attributes{AppName: "myapp"})
	c.DrainWriteBuffer()

	_, ok := c.GetExecution(id)
	require.False(t, ok, "expected miss when enrichment disabled")
}

// TestCacheFIFOEviction confirms that once the configured entry limit is
// exceeded, the oldest entries are evicted while the newest remain.
// Eviction is block-granular (the cache drops whole blocks of recently
// arrived entries at a time), so the test inserts enough entries to
// guarantee that several blocks exist before, during, and after the
// eviction sweep.
func TestCacheFIFOEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		limit    = 256
		numWrite = 4096
	)
	c, _, cleanup := startTestCache(t, true /* enabled */, limit)
	defer cleanup()

	ids := make([]clusterunique.ID, numWrite)
	for i := range ids {
		ids[i] = makeID(uint64(i))
		c.PutExecution(ids[i], enrichment.Attributes{AppName: "x"})
	}
	c.DrainWriteBuffer()

	testutils.SucceedsWithin(t, func() error {
		if _, ok := c.GetExecution(ids[0]); ok {
			return errors.New("expected oldest entry to be evicted, but found it")
		}
		if _, ok := c.GetExecution(ids[len(ids)-1]); !ok {
			return errors.New("expected newest entry to be present, but it was missing")
		}
		if got := c.Size(); got > limit {
			return errors.Newf("cache size %d unexpectedly exceeds limit %d", got, limit)
		}
		return nil
	}, 3*time.Second)
}

// TestCacheMetrics confirms the put/get/miss counters are updated by the
// corresponding API calls.
func TestCacheMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c, metrics, cleanup := startTestCache(t, true /* enabled */, 1024 /* limit */)
	defer cleanup()

	id := makeID(1)
	c.PutExecution(id, enrichment.Attributes{AppName: "x"})
	c.DrainWriteBuffer()

	require.Equal(t, int64(1), metrics.CachePutCount.Count())

	testutils.SucceedsWithin(t, func() error {
		if _, ok := c.GetExecution(id); !ok {
			return errors.New("entry not yet visible")
		}
		return nil
	}, 3*time.Second)
	require.GreaterOrEqual(t, metrics.CacheGetCount.Count(), int64(1))

	// SucceedsWithin may have produced miss-counter increments while the
	// in-flight block was being ingested. Capture the baseline before the
	// explicit miss and verify only the delta.
	missesBefore := metrics.CacheGetMissCount.Count()
	_, ok := c.GetExecution(makeID(99))
	require.False(t, ok)
	require.Equal(t, missesBefore+1, metrics.CacheGetMissCount.Count())
}
