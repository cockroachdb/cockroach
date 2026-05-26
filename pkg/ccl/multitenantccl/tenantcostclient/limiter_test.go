// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostclient

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TestLimiterNotify tests that low tokens notifications are sent at the
// expected times.
func TestLimiterNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)
	ch := make(chan struct{}, 100)

	var met metrics
	met.Init(roachpb.Locality{})

	var lim limiter
	lim.Init(&met, ts, ch)
	lim.Reconfigure(start, limiterReconfigureArgs{NewRate: 100})

	check := func(expected string) {
		t.Helper()
		actual := lim.String(ts.Now())
		if actual != expected {
			t.Errorf("expected: %s\nactual: %s\n", expected, actual)
		}
	}

	// Check notification.
	checkNoNotification := func() {
		t.Helper()
		select {
		case <-ch:
			t.Error("unexpected notification")
		default:
		}
	}

	checkNotification := func() {
		t.Helper()
		select {
		case <-ch:
		default:
			t.Error("expected notification")
		}
	}

	checkNoNotification()
	args := limiterReconfigureArgs{
		NewTokens:       30,
		NewRate:         10,
		NotifyThreshold: 5,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("30.00 tokens filling @ 10.00 tokens/s")

	lim.RemoveTokens(ts.Now(), 20)
	// No notification: we did not go below the threshold.
	checkNoNotification()
	// Now we should get a notification.
	lim.RemoveTokens(ts.Now(), 8)
	checkNotification()
	check("2.00 tokens filling @ 10.00 tokens/s")

	// We only get one notification (until we Reconfigure or StartNotification).
	lim.RemoveTokens(ts.Now(), 1)
	checkNoNotification()

	// Reconfigure without enough tokens to meet the threshold and ensure we get
	// a notification.
	args = limiterReconfigureArgs{
		NewTokens:       1,
		NotifyThreshold: 5,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNotification()
	check("2.00 tokens filling @ 0.00 tokens/s")

	// Reconfigure with enough tokens to exceed threshold and ensure there is no
	// notification.
	args = limiterReconfigureArgs{
		NewTokens: 80,
		NewRate:   1,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("82.00 tokens filling @ 1.00 tokens/s")

	// Call SetupNotification with a high threshold and ensure notification.
	lim.SetupNotification(ts.Now(), 83)
	checkNotification()

	// And then with lower threshold, and ensure no notification.
	lim.SetupNotification(ts.Now(), 40)
	checkNoNotification()

	// Refill bucket.
	args = limiterReconfigureArgs{
		NewTokens:       23,
		NewRate:         10,
		NotifyThreshold: 40,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("105.00 tokens filling @ 10.00 tokens/s")

	// Try a fulfilled Acquire that doesn't drop below threshold.
	fulfill := func(amount float64) {
		t.Helper()
		req := &waitRequest{needed: amount}
		if ok, _ := req.Acquire(ctx, &lim); !ok {
			t.Fatalf("failed to fulfill")
		}
	}

	fulfill(10)
	checkNoNotification()
	check("95.00 tokens filling @ 10.00 tokens/s")

	// Try a fulfilled Acquire that does drop below the threshold.
	fulfill(60)
	checkNotification()
	check("35.00 tokens filling @ 10.00 tokens/s")

	// Refill bucket.
	ts.Advance(5 * time.Second)
	fulfill(0)
	checkNoNotification()
	check("85.00 tokens filling @ 10.00 tokens/s")
	lim.SetupNotification(ts.Now(), 5)

	// Fail to fulfill a request and expect a notification.
	req := &waitRequest{needed: 100}
	if ok, _ := req.Acquire(ctx, &lim); ok {
		t.Fatalf("fulfilled incorrectly")
	}
	checkNotification()
	check("85.00 tokens filling @ 10.00 tokens/s (100.00 waiting tokens)")

	// Add enough tokens to fulfill the waiting request and trigger a notification.
	args = limiterReconfigureArgs{
		NewTokens:       15,
		NotifyThreshold: 5,
	}
	lim.Reconfigure(ts.Now(), args)
	if ok, _ := req.Acquire(ctx, &lim); !ok {
		t.Fatalf("failed to fulfill")
	}
	checkNotification()
	check("0.00 tokens filling @ 0.00 tokens/s")

	// Ensure that MaxTokens is enforced.
	args = limiterReconfigureArgs{
		NewTokens: 100,
		MaxTokens: 50,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("50.00 tokens filling @ 0.00 tokens/s (limited to 50.00 tokens)")
}

// TestLimiterMetrics tests that limiter metrics are updated.
func TestLimiterMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)
	ch := make(chan struct{}, 100)

	var met metrics
	met.Init(roachpb.Locality{})

	var lim limiter
	lim.Init(&met, ts, ch)

	ensureMetricValue := func(metric *metric.Gauge, expected int64) {
		testutils.SucceedsWithin(t, func() error {
			val := metric.Value()
			if val == expected {
				return nil
			}
			return errors.New("metric doesn't have expected value")
		}, 30*time.Second)
	}

	// Create a blocking request and wait until the metric reflects that.
	go func() {
		if err := lim.Wait(ctx, 1000); err != nil {
			t.Errorf("failed to wait: %v", err)
		}
	}()
	ensureMetricValue(met.CurrentBlocked, 1)

	// Unblock the request and ensure the metric changes.
	lim.Reconfigure(ts.Now(), limiterReconfigureArgs{NewTokens: 1000})
	ensureMetricValue(met.CurrentBlocked, 0)
}
