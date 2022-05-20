// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestLimiterNotify tests that low RU notifications are sent at the expected
// times.
func TestLimiterNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)
	ch := make(chan struct{}, 100)

	var lim limiter
	lim.Init(ts, ch)
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
	check("30.00 RU filling @ 10.00 RU/s")

	lim.RemoveRU(ts.Now(), 20)
	// No notification: we did not go below the threshold.
	checkNoNotification()
	// Now we should get a notification.
	lim.RemoveRU(ts.Now(), 8)
	checkNotification()
	check("2.00 RU filling @ 10.00 RU/s")

	// We only get one notification (until we Reconfigure or StartNotification).
	lim.RemoveRU(ts.Now(), 1)
	checkNoNotification()

	// Reconfigure without enough tokens to meet the threshold and ensure we get
	// a notification.
	args = limiterReconfigureArgs{
		NewTokens:       1,
		NotifyThreshold: 5,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNotification()
	check("2.00 RU filling @ 0.00 RU/s")

	// Reconfigure with enough tokens to exceed threshold and ensure there is no
	// notification.
	args = limiterReconfigureArgs{
		NewTokens: 80,
		NewRate:   1,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("82.00 RU filling @ 1.00 RU/s")

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
	check("105.00 RU filling @ 10.00 RU/s")

	// Try a fulfilled Acquire that doesn't drop below threshold.
	fulfill := func(amount tenantcostmodel.RU) {
		t.Helper()
		req := &waitRequest{needed: amount}
		if ok, _ := req.Acquire(ctx, &lim); !ok {
			t.Fatalf("failed to fulfill")
		}
	}

	fulfill(10)
	checkNoNotification()
	check("95.00 RU filling @ 10.00 RU/s")

	// Try a fulfilled Acquire that does drop below the threshold.
	fulfill(60)
	checkNotification()
	check("35.00 RU filling @ 10.00 RU/s")

	// Refill bucket.
	ts.Advance(5 * time.Second)
	fulfill(0)
	checkNoNotification()
	check("85.00 RU filling @ 10.00 RU/s")
	lim.SetupNotification(ts.Now(), 5)

	// Fail to fulfill a request and expect a notification.
	req := &waitRequest{needed: 100}
	if ok, _ := req.Acquire(ctx, &lim); ok {
		t.Fatalf("fulfilled incorrectly")
	}
	checkNotification()
	check("85.00 RU filling @ 10.00 RU/s (100.00 waiting RU)")

	// Add enough RU to fulfill the waiting request and trigger a notification.
	args = limiterReconfigureArgs{
		NewTokens:       15,
		NotifyThreshold: 5,
	}
	lim.Reconfigure(ts.Now(), args)
	if ok, _ := req.Acquire(ctx, &lim); !ok {
		t.Fatalf("failed to fulfill")
	}
	checkNotification()
	check("0.00 RU filling @ 0.00 RU/s")
}
