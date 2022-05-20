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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var saveDebtCSV = flag.String(
	"save-debt-csv", "",
	"save latency data from TestTokenBucketDebt to a csv file",
)

// TestTokenBucketDebt simulates a closed-loop workload with parallelism 1 in
// combination with incurring a fixed amount of debt every second. It verifies
// that queue times are never too high, and optionally emits all queue time
// information into a csv file.
func TestLimiterDebt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	var lim limiter
	lim.Init(ts, nil /* notifyChan */)
	lim.Reconfigure(start, limiterReconfigureArgs{NewRate: 100})

	const tickDuration = time.Millisecond
	const debtPeriod = time.Second
	const debtTicks = int(debtPeriod / tickDuration)
	const totalTicks = 10 * debtTicks
	const debt tenantcostmodel.RU = 50

	const reqRUMean = 1.0
	const reqRUStdDev = reqRUMean / 2.0

	// Use a fixed seed so the result is reproducible.
	r := rand.New(rand.NewSource(1234))
	randRu := func() tenantcostmodel.RU {
		ru := r.NormFloat64()*reqRUStdDev + reqRUMean
		if ru < 0 {
			return 0
		}
		return tenantcostmodel.RU(ru)
	}

	out := io.Discard
	if *saveDebtCSV != "" {
		file, err := os.Create(*saveDebtCSV)
		if err != nil {
			t.Fatalf("error creating csv file: %v", err)
		}
		defer func() {
			if err := file.Close(); err != nil {
				t.Errorf("error closing csv file: %v", err)
			}
		}()
		out = file
	}

	var req *waitRequest
	reqTick := 0
	var kvRU tenantcostmodel.RU
	ctx := context.Background()

	fmt.Fprintf(out, "time (s), KV RU, SQL RU, queue latency (ms), SQL RU remaining, tokens remaining\n")
	for tick := 0; tick < totalTicks; tick++ {
		var sqlRU tenantcostmodel.RU
		if tick > 0 && tick%debtTicks == 0 {
			sqlRU = debt
			lim.OnTick(ts.Now(), sqlRU, kvRU)
			kvRU = 0
		}

		if req == nil {
			// Create a request now.
			req = &waitRequest{kvRU: randRu(), sqlRU: -1}
			kvRU += req.kvRU
			reqTick = tick
		}

		if fulfilled, _ := req.Acquire(ctx, &lim); fulfilled {
			latency := tick - reqTick
			if latency > 100 {
				// A single request took longer than 100ms; we did a poor job
				// smoothing out the debt.
				t.Fatalf("high latency for request: %d ms", latency)
			}
			fmt.Fprintf(out, "%8.3f,%6.2f,%8.1f,%18d,%17.2f,%17.2f\n",
				(time.Duration(tick) * tickDuration).Seconds(),
				req.kvRU, req.sqlRU, latency, lim.qp.sqlRU, lim.qp.tb.current)

			req = nil
		} else {
			fmt.Fprintf(out, "%8.3f,      ,        ,                  ,%17.2f,%17.2f\n",
				(time.Duration(tick) * tickDuration).Seconds(), lim.qp.sqlRU, lim.qp.tb.current)
		}

		ts.Advance(tickDuration)
	}
}

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
	check("30.00 RU filling @ 10.00 RU/s (0.00 SQL RU, 0.00 waiting RU, 0.00 SQL/KV ratio)")

	lim.RemoveRU(ts.Now(), 20)
	// No notification: we did not go below the threshold.
	checkNoNotification()
	// Now we should get a notification.
	lim.RemoveRU(ts.Now(), 8)
	checkNotification()
	check("2.00 RU filling @ 10.00 RU/s (0.00 SQL RU, 0.00 waiting RU, 0.00 SQL/KV ratio)")

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
	check("2.00 RU filling @ 0.00 RU/s (0.00 SQL RU, 0.00 waiting RU, 0.00 SQL/KV ratio)")

	// Reconfigure with enough tokens to exceed threshold and ensure there is no
	// notification.
	args = limiterReconfigureArgs{
		NewTokens: 80,
		NewRate:   1,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("82.00 RU filling @ 1.00 RU/s (0.00 SQL RU, 0.00 waiting RU, 0.00 SQL/KV ratio)")

	// Call SetupNotification with a high threshold and ensure notification.
	lim.SetupNotification(ts.Now(), 83)
	checkNotification()

	// And then with lower threshold, and ensure no notification.
	lim.SetupNotification(ts.Now(), 40)
	checkNoNotification()

	// Set consumed SQL RU and KV RU per tick in the limiter.
	lim.OnTick(ts.Now(), 25, 50)
	checkNoNotification()
	check("82.00 RU filling @ 1.00 RU/s (25.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")

	// Ensure notification when more SQL RU is consumed.
	lim.OnTick(ts.Now(), 25, 50)
	checkNotification()
	check("57.00 RU filling @ 1.00 RU/s (25.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")

	// Refill bucket.
	args = limiterReconfigureArgs{
		NewTokens:       23,
		NewRate:         10,
		NotifyThreshold: 40,
	}
	lim.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("80.00 RU filling @ 10.00 RU/s (25.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")

	// Try a fulfilled Acquire that doesn't drop below threshold.
	fulfill := func(amount tenantcostmodel.RU) {
		t.Helper()
		req := &waitRequest{kvRU: amount, sqlRU: -1}
		if ok, _ := req.Acquire(ctx, &lim); !ok {
			t.Fatalf("failed to fulfill")
		}
	}

	fulfill(10)
	checkNoNotification()
	check("65.00 RU filling @ 10.00 RU/s (20.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")

	// Try a fulfilled Acquire that does drop below the threshold (when SQL RU
	// is accounted for).
	fulfill(10)
	checkNotification()
	check("50.00 RU filling @ 10.00 RU/s (15.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")

	// Refill bucket.
	ts.Advance(5 * time.Second)
	fulfill(0)
	checkNoNotification()
	check("100.00 RU filling @ 10.00 RU/s (15.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")
	lim.SetupNotification(ts.Now(), 5)

	// Fail to fulfill a request and expect a notification.
	req := &waitRequest{kvRU: 100, sqlRU: -1}
	if ok, _ := req.Acquire(ctx, &lim); ok {
		t.Fatalf("fulfilled incorrectly")
	}
	checkNotification()
	check("100.00 RU filling @ 10.00 RU/s (0.00 SQL RU, 115.00 waiting RU, 0.50 SQL/KV ratio)")

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
	check("0.00 RU filling @ 0.00 RU/s (0.00 SQL RU, 0.00 waiting RU, 0.50 SQL/KV ratio)")
}
