// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var saveDebtCSV = flag.String(
	"save-debt-csv", "",
	"save latency data from TestTokenBucketDebt to a csv file",
)

func TestTokenBucket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	ch := make(chan struct{}, 100)

	var tb tokenBucket
	tb.Init(ts.Now(), ch, 10 /* rate */, 100 /* available */)

	check := func(expected float64) {
		t.Helper()
		available := float64(tb.AvailableTokens(ts.Now()))
		delta := math.Abs(available - expected)
		if delta > 1e-5 {
			t.Errorf("expected %g tokens, got %g", expected, available)
		}
	}
	check(100)

	// Verify basic update.
	ts.Advance(1 * time.Second)
	check(110)

	// Check RemoveTokens.
	tb.RemoveTokens(ts.Now(), 200)
	check(-90)

	ts.Advance(1 * time.Second)
	check(-80)
	ts.Advance(15 * time.Second)
	check(70)

	fulfill := func(amount tenantcostmodel.RU) {
		t.Helper()
		if ok, _ := tb.TryToFulfill(ts.Now(), amount); !ok {
			t.Fatalf("failed to fulfill")
		}
	}
	fulfill(50)
	check(20)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 40); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := 2 * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check(20)

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
	args := tokenBucketReconfigureArgs{
		NewRate:         10,
		NotifyThreshold: 5,
	}
	tb.Reconfigure(ts.Now(), args)

	checkNoNotification()
	ts.Advance(1 * time.Second)
	check(30)
	fulfill(20)
	// No notification: we did not go below the threshold.
	checkNoNotification()
	// Now we should get a notification.
	fulfill(8)
	checkNotification()
	check(2)

	// We only get one notification (until we Reconfigure or StartNotification).
	fulfill(1)
	checkNoNotification()

	// Verify that we get notified when we block, even if the current amount is
	// above the threshold.
	args = tokenBucketReconfigureArgs{
		NewTokens:       10,
		NewRate:         10,
		NotifyThreshold: 5,
	}
	tb.Reconfigure(ts.Now(), args)
	check(11)
	if ok, _ := tb.TryToFulfill(ts.Now(), 38); ok {
		t.Fatalf("fulfilled incorrectly")
	}
	checkNotification()

	args = tokenBucketReconfigureArgs{
		NewTokens: 80,
		NewRate:   1,
	}
	tb.Reconfigure(ts.Now(), args)
	check(91)
	ts.Advance(1 * time.Second)

	checkNoNotification()
	tb.SetupNotification(ts.Now(), 50)
	checkNoNotification()
	fulfill(60)
	checkNotification()
}

// TestTokenBucketTryToFulfill verifies that the tryAgainAfter time returned by
// TryToFulfill is consistent if recalculated after some time has passed.
func TestTokenBucketTryToFulfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, _ := randutil.NewPseudoRand()
	randRU := func(min, max float64) tenantcostmodel.RU {
		return tenantcostmodel.RU(min + r.Float64()*(max-min))
	}
	randDuration := func(max time.Duration) time.Duration {
		return time.Duration(r.Intn(int(max + 1)))
	}

	start := timeutil.Now()
	const runs = 50000
	for run := 0; run < runs; run++ {
		var tb tokenBucket
		clock := timeutil.NewManualTime(start)
		tb.Init(clock.Now(), nil, randRU(1, 100), randRU(0, 500))

		// Advance a random amount of time.
		clock.Advance(randDuration(100 * time.Millisecond))

		if rand.Intn(5) > 0 {
			// Add some debt and advance more.
			tb.RemoveTokens(clock.Now(), randRU(1, 500))
			clock.Advance(randDuration(100 * time.Millisecond))
		}

		// Fulfill requests until we can't anymore.
		var ru tenantcostmodel.RU
		var tryAgainAfter time.Duration
		for {
			ru = randRU(0, 100)
			var ok bool
			ok, tryAgainAfter = tb.TryToFulfill(clock.Now(), ru)
			if !ok {
				break
			}
		}
		if tryAgainAfter == maxTryAgainAfterSeconds*time.Second {
			// TryToFullfill has a cap; we cannot crosscheck the value if that cap is
			// hit.
			continue
		}
		state := tb
		// Now check that if we advance the time a bit, the tryAgainAfter time
		// agrees with the previous one.
		advance := randDuration(tryAgainAfter)
		clock.Advance(advance)
		ok, newTryAgainAfter := tb.TryToFulfill(clock.Now(), ru)
		if ok {
			newTryAgainAfter = 0
		}
		// Check that the two calls agree on when the request can go through.
		diff := advance + newTryAgainAfter - tryAgainAfter
		const tolerance = 10 * time.Nanosecond
		if diff < -tolerance || diff > tolerance {
			t.Fatalf(
				"inconsistent tryAgainAfter\nstate: %+v\nru: %f\ntryAgainAfter: %s\ntryAgainAfter after %s: %s",
				state, ru, tryAgainAfter, advance, newTryAgainAfter,
			)
		}
	}
}

// TestTokenBucketDebt simulates a closed-loop workload with parallelism 1 in
// combination with incurring a fixed amount of debt every second. It verifies
// that queue times are never too high, and optionally emits all queue time
// information into a csv file.
func TestTokenBucketDebt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	var tb tokenBucket
	tb.Init(ts.Now(), nil, 100 /* rate */, 0 /* available */)

	const tickDuration = time.Millisecond
	const debtPeriod = time.Second
	const debtTicks = int(debtPeriod / tickDuration)
	const totalTicks = 10 * debtTicks
	const debt tenantcostmodel.RU = 50

	const reqRUMean = 1
	const reqRUStdDev = reqRUMean / 2

	// Use a fixed seed so the result is reproducible.
	r := rand.New(rand.NewSource(1234))
	randRu := func() tenantcostmodel.RU {
		ru := r.NormFloat64()*reqRUStdDev + reqRUMean
		if ru < 0 {
			return 0
		}
		return tenantcostmodel.RU(ru)
	}

	ru := randRu()
	reqTick := 0

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

	fmt.Fprintf(out, "time (s),queue latency (ms),debt applied (RU)\n")
	for tick := 0; tick < totalTicks; tick++ {
		now := ts.Now()
		var d tenantcostmodel.RU
		if tick > 0 && tick%debtTicks == 0 {
			d = debt
			tb.RemoveTokens(now, debt)
		}
		if ok, _ := tb.TryToFulfill(now, ru); ok {
			latency := tick - reqTick
			if latency > 100 {
				// A single request took longer than 100ms; we did a poor job smoothing
				// out the debt.
				t.Fatalf("high latency for request: %d ms", latency)
			}
			fmt.Fprintf(out, "%f,%d,%f\n", (time.Duration(tick) * tickDuration).Seconds(), latency, d)
			ru = randRu()
			reqTick = tick
		} else {
			fmt.Fprintf(out, "%f,,%f\n", (time.Duration(tick) * tickDuration).Seconds(), d)
		}
		ts.Advance(tickDuration)
	}
}
