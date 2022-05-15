// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package tenantcostclient

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestTokenBucket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	start := timeutil.Now()
	ts := timeutil.NewManualTime(start)

	ch := make(chan struct{}, 100)

	var tb tokenBucket
	tb.Init(ts.Now(), ch)
	tb.Reconfigure(ts.Now(), tokenBucketReconfigureArgs{NewRate: 10, NewTokens: 100})

	check := func(expected string) {
		t.Helper()
		tb.update(ts.Now())
		actual := tb.String()
		if actual != expected {
			t.Errorf("expected: %s\nactual: %s\n", expected, actual)
		}
	}
	check("100.00 RU filling @ 10.00 RU/s")

	// Verify basic update.
	ts.Advance(1 * time.Second)
	check("110.00 RU filling @ 10.00 RU/s")
	// Check RemoveRU.
	tb.RemoveTokens(ts.Now(), 200)
	check("-90.00 RU filling @ 10.00 RU/s")

	ts.Advance(1 * time.Second)
	check("-80.00 RU filling @ 10.00 RU/s")
	ts.Advance(15 * time.Second)
	check("70.00 RU filling @ 10.00 RU/s")

	fulfill := func(amount tenantcostmodel.RU) {
		t.Helper()
		if ok, _ := tb.TryToFulfill(ts.Now(), amount); !ok {
			t.Fatalf("failed to fulfill")
		}
	}
	fulfill(100)
	check("-30.00 RU filling @ 10.00 RU/s")

	// TryAgainAfter should be the time to pay off entire debt.
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 0); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := 3 * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("-30.00 RU filling @ 10.00 RU/s")

	// Create massive debt and ensure that delay is = maxTryAgainAfterSeconds.
	tb.RemoveTokens(ts.Now(), maxTryAgainAfterSeconds*10)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 0); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := maxTryAgainAfterSeconds * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("-10030.00 RU filling @ 10.00 RU/s")
	ts.Advance(maxTryAgainAfterSeconds * time.Second)
	check("-30.00 RU filling @ 10.00 RU/s")

	// Set zero rate.
	args := tokenBucketReconfigureArgs{
		NewRate: 0,
	}
	tb.Reconfigure(ts.Now(), args)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 0); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := maxTryAgainAfterSeconds * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}

	// Set infinite rate.
	args = tokenBucketReconfigureArgs{
		NewRate: tenantcostmodel.RU(math.Inf(1)),
	}
	tb.Reconfigure(ts.Now(), args)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 0); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := time.Nanosecond; tryAgainAfter != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
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
	args = tokenBucketReconfigureArgs{
		NewTokens:       50,
		NewRate:         10,
		NotifyThreshold: 5,
	}
	tb.Reconfigure(ts.Now(), args)

	checkNoNotification()
	ts.Advance(1 * time.Second)
	check("30.00 RU filling @ 10.00 RU/s")
	fulfill(20)
	// No notification: we did not go below the threshold.
	checkNoNotification()
	// Now we should get a notification.
	fulfill(8)
	checkNotification()
	check("2.00 RU filling @ 10.00 RU/s")

	// We only get one notification (until we Reconfigure or StartNotification).
	fulfill(1)
	checkNoNotification()

	// Reconfigure without enough tokens to meet the threshold and ensure we get
	// a notification.
	args = tokenBucketReconfigureArgs{
		NewTokens:       1,
		NotifyThreshold: 5,
	}
	tb.Reconfigure(ts.Now(), args)
	checkNotification()
	check("2.00 RU filling @ 0.00 RU/s")

	// Reconfigure with enough tokens to exceed threshold and ensure there is no
	// notification.
	args = tokenBucketReconfigureArgs{
		NewTokens: 80,
		NewRate:   1,
	}
	tb.Reconfigure(ts.Now(), args)
	checkNoNotification()
	check("82.00 RU filling @ 1.00 RU/s")

	// Call SetupNotification with a high threshold and ensure notification.
	tb.SetupNotification(ts.Now(), 83)
	checkNotification()

	// And then with low threshold, and ensure no notification.
	tb.SetupNotification(ts.Now(), 20)
	checkNoNotification()

	// Remove tokens and ensure notification.
	tb.RemoveTokens(ts.Now(), 70)
	checkNotification()
	check("12.00 RU filling @ 1.00 RU/s")
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
		tb.Init(clock.Now(), nil)
		tb.Reconfigure(clock.Now(), tokenBucketReconfigureArgs{
			NewRate:   randRU(0, 500),
			NewTokens: randRU(1, 100),
		})

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
