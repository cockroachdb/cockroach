// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

	var tb tokenBucket
	tb.Init(ts.Now())
	tb.Reconfigure(ts.Now(), tokenBucketReconfigureArgs{NewRate: 10, NewTokens: 100})

	check := func(expected string) {
		t.Helper()
		tb.update(ts.Now())
		actual := tb.String(ts.Now())
		if actual != expected {
			t.Errorf("expected: %s\nactual: %s\n", expected, actual)
		}
	}
	check("100.00 tokens filling @ 10.00 tokens/s")

	// Verify basic update.
	ts.Advance(1 * time.Second)
	check("110.00 tokens filling @ 10.00 tokens/s")
	// Check RemoveTokens.
	tb.RemoveTokens(ts.Now(), 200)
	check("-70.00 tokens filling @ 10.00 tokens/s (20.00 waiting debt @ 10.00 tokens/s)")
	ts.Advance(1 * time.Second)
	check("-70.00 tokens filling @ 10.00 tokens/s (10.00 waiting debt @ 10.00 tokens/s)")
	tb.RemoveTokens(ts.Now(), 5)
	check("-70.00 tokens filling @ 10.00 tokens/s (15.00 waiting debt @ 7.50 tokens/s)")
	ts.Advance(1 * time.Second)
	check("-67.50 tokens filling @ 10.00 tokens/s (7.50 waiting debt @ 7.50 tokens/s)")
	tb.RemoveTokens(ts.Now(), 5)
	check("-67.50 tokens filling @ 10.00 tokens/s (12.50 waiting debt @ 6.25 tokens/s)")

	ts.Advance(15 * time.Second)
	check("70.00 tokens filling @ 10.00 tokens/s")

	fulfill := func(amount float64) {
		t.Helper()
		if ok, _ := tb.TryToFulfill(ts.Now(), amount); !ok {
			t.Fatalf("failed to fulfill")
		}
	}
	fulfill(40)
	check("30.00 tokens filling @ 10.00 tokens/s")

	// TryAgainAfter should be the time to reach 60 tokens.
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 60); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := 3 * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("30.00 tokens filling @ 10.00 tokens/s")

	// Create massive debt and ensure that delay is = maxTryAgainAfterSeconds.
	tb.RemoveTokens(ts.Now(), maxTryAgainAfterSeconds*10+60)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 0); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := maxTryAgainAfterSeconds * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("-10010.00 tokens filling @ 10.00 tokens/s (20.00 waiting debt @ 10.00 tokens/s)")
	ts.Advance(maxTryAgainAfterSeconds * time.Second)
	check("-30.00 tokens filling @ 10.00 tokens/s")

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
		NewRate: math.Inf(1),
	}
	tb.Reconfigure(ts.Now(), args)
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 0); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := time.Nanosecond; tryAgainAfter != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}

	// Set limit.
	args = tokenBucketReconfigureArgs{
		NewRate:   10,
		NewTokens: 100,
		MaxTokens: 50,
	}
	check("-30.00 tokens filling @ +Inf tokens/s")
	tb.Reconfigure(ts.Now(), args)
	check("50.00 tokens filling @ 10.00 tokens/s (limited to 50.00 tokens)")
	ts.Advance(time.Second)
	check("50.00 tokens filling @ 10.00 tokens/s (limited to 50.00 tokens)")
	ts.Advance(time.Second)
	tb.RemoveTokens(ts.Now(), 15)
	check("35.00 tokens filling @ 10.00 tokens/s (limited to 50.00 tokens)")
	ts.Advance(2 * time.Second)
	fulfill(10)
	check("40.00 tokens filling @ 10.00 tokens/s (limited to 50.00 tokens)")

	// Fulfill amount > limit.
	if ok, tryAgainAfter := tb.TryToFulfill(ts.Now(), 60); ok {
		t.Fatalf("fulfilled incorrectly")
	} else if exp := 2 * time.Second; tryAgainAfter.Round(time.Millisecond) != exp {
		t.Fatalf("tryAgainAfter: expected %s, got %s", exp, tryAgainAfter)
	}
	check("40.00 tokens filling @ 10.00 tokens/s (limited to 50.00 tokens)")
	ts.Advance(1 * time.Second)
	fulfill(60)
	check("0.00 tokens filling @ 10.00 tokens/s (limited to 50.00 tokens) (10.00 waiting debt @ 5.00 tokens/s)")
	ts.Advance(8 * time.Second)
	if available := tb.AvailableTokens(ts.Now()); available != 50.0 {
		t.Fatalf("AvailableTokens: expected %.2f, got %.2f", 50.0, available)
	}
}

// TestTokenBucketTryToFulfill verifies that the tryAgainAfter time returned by
// TryToFulfill is consistent if recalculated after some time has passed.
func TestTokenBucketTryToFulfill(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r, _ := randutil.NewPseudoRand()
	randTokens := func(min, max float64) float64 {
		return min + r.Float64()*(max-min)
	}
	randDuration := func(max time.Duration) time.Duration {
		return time.Duration(r.Intn(int(max + 1)))
	}

	start := timeutil.Now()
	const runs = 50000
	for run := 0; run < runs; run++ {
		var tb tokenBucket
		clock := timeutil.NewManualTime(start)
		tb.Init(clock.Now())
		tb.Reconfigure(clock.Now(), tokenBucketReconfigureArgs{
			NewRate:   randTokens(0, 500),
			NewTokens: randTokens(1, 100),
		})

		// Advance a random amount of time.
		clock.Advance(randDuration(100 * time.Millisecond))

		if rand.Intn(5) > 0 {
			// Add some debt and advance more.
			tb.RemoveTokens(clock.Now(), randTokens(1, 500))
			clock.Advance(randDuration(100 * time.Millisecond))
		}

		// Fulfill requests until we can't anymore.
		var ru float64
		var tryAgainAfter time.Duration
		for {
			ru = randTokens(0, 100)
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
	tb.Init(ts.Now())
	tb.Reconfigure(start, tokenBucketReconfigureArgs{NewRate: 100})

	const tickDuration = time.Millisecond
	const debtPeriod = time.Second
	const debtTicks = int(debtPeriod / tickDuration)
	const totalTicks = 10 * debtTicks
	const debt float64 = 50

	const reqTokensMean = 1
	const reqTokensStdDev = reqTokensMean / 2

	// Use a fixed seed so the result is reproducible.
	r := rand.New(rand.NewSource(1234))
	randRu := func() float64 {
		ru := r.NormFloat64()*reqTokensStdDev + reqTokensMean
		if ru < 0 {
			return 0
		}
		return ru
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

	fmt.Fprintf(out, "time (s),queue latency (ms),debt applied (tokens)\n")
	for tick := 0; tick < totalTicks; tick++ {
		now := ts.Now()
		var d float64
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
