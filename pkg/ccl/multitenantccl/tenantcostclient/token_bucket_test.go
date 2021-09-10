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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

	// Check AdjustTokens.
	tb.AdjustTokens(ts.Now(), -200)
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
		TokenAdjustment: 10,
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
		TokenAdjustment: 80,
		NewRate:         1,
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
