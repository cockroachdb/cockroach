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
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
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
	lim.Reconfigure(start, tokenBucketReconfigureArgs{NewRate: 100})

	const tickDuration = time.Millisecond
	const debtPeriod = time.Second
	const debtTicks = int(debtPeriod / tickDuration)
	const totalTicks = 10 * debtTicks
	const debt tenantcostmodel.RU = 50

	const reqRUMean = 2.0
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
	opsCount := 0
	ctx := context.Background()

	fmt.Fprintf(out, "time (s), RUs requested, queue latency (ms), debt applied (RU), available RUs, extra RUs\n")
	for tick := 0; tick < totalTicks; tick++ {
		var d tenantcostmodel.RU
		if tick > 0 && tick%debtTicks == 0 {
			d = debt
			lim.OnTick(ts.Now(), d)
			opsCount = 0
		}

		if req != nil {
			if tenantcostmodel.RU(req.needed) <= lim.AvailableRU(ts.Now()) {
				// This should not block, since we checked above there are enough
				// tokens.
				if err := lim.qp.Acquire(ctx, req); err != nil {
					t.Fatalf("error during Acquire: %v", err)
				}

				opsCount++
				latency := tick - reqTick
				if latency > 100 {
					// A single request took longer than 100ms; we did a poor job smoothing
					// out the debt.
					t.Fatalf("high latency for request: %d ms", latency)
				}
				fmt.Fprintf(out, "%8.3f,%14.2f,%19d,%18.1f,%14.2f,%10.2f\n",
					(time.Duration(tick) * tickDuration).Seconds(),
					req.needed, latency, d, lim.AvailableRU(ts.Now()), lim.debtRU())

				req = nil
			} else {
				fmt.Fprintf(out, "%8.3f,              ,                   ,%18.1f,%14.2f,%10.2f\n",
					(time.Duration(tick) * tickDuration).Seconds(),
					d, lim.AvailableRU(ts.Now()), lim.debtRU())
			}
		}

		if req == nil {
			// Create a request now.
			req = &waitRequest{needed: quotapool.Tokens(randRu() + lim.amortizeDebtRU())}
			reqTick = tick
		}

		ts.Advance(tickDuration)
	}
}
