// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestCPUTimeTokenGranter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	granter := newCPUTimeTokenGranter(makeCPUTimeTokenMetrics(), timeutil.DefaultTimeSource{})
	var buf strings.Builder
	var lastGranterStateStr string
	requester := &testRequester{
		additionalID: "",
		granter:      granter,
		buf:          &buf,
	}
	granter.requester = requester
	requester.returnValueFromHasWaitingRequests = noBurst

	flushAndReset := func(init bool) string {
		granterStateStr := granter.formatBuckets()
		if granterStateStr != lastGranterStateStr {
			fmt.Fprint(&buf, granterStateStr)
		}
		lastGranterStateStr = granterStateStr
		if init {
			fmt.Fprint(&buf, "requester: "+requester.String()+"\n")
		}
		str := buf.String()
		buf.Reset()
		return str
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_granter"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			granter.mu.buckets[canBurst].tokens = 0
			granter.mu.buckets[noBurst].tokens = 0
			if d.HasArg("can_burst") {
				var n int64
				d.ScanArgs(t, "can_burst", &n)
				granter.mu.buckets[canBurst].tokens = n
			}
			if d.HasArg("no_burst") {
				var n int64
				d.ScanArgs(t, "no_burst", &n)
				granter.mu.buckets[noBurst].tokens = n
			}
			if d.HasArg("waiter") {
				var n int64
				d.ScanArgs(t, "waiter", &n)
				requester.waitingRequests = true
				requester.returnValueFromGranted = n
			} else {
				requester.waitingRequests = false
				requester.returnValueFromGranted = 0
			}
			if d.HasArg("burst_waiter") {
				if !d.HasArg("waiter") {
					panic("must set waiter")
				}
				requester.returnValueFromHasWaitingRequests = canBurst
			} else {
				requester.returnValueFromHasWaitingRequests = noBurst
			}
			return flushAndReset(true /* init */)

		case "try-get":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			qual := noBurst
			if d.HasArg("burst") {
				qual = canBurst
			}
			requester.tryGet(qual, int64(v))
			return flushAndReset(false /* init */)

		case "return-grant":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requester.returnGrant(int64(v))
			return flushAndReset(false /* init */)

		case "took-without-permission":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requester.tookWithoutPermission(int64(v))
			return flushAndReset(false /* init */)

		case "refill":
			// The delta, bucket capacity, & bucket minimums are hard-coded.
			var delta [numBurstQualifications]int64
			delta[canBurst] = 5
			delta[noBurst] = 4
			var bucketCapacity [numBurstQualifications]int64
			bucketCapacity[canBurst] = 20
			bucketCapacity[noBurst] = 16
			var bucketMins [numBurstQualifications]int64
			bucketMins[canBurst] = 0
			bucketMins[noBurst] = -4
			granter.refill(delta, bucketCapacity, bucketMins, true /* updateMetrics */)
			fmt.Fprint(&buf, "refill(\n")
			for qual := 0; qual < int(numBurstQualifications); qual++ {
				fmt.Fprintf(&buf, "\t%s -> delta: %v, cap: %v, min: %v\n",
					burstQualification(qual).String(), delta[qual],
					bucketCapacity[qual], bucketMins[qual])
			}
			fmt.Fprint(&buf, ")\n")
			return flushAndReset(false /* init */)

		case "reset-tokens-used":
			used := granter.resetTokensUsedInInterval()
			fmt.Fprintf(&buf, "reset-tokens-used-in-interval() returned %d\n", used)
			return flushAndReset(false /* init */)

		case "continue-grant-chain":
			requester.continueGrantChain()
			return flushAndReset(false /* init */)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func TestExhaustedDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	c := metric.NewCounter(metric.Metadata{Name: "test_exhausted_nanos"})
	tb := tokenBucket{
		tokens:            0,
		exhaustedStart:    t0,
		exhaustedDuration: c,
	}

	// Bucket starts at 0 (exhausted). Recover after 1s.
	tb.updateTokenCount(100, t0.Add(1*time.Second), true /* flushToMetricNow */)
	require.Equal(t, int64(time.Second), c.Count())

	// Exhaust again after 500ms.
	tb.updateTokenCount(-100, t0.Add(1500*time.Millisecond), false /* flushToMetricNow */)
	require.Equal(t, int64(time.Second), c.Count())

	// Recover after another 2s. Total = 1s + 2s = 3s.
	tb.updateTokenCount(200, t0.Add(3500*time.Millisecond), true /* flushToMetricNow */)
	require.Equal(t, int64(3*time.Second), c.Count())

	// Enter exhaustion again.
	tb.updateTokenCount(-50, t0.Add(4*time.Second), false /* flushToMetricNow */)
	require.Equal(t, int64(3*time.Second), c.Count())

	// Still exhausted, no flush tick — counter unchanged.
	tb.updateTokenCount(-100, t0.Add(5*time.Second), false /* flushToMetricNow */)
	require.Equal(t, int64(3*time.Second), c.Count())

	// Still exhausted, flush tick — flushes 2s since exhaustion started. Total = 5s.
	tb.updateTokenCount(-100, t0.Add(6*time.Second), true /* flushToMetricNow */)
	require.Equal(t, int64(5*time.Second), c.Count())
}
