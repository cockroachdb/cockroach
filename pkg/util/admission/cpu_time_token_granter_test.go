// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

const (
	testTier0 resourceTier = iota
	testTier1
)

func TestCPUTimeTokenGranter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var requesters [numResourceTiers]*testRequester
	granter := &cpuTimeTokenGranter{}
	tier0Granter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: granter,
	}
	tier1Granter := &cpuTimeTokenChildGranter{
		tier:   testTier1,
		parent: granter,
	}
	var buf strings.Builder
	var lastGranterStateStr string
	flushAndReset := func(init bool) string {
		granterStateStr := granter.String()
		if granterStateStr != lastGranterStateStr {
			fmt.Fprint(&buf, granterStateStr)
		}
		lastGranterStateStr = granterStateStr
		if init {
			fmt.Fprint(&buf, "tier0requester: "+requesters[0].String()+"\n")
			fmt.Fprint(&buf, "tier1requester: "+requesters[1].String()+"\n")
		}
		str := buf.String()
		buf.Reset()
		return str
	}
	requesters[testTier0] = &testRequester{
		additionalID: "tier0",
		granter:      tier0Granter,
		buf:          &buf,
	}
	requesters[testTier1] = &testRequester{
		additionalID: "tier1",
		granter:      tier1Granter,
		buf:          &buf,
	}
	granter.requester[testTier0] = requesters[testTier0]
	granter.requester[testTier1] = requesters[testTier1]

	requesters[testTier0].returnValueFromHasWaitingRequests = noBurst

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_granter"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			granter.mu.buckets[testTier0][canBurst].tokens = 0
			granter.mu.buckets[testTier0][noBurst].tokens = 0
			granter.mu.buckets[testTier1][canBurst].tokens = 0
			granter.mu.buckets[testTier1][noBurst].tokens = 0
			if d.HasArg("tier0burst") {
				var n int64
				d.ScanArgs(t, "tier0burst", &n)
				granter.mu.buckets[testTier0][canBurst].tokens = n
			}
			if d.HasArg("tier0") {
				var n int64
				d.ScanArgs(t, "tier0", &n)
				granter.mu.buckets[testTier0][noBurst].tokens = n
			}
			if d.HasArg("tier1burst") {
				var n int64
				d.ScanArgs(t, "tier1burst", &n)
				granter.mu.buckets[testTier1][canBurst].tokens = n
			}
			if d.HasArg("tier1") {
				var n int64
				d.ScanArgs(t, "tier1", &n)
				granter.mu.buckets[testTier1][noBurst].tokens = n
			}
			if d.HasArg("tier0waiter") {
				var n int64
				d.ScanArgs(t, "tier0waiter", &n)
				requesters[testTier0].waitingRequests = true
				requesters[testTier0].returnValueFromGranted = n
			} else {
				requesters[testTier0].waitingRequests = false
				requesters[testTier0].returnValueFromGranted = 0
			}
			if d.HasArg("tier1waiter") {
				var n int64
				d.ScanArgs(t, "tier1waiter", &n)
				requesters[testTier1].waitingRequests = true
				requesters[testTier1].returnValueFromGranted = n
			} else {
				requesters[testTier1].waitingRequests = false
				requesters[testTier1].returnValueFromGranted = 0
			}
			if d.HasArg("tier1burstwaiter") {
				if !d.HasArg("tier1waiter") {
					panic("must set tier1waiter")
				}
				requesters[testTier1].returnValueFromHasWaitingRequests = canBurst
			} else {
				requesters[testTier1].returnValueFromHasWaitingRequests = noBurst
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
			requesters[scanResourceTier(t, d)].tryGet(qual, int64(v))
			return flushAndReset(false /* init */)

		case "return-grant":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requesters[scanResourceTier(t, d)].returnGrant(int64(v))
			return flushAndReset(false /* init */)

		case "took-without-permission":
			v := 1
			if d.HasArg("v") {
				d.ScanArgs(t, "v", &v)
			}
			requesters[scanResourceTier(t, d)].tookWithoutPermission(int64(v))
			return flushAndReset(false /* init */)

		case "refill":
			// The delta & the bucket capacity are hard-coded. It is unwiedly
			// to make them data-driven arguments, and the payoff would be
			// low anyway.
			var delta [numResourceTiers][numBurstQualifications]int64
			delta[testTier0][canBurst] = 5
			delta[testTier0][noBurst] = 4
			delta[testTier1][canBurst] = 3
			delta[testTier1][noBurst] = 1
			var bucketCapacity [numResourceTiers][numBurstQualifications]int64
			bucketCapacity[testTier0][canBurst] = 4
			bucketCapacity[testTier0][noBurst] = 3
			bucketCapacity[testTier1][canBurst] = 10
			bucketCapacity[testTier1][noBurst] = 1
			granter.refill(delta, bucketCapacity)
			fmt.Fprintf(&buf, "refill(%v %v)\n", delta, bucketCapacity)
			return flushAndReset(false /* init */)

		// For cpuTimeTokenChildGranter, this is a NOP. Still, it will be
		// called in production. So best to test it doesn't panic, or similar.
		case "continue-grant-chain":
			requesters[scanResourceTier(t, d)].continueGrantChain()
			return flushAndReset(false /* init */)

		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

func scanResourceTier(t *testing.T, d *datadriven.TestData) resourceTier {
	var kindStr string
	d.ScanArgs(t, "tier", &kindStr)
	switch kindStr {
	case "tier0":
		return testTier0
	case "tier1":
		return testTier1
	}
	panic("unknown resourceTier")
}
