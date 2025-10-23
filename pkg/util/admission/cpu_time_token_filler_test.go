// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
)

func TestCPUTimeTokenFiller(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Fixed time for reproducibility.
	unixNanos := int64(1758938600000000000) // 2025-09-24T14:30:00Z
	startTime := time.Unix(0, unixNanos).UTC()
	testTime := timeutil.NewManualTime(startTime)

	var buf strings.Builder
	allocator := testTokenAllocator{buf: &buf}
	var filler cpuTimeTokenFiller
	flushAndReset := func() string {
		fmt.Fprintf(&buf, "elapsed: %s\n", testTime.Since(startTime))
		str := buf.String()
		buf.Reset()
		return str
	}

	tickCh := make(chan struct{})
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_filler"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			filler = cpuTimeTokenFiller{
				allocator:  &allocator,
				closeCh:    make(chan struct{}),
				timeSource: testTime,
				tickCh:     &tickCh,
			}
			filler.start()
			return flushAndReset()
		case "advance":
			var dur time.Duration
			d.ScanArgs(t, "dur", &dur)
			testTime.AdvanceInOneTick(dur)
			<-tickCh
			return flushAndReset()
		case "stop":
			close(filler.closeCh)
			return flushAndReset()
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}

type testTokenAllocator struct {
	buf *strings.Builder
}

func (a *testTokenAllocator) resetInterval() {
	fmt.Fprintf(a.buf, "resetInterval()\n")
}

func (a *testTokenAllocator) allocateTokens(remainingTicks int64) {
	fmt.Fprintf(a.buf, "allocateTokens(%d)\n", remainingTicks)
}

func TestCPUTimeTokenAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	granter := &cpuTimeTokenGranter{}
	tier0Granter := &cpuTimeTokenChildGranter{
		tier:   testTier0,
		parent: granter,
	}
	tier1Granter := &cpuTimeTokenChildGranter{
		tier:   testTier1,
		parent: granter,
	}
	var requesters [numResourceTiers]*testRequester
	requesters[testTier0] = &testRequester{
		additionalID: "tier0",
		granter:      tier0Granter,
	}
	requesters[testTier1] = &testRequester{
		additionalID: "tier1",
		granter:      tier1Granter,
	}
	granter.requester[testTier0] = requesters[testTier0]
	granter.requester[testTier1] = requesters[testTier1]

	allocator := cpuTimeTokenAllocator{
		granter: granter,
	}
	allocator.rates[testTier0][canBurst] = 5
	allocator.rates[testTier0][noBurst] = 4
	allocator.rates[testTier1][canBurst] = 3
	allocator.rates[testTier1][noBurst] = 2
	allocator.bucketCapacity = allocator.rates

	var buf strings.Builder
	flushAndReset := func(printGranter bool) string {
		if printGranter {
			fmt.Fprint(&buf, granter.String())
		}
		str := buf.String()
		buf.Reset()
		return str
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "cpu_time_token_allocator"), func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "resetInterval":
			allocator.resetInterval()
			return flushAndReset(false /* printGranter */)
		case "allocate":
			var remainingTicks int64
			d.ScanArgs(t, "remaining", &remainingTicks)
			allocator.allocateTokens(remainingTicks)
			return flushAndReset(true /* printGranter */)
		case "clear":
			granter.mu.buckets[testTier0][canBurst].tokens = 0
			granter.mu.buckets[testTier0][noBurst].tokens = 0
			granter.mu.buckets[testTier1][canBurst].tokens = 0
			granter.mu.buckets[testTier1][noBurst].tokens = 0
			return flushAndReset(true /* printGranter */)
		default:
			return fmt.Sprintf("unknown command: %s", d.Cmd)
		}
	})
}
