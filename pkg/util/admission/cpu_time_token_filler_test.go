package admission

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCPUTimeTokenFiller(t *testing.T) {
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

	// Fixed time for reproducibility.
	unixNanos := int64(1758938600000000000) // 2025-09-24T14:30:00Z
	testTime := timeutil.NewManualTime(time.Unix(0, unixNanos).UTC())
	tickCh := make(chan struct{})
	adjuster := cpuTimeTokenFiller{
		granter:     granter,
		closeCh:     make(chan struct{}),
		timeSource:  testTime,
		timePerTick: 200 * time.Millisecond,
		tickCh:      &tickCh,
	}
	adjuster.rates[testTier0][canBurst] = 5
	adjuster.rates[testTier0][noBurst] = 4
	adjuster.rates[testTier1][canBurst] = 3
	adjuster.rates[testTier1][noBurst] = 2
	adjuster.burstBudget = adjuster.rates
	adjuster.start()

	// 200ms x5 -> 1s. At end of 1s, buckets should be full.
	for i := 0; i < 5; i++ {
		testTime.Advance(200 * time.Millisecond)
		<-tickCh
		granter.mu.Lock()
		// Buckets should gradually fill up, adding tokens on each tick. On
		// final tick, there are no more tokens to add.
		if i == 4 {
			require.Equal(t, int64(4), granter.mu.buckets[testTier0][noBurst].tokens)
		} else {
			require.Equal(t, int64(i+1), granter.mu.buckets[testTier0][noBurst].tokens)
		}
		granter.mu.Unlock()
	}

	granter.mu.Lock()
	require.Equal(t, adjuster.rates[testTier0][canBurst], granter.mu.buckets[testTier0][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier0][noBurst], granter.mu.buckets[testTier0][noBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][canBurst], granter.mu.buckets[testTier1][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][noBurst], granter.mu.buckets[testTier1][noBurst].tokens)

	granter.mu.buckets[testTier0][canBurst].tokens = 0
	granter.mu.buckets[testTier0][noBurst].tokens = 0
	granter.mu.buckets[testTier1][canBurst].tokens = 0
	granter.mu.buckets[testTier1][noBurst].tokens = 0
	granter.mu.Unlock()

	// Above clear is a test that buckets are refilled at adjuster.rates
	// every 1s, not just on the first interval (see comments in start
	// about how time is split into intervals for more).
	for i := 0; i < 5; i++ {
		testTime.Advance(200 * time.Millisecond)
		<-tickCh
	}
	granter.mu.Lock()
	require.Equal(t, adjuster.rates[testTier0][canBurst], granter.mu.buckets[testTier0][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier0][noBurst], granter.mu.buckets[testTier0][noBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][canBurst], granter.mu.buckets[testTier1][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][noBurst], granter.mu.buckets[testTier1][noBurst].tokens)

	granter.mu.buckets[testTier0][canBurst].tokens = 0
	granter.mu.buckets[testTier0][noBurst].tokens = 0
	granter.mu.buckets[testTier1][canBurst].tokens = 0
	granter.mu.buckets[testTier1][noBurst].tokens = 0
	granter.mu.Unlock()

	// In the presence of delayed and dropped ticks, the correct number of tokens will
	// be added to the buckets; they just may be added in a less smooth fashion than
	// normal. Thus, we expect the buckets to be full again, after a single 1s tick.
	testTime.AdvanceInOneTick(time.Second)
	<-tickCh

	granter.mu.Lock()
	require.Equal(t, adjuster.rates[testTier0][canBurst], granter.mu.buckets[testTier0][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier0][noBurst], granter.mu.buckets[testTier0][noBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][canBurst], granter.mu.buckets[testTier1][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][noBurst], granter.mu.buckets[testTier1][noBurst].tokens)

	granter.mu.buckets[testTier0][canBurst].tokens = 0
	granter.mu.buckets[testTier0][noBurst].tokens = 0
	granter.mu.buckets[testTier1][canBurst].tokens = 0
	granter.mu.buckets[testTier1][noBurst].tokens = 0
	granter.mu.Unlock()

	// Another test of delayed and dropped ticks. In this case, we tick 4 times in an
	// interval instead of 5. That is, a single tick is dropped.
	testTime.AdvanceInOneTick(400 * time.Millisecond)
	<-tickCh
	testTime.AdvanceInOneTick(200 * time.Millisecond)
	<-tickCh
	testTime.AdvanceInOneTick(200 * time.Millisecond)
	<-tickCh
	testTime.AdvanceInOneTick(200 * time.Millisecond)
	<-tickCh

	granter.mu.Lock()
	require.Equal(t, adjuster.rates[testTier0][canBurst], granter.mu.buckets[testTier0][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier0][noBurst], granter.mu.buckets[testTier0][noBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][canBurst], granter.mu.buckets[testTier1][canBurst].tokens)
	require.Equal(t, adjuster.rates[testTier1][noBurst], granter.mu.buckets[testTier1][noBurst].tokens)
	granter.mu.Unlock()

	close(adjuster.closeCh)
}
