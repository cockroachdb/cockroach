// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import "github.com/cockroachdb/redact"

// cpuTimeBurstBucket is a per-tenant token bucket that determines whether a
// tenant qualifies for burst priority. If a tenant qualifies, two things
// happen:
//
//  1. In the WorkQueue, its work sorts above the work of tenants that don't
//     qualify for burst priority.
//  2. It has access to more CPU time tokens than tenants that don't qualify
//     (see cpu_time_token_granter.go for more).
//
// These two things are related. Since canBurst tenants have access to more
// CPU time than noBurst tenants, it is important that work from a
// canBurst tenant always sorts before work from a noBurst tenant -- else
// available capacity is left on the table.
//
// The bucket works as follows:
//   - Tokens are added periodically via refill(), called by
//     cpuTimeTokenAllocator.
//   - Tokens are deducted when work is admitted, etc. via adjust().
//   - A tenant qualifies for burst (canBurst) when bucket is > 90% full.
//   - The bucket can go negative (down to -capacity/4) to allow recovery.
//
// The bucket capacity is derived from the noBurst refill rate in
// cpuTimeTokenAllocator (capacity = noBurst_refill_rate / 4).
// With cluster settings at their default values, this implies that
// an application tenant can burst, if they are using roughly less
// than 20% of the CPU on a CRDB node (0.8 * 0.25 = 0.2).
type cpuTimeBurstBucket struct {
	tokens   int64
	capacity int64
	// disabled is true when mode != usesCPUTimeTokens, causing
	// burstQualification to always return noBurst. This effectively
	// disables the burstQualification functionality.
	disabled bool
}

func (m *cpuTimeBurstBucket) init(capacity int64, disabled bool) {
	// The bucket of a new tenant is inited full. This implies that
	// a tenant can burst when its work first appears on a KV node.
	// After <= 1s, the bucket state should track the usage of the
	// tenant accurately.
	*m = cpuTimeBurstBucket{tokens: capacity, capacity: capacity, disabled: disabled}
}

// burstQualification returns whether this tenant qualifies for burst
// priority. See the comments above cpuTimeBurstBucket for more.
func (m *cpuTimeBurstBucket) burstQualification() burstQualification {
	if m.disabled {
		return noBurst
	}
	// Note that at CRDB startup time, the capacity that is passed into
	// cpuTimeBurstBucket.init will be zero, until 1ms passes, and the
	// first call to refillBurstBuckets is made by cpuTimeTokenAllocator.
	// So it is important that this code does not assume that m.capacity
	// is non-zero. (There is a test for this case in
	// TestCPUTimeTokenBurst.)
	if m.tokens > (m.capacity*9)/10 {
		return canBurst
	}
	return noBurst
}

// adjust modifies the token count by delta. A positive delta adds tokens
// (e.g., when returning unused resources), while a negative delta removes
// tokens (e.g., when work is admitted). The token count is capped at
// capacity but has no floor here. The floor is enforced in refill, which
// is called every 1ms. There is no need to enforce the floor more
// frequently than that.
func (m *cpuTimeBurstBucket) adjust(delta int64) {
	m.tokens += delta
	m.tokens = min(m.tokens, m.capacity)
}

// refill adds tokens to the bucket and updates capacity. This is called
// periodically by cpuTimeTokenAllocator (every 1ms). The token count is capped
// at capacity and floored at -capacity/4. The negative floor allows tenants
// that have gone into debt (consumed more than their share) to recover over
// time rather than being disqualified from bursting for arbitrarily long periods
// of time.
func (m *cpuTimeBurstBucket) refill(toAdd int64, capacity int64) {
	m.capacity = capacity
	m.adjust(toAdd)
	m.tokens = max(m.tokens, -m.capacity/4)
}

func (m *cpuTimeBurstBucket) String() string {
	return redact.StringWithoutMarkers(m)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m *cpuTimeBurstBucket) SafeFormat(s redact.SafePrinter, _ rune) {
	var fullness float64
	if m.capacity > 0 {
		fullness = float64(m.tokens) / float64(m.capacity) * 100
	}
	s.Printf("fullness=%.1f%% tokens=%d capacity=%d qual=%s",
		fullness, m.tokens, m.capacity, m.burstQualification())
}
