// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admissionpb

// minNormalizationDenominator is the floor applied to the sum of CPU
// weights when computing each group's BurstFrac. Without it, a single
// group with CPUWeight=1 would appear to receive 100% of CPU; with it,
// it receives 1%, which matches operator intuition for "a small group
// in an otherwise unloaded cluster".
const minNormalizationDenominator = 100

// Normalize fills the BurstFrac of each ResourceGroupConfig in cfgs:
//
//	BurstFrac = CPUWeight / max(minNormalizationDenominator, sum(CPUWeight))
//
// The floor in the denominator prevents a single small group from
// appearing to receive 100% of CPU when no other group is configured.
// Once the configured weights sum to at least the floor, each group's
// BurstFrac is simply its share of the total.
//
// The slice is mutated in place and returned for chaining. Callers that
// need to preserve the original BurstFrac values must copy first.
//
// CPUWeight values must be non-negative; SQL DDL rejects zero and
// negative values at ingest. As defense in depth, a non-positive sum
// produces BurstFrac=0 for every group rather than dividing by zero.
func Normalize(cfgs []ResourceGroupConfig) []ResourceGroupConfig {
	var sum int64
	for _, c := range cfgs {
		if c.CPUWeight > 0 {
			sum += c.CPUWeight
		}
	}
	denom := sum
	if denom < minNormalizationDenominator {
		denom = minNormalizationDenominator
	}
	for i := range cfgs {
		if cfgs[i].CPUWeight <= 0 {
			cfgs[i].BurstFrac = 0
			continue
		}
		cfgs[i].BurstFrac = float64(cfgs[i].CPUWeight) / float64(denom)
	}
	return cfgs
}
