// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admissionpb

// Normalize fills the BurstFrac of each ResourceGroupConfig in cfgs:
//
//	BurstFrac = CPUWeight / sum(CPUWeight)
//
// The slice is mutated in place and returned for chaining. Callers that
// need to preserve the original BurstFrac values must copy first.
//
// CPUWeight values must be non-negative; SQL DDL rejects zero and
// negative values at ingest.
func Normalize(cfgs []ResourceGroupConfig) []ResourceGroupConfig {
	var sum int64
	for _, c := range cfgs {
		if c.CPUWeight > 0 {
			sum += c.CPUWeight
		}
	}
	for i := range cfgs {
		if cfgs[i].CPUWeight <= 0 {
			cfgs[i].BurstFrac = 0
			continue
		}
		cfgs[i].BurstFrac = float64(cfgs[i].CPUWeight) / float64(sum)
	}
	return cfgs
}
