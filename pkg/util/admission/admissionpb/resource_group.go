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
// A zero CPUWeight yields BurstFrac=0; SQL DDL rejects zero at ingest,
// so this only guards configs that predate a weight being set.
func Normalize(cfgs []ResourceGroupConfig) []ResourceGroupConfig {
	var sum uint64
	for _, c := range cfgs {
		sum += uint64(c.CPUWeight)
	}
	for i := range cfgs {
		if cfgs[i].CPUWeight == 0 {
			cfgs[i].BurstFrac = 0
			continue
		}
		cfgs[i].BurstFrac = float64(cfgs[i].CPUWeight) / float64(sum)
	}
	return cfgs
}
