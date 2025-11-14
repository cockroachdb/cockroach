// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/mmaprototype/mmaload"
	"github.com/cockroachdb/redact"
)

// A storeLoadSummary is a classification of a store's load relative to the mean
// load across a set of permissible stores (often, all that satisfy the
// constraints for a given range). Sources and targets are primarily picked
// based on the store and node level load summaries contained in this struct.
type storeLoadSummary struct {
	worstDim                                               mmaload.LoadDimension // for logging only
	sls                                                    loadSummary
	nls                                                    loadSummary
	dimSummary                                             [mmaload.NumLoadDimensions]loadSummary
	highDiskSpaceUtilization                               bool
	maxFractionPendingIncrease, maxFractionPendingDecrease float64

	loadSeqNum uint64
}

func (sls storeLoadSummary) String() string {
	return redact.StringWithoutMarkers(sls)
}

func (sls storeLoadSummary) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(store=%v worst=%v cpu=%v writes=%v bytes=%v node=%v high_disk=%v frac_pending=%.2f,%.2f(%t))",
		sls.sls, sls.worstDim, sls.dimSummary[mmaload.CPURate], sls.dimSummary[mmaload.WriteBandwidth], sls.dimSummary[mmaload.ByteSize],
		sls.nls, sls.highDiskSpaceUtilization, sls.maxFractionPendingIncrease,
		sls.maxFractionPendingDecrease,
		sls.maxFractionPendingIncrease < epsilon && sls.maxFractionPendingDecrease < epsilon)
}
