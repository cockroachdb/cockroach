// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import "github.com/cockroachdb/redact"

type storeLoadSummary struct {
	worstDim                                               LoadDimension // for logging only
	sls                                                    loadSummary
	nls                                                    loadSummary
	dimSummary                                             [NumLoadDimensions]loadSummary
	highDiskSpaceUtilization                               bool
	maxFractionPendingIncrease, maxFractionPendingDecrease float64

	loadSeqNum uint64
}

func (sls storeLoadSummary) String() string {
	return redact.StringWithoutMarkers(sls)
}

func (sls storeLoadSummary) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("(store=%v worst=%v cpu=%v writes=%v bytes=%v node=%v high_disk=%v frac_pending=%.2f,%.2f(%t))",
		sls.sls, sls.worstDim, sls.dimSummary[CPURate], sls.dimSummary[WriteBandwidth], sls.dimSummary[ByteSize],
		sls.nls, sls.highDiskSpaceUtilization, sls.maxFractionPendingIncrease,
		sls.maxFractionPendingDecrease,
		sls.maxFractionPendingIncrease < epsilon && sls.maxFractionPendingDecrease < epsilon)
}
