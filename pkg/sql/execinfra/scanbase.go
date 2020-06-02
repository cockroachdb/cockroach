// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// ParallelScanResultThreshold is the number of results up to which, if the
// maximum number of results returned by a scan is known, the table reader
// disables batch limits in the dist sender. This results in the parallelization
// of these scans.
const ParallelScanResultThreshold = 10000

// ScanShouldLimitBatches returns whether the scan should pace itself.
func ScanShouldLimitBatches(maxResults uint64, limitHint int64, flowCtx *FlowCtx) bool {
	// We don't limit batches if the scan doesn't have a limit, and if the
	// spans scanned will return less than the ParallelScanResultThreshold.
	// This enables distsender parallelism - if we limit batches, distsender
	// does *not* parallelize multi-range scan requests.
	if maxResults != 0 &&
		maxResults < ParallelScanResultThreshold &&
		limitHint == 0 &&
		sqlbase.ParallelScans.Get(&flowCtx.Cfg.Settings.SV) {
		return false
	}
	return true
}

// Prettier aliases for execinfrapb.ScanVisibility values.
const (
	ScanVisibilityPublic             = execinfrapb.ScanVisibility_PUBLIC
	ScanVisibilityPublicAndNotPublic = execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
)
