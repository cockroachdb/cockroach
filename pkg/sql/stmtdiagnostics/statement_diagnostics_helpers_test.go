// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stmtdiagnostics

import (
	"context"
	"time"
)

// TestingFindRequest exports findRequest for testing purposes.
func (r *Registry) TestingFindRequest(requestID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.findRequestLocked(RequestID(requestID))
}

// InsertRequestInternal exposes the form of insert which returns the request ID
// as an int64 to tests in this package.
func (r *Registry) InsertRequestInternal(
	ctx context.Context,
	fprint string,
	planGist string,
	antiPlanGist bool,
	samplingProbability float64,
	minExecutionLatency time.Duration,
	expiresAfter time.Duration,
) (int64, error) {
	// Note that redacted bundles are checked in TestExplainAnalyzeDebug.
	id, err := r.insertRequestInternal(
		ctx, fprint, planGist, antiPlanGist, samplingProbability,
		minExecutionLatency, expiresAfter, false, /* redacted */
	)
	return int64(id), err
}

// PollingInterval is exposed to override in tests.
var PollingInterval = pollingInterval
