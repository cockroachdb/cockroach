// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

// avgResponseEstimator is a helper that estimates the average size of responses
// received by the Streamer. It is **not** thread-safe.
type avgResponseEstimator struct {
	// responseBytes tracks the total footprint of all responses that the
	// Streamer has already received.
	responseBytes int64
	numResponses  int64
}

// TODO(yuzefovich): use the optimizer-driven estimates.
const initialAvgResponseSize = 1 << 10 // 1KiB

func (e *avgResponseEstimator) getAvgResponseSize() int64 {
	if e.numResponses == 0 {
		return initialAvgResponseSize
	}
	// TODO(yuzefovich): we currently use a simple average over the received
	// responses, but it is likely to be suboptimal because it would be unfair
	// to "large" batches that come in late (i.e. it would not be reactive
	// enough). Consider using another function here.
	return e.responseBytes / e.numResponses
}

// update updates the actual information of the estimator based on numResponses
// responses that took up responseBytes bytes and correspond to a single
// BatchResponse.
func (e *avgResponseEstimator) update(responseBytes int64, numResponses int64) {
	e.responseBytes += responseBytes
	e.numResponses += numResponses
}
