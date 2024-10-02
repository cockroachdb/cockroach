// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstreamer

import "github.com/cockroachdb/cockroach/pkg/settings"

// avgResponseEstimator is a helper that estimates the average size of responses
// received by the Streamer. It is **not** thread-safe.
type avgResponseEstimator struct {
	avgResponseSizeMultiple float64
	// responseBytes tracks the total footprint of all responses that the
	// Streamer has already received.
	responseBytes float64
	// numRequestsStarted tracks the number of single-range requests that we
	// have started evaluating (regardless of the fact whether the evaluation is
	// finished or needs to be resumed). Any request included into a
	// BatchRequest can be "not started" if the BatchRequest's limits were
	// exhausted before the request's turn came. If the originally-enqueued
	// request spanned multiple ranges, then each single-range request is
	// tracked separately here.
	numRequestsStarted float64
}

const (
	// InitialAvgResponseSize is the initial estimate of the size of a single
	// response.
	// TODO(yuzefovich): use the optimizer-driven estimates.
	InitialAvgResponseSize = 1 << 10 // 1KiB
	// This value was determined using tpchvec/bench test on all TPC-H queries
	// as well as the query in TestStreamerVaryingResponseSizes.
	defaultAvgResponseSizeMultiple = 3.0
)

// streamerAvgResponseSizeMultiple determines the multiple used when calculating
// the average response size.
var streamerAvgResponseSizeMultiple = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"sql.distsql.streamer.avg_response_size_multiple",
	"determines the multiple used when calculating the average response size by the streamer component",
	defaultAvgResponseSizeMultiple,
	settings.NonNegativeFloat,
)

func (e *avgResponseEstimator) init(sv *settings.Values) {
	e.avgResponseSizeMultiple = streamerAvgResponseSizeMultiple.Get(sv)
}

// getAvgResponseSize returns the current estimate of a footprint of a response
// to one single-range request.
//
// This means that if a ScanRequest spans multiple ranges, then this estimate
// should be used for the TargetBytes parameter for each truncated single-range
// ScanRequest independently (rather than the whole multi-range original one).
// Also, it means that if one single-range ScanRequest needs to be paginated
// across BatchRequests (i.e. there will be "resume" ScanRequests), then the
// estimate includes the footprint of all those "resume" responses as well as of
// the first "non-resume" response.
// TODO(yuzefovich): we might want to have a separate estimate for Gets and
// Scans.
func (e *avgResponseEstimator) getAvgResponseSize() int64 {
	if e.numRequestsStarted == 0 {
		return InitialAvgResponseSize
	}
	// We're estimating the response size as the average over the received
	// responses. Importantly, we divide the total responses' footprint by the
	// number of "started" requests. This allows us to handle partial
	// ScanResponses that need to be resumed across BatchRequests without
	// double-counting them in the denominator of the average computation (which
	// would lead to artificially low estimate, see #103586 for an example).
	//
	// Note that we're multiplying the average by a response size multiple for a
	// couple of reasons:
	//
	// 1. this allows us to fulfill requests that are slightly larger than the
	// current average. For example, imagine that we're processing three
	// requests of sizes 100B, 110B, and 120B sequentially, one at a time.
	// Without the multiple, after the first request, our estimate would be 100B
	// so the second request would come back empty (with ResumeNextBytes=110),
	// so we'd have to re-issue the second request. At that point the average is
	// 105B, so the third request would again come back empty and need to be
	// re-issued with larger TargetBytes. Having the multiple allows us to
	// handle such a scenario without any requests coming back empty. In
	// particular, TPC-H Q17 has similar setup.
	//
	// 2. this allows us to slowly grow the TargetBytes parameter over time when
	// requests can be returned partially multiple times (i.e. Scan requests
	// spanning multiple rows). Consider a case when a single Scan request has
	// to return 1MB worth of data, but each row is only 100B. With the initial
	// estimate of 1KB, every request would always come back with exactly 10
	// rows, and the avg response size would always stay at 1KB. We'd end up
	// issuing 1000 of such requests. Having a multiple here allows us to grow
	// the estimate over time, reducing the total number of requests needed
	// (although the growth is still not fast enough).
	//
	// TODO(yuzefovich): we currently use a simple average over the received
	// responses, but it is likely to be suboptimal because it would be unfair
	// to "large" batches that come in late (i.e. it would not be reactive
	// enough). Consider using another function here.
	return int64(e.responseBytes / e.numRequestsStarted * e.avgResponseSizeMultiple)
}

// update updates the actual information of the estimator based on responses
// that took up responseBytes bytes in footprint and correspond to a single
// BatchResponse. numRequestsStarted indicates the number of requests that were
// just started in that BatchResponse.
func (e *avgResponseEstimator) update(responseBytes int64, numRequestsStarted int) {
	e.responseBytes += float64(responseBytes)
	e.numRequestsStarted += float64(numRequestsStarted)
}
