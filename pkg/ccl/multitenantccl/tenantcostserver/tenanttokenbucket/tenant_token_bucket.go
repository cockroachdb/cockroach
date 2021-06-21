// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This package implements the tenant token bucket server-side algorithm
// described in the distributed token bucket RFC. It has minimal
// dependencies and is meant to be testable on its own.
package tenanttokenbucket

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// State of the distributed token bucket.
type State struct {
	// Burst limit in RUs.
	RUBurstLimit float64
	// Refill rate in RUs/second.
	RURefillRate float64

	// Available (burst) RUs.
	RUCurrent float64

	// Sum of the last reported share value for each tenant.
	CurrentShareSum float64
}

// Request processes a request for more tokens and updates the State
// accordingly.
func (s *State) Request(
	req *roachpb.TokenBucketRequest, now time.Time,
) roachpb.TokenBucketResponse {
	var res roachpb.TokenBucketResponse
	res.OperationTimestamp = now
	// TODO(radu): for now we always grant tokens.
	res.GrantedTokens = req.RequestedTokens
	res.TrickleTime = 0
	res.MaxBurstTokens = 0
	return res
}
