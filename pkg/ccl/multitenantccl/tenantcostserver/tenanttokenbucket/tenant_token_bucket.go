// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package tenanttokenbucket implements the tenant token bucket server-side
// algorithm described in the distributed token bucket RFC. It has minimal
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
	// TODO(radu): fill in response.
	return res
}

// TODO(radu): add Reconfigure API.
