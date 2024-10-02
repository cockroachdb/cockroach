// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package apiconstants

const (
	// APIV2Path is the prefix for the RESTful v2 API.
	APIV2Path = "/api/v2/"

	// AdminPrefix is the prefix for RESTful endpoints used to provide an
	// administrative interface to the cockroach cluster.
	AdminPrefix = "/_admin/v1/"

	// AdminHealth is the canonical URL path to the health endpoint.
	// (This is also aliased via /health.)
	AdminHealth = AdminPrefix + "health"

	// StatusPrefix is the root of the cluster statistics and metrics API.
	StatusPrefix = "/_status/"

	// StatusVars exposes Prometheus metrics for monitoring consumption.
	StatusVars = StatusPrefix + "vars"

	// LoadStatusVars exposes prometheus metrics for instant monitoring of CPU load.
	LoadStatusVars = StatusPrefix + "load"

	// DefaultAPIEventLimit is the default maximum number of events
	// returned by any endpoints returning events.
	DefaultAPIEventLimit = 1000

	// MaxConcurrentRequests is the maximum number of RPC fan-out requests
	// that will be made at any point of time.
	MaxConcurrentRequests = 100

	// MaxConcurrentPaginatedRequests is the maximum number of RPC fan-out
	// requests that will be made at any point of time for a row-limited /
	// paginated request. This should be much lower than maxConcurrentRequests
	// as too much concurrency here can result in wasted results.
	MaxConcurrentPaginatedRequests = 4
)
