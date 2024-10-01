// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvbase

// TenantsStorageMetricsSet is the set of all metric names contained
// within TenantsStorageMetrics, recorded at the individual tenant level.
//
// Made available in kvbase to help avoid import cycles.
var TenantsStorageMetricsSet map[string]struct{}
