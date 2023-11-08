// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvbase

// TenantsStorageMetricsSet is the set of all metric names contained
// within TenantsStorageMetrics, recorded at the individual tenant level.
//
// Made available in kvbase to help avoid import cycles.
var TenantsStorageMetricsSet map[string]struct{}
