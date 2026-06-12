// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

// StoreTenantMetricsForTesting exposes the package-internal storeTenantMetrics
// set so that tests in other packages can assert it stays in sync with its
// source of truth, kvbase.TenantsStorageMetricsSet.
var StoreTenantMetricsForTesting = storeTenantMetrics
