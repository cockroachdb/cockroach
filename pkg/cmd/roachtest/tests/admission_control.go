// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import "github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"

func registerAdmission(r registry.Registry) {
	// TODO(irfansharif): Can we write these tests using cgroups instead?
	// Limiting CPU/bandwidth directly?

	// TODO(irfansharif): Some of these tests hooks into prometheus/grafana.
	// It'd be nice to use the grafana annotations API to explicitly annotate
	// the points at which we do cluster-level things, like set zone configs to
	// trigger a round of snapshots.

	// TODO(irfansharif): Integrate with probabilistic tracing machinery,
	// capturing outliers automatically for later analysis.

	// TODO(irfansharif): Look into clusterstats and what that emits to
	// roachperf. Need to munge with histogram data to compute % test run spent
	// over some latency threshold. Will be Useful to track over time.

	registerFollowerOverload(r)
	registerElasticControlForBackups(r)
	registerElasticControlForCDC(r)
	registerElasticControlForRowLevelTTL(r)
	registerMultiStoreOverload(r)
	registerMultiTenantFairness(r)
	registerSnapshotOverload(r)
	registerSnapshotOverloadIO(r)
	registerTPCCOverload(r)
	registerTPCCSevereOverload(r)
	registerIndexOverload(r)
	registerIndexBackfill(r)
	registerDatabaseDrop(r)
	registerIntentResolutionOverload(r)
	registerElasticIO(r)
	registerDiskBandwidthOverload(r)
	registerElasticWorkloadMixedVersion(r)
}
