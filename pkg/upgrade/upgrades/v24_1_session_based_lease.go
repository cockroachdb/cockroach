// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// disableWritesForExpiryBasedLeases disables writing expiry based leases, ensuring
// that any new leases are session based.
func disableWritesForExpiryBasedLeases(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	return nil
}

// adoptUsingOnlySessionBasedLeases ensures that every active lease has a session
// bassed equivalent.
func adoptUsingOnlySessionBasedLeases(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	return nil
}

// upgradeSystemLeasesDescriptor upgrades the system.lease descriptor to be
// session based.
func upgradeSystemLeasesDescriptor(
	ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	return nil
}
