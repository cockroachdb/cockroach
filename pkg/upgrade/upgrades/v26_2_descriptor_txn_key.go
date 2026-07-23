// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

// descriptorTxnKeyCleanup deletes usage of special txn tracking keys from
// the system.descriptor table.
func descriptorTxnKeyCleanup(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	err := d.LeaseManager.CleanUpdateKeysForUpgrade(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to clean up lease update keys")
	}
	return nil
}
