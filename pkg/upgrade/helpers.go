// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrade

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// FenceVersionFor constructs the appropriate "fence version" for the given
// cluster version. Fence versions allow the upgrades infrastructure to safely
// step through consecutive cluster versions in the presence of Nodes (running
// any binary version) being added to the cluster. See the upgrade manager
// above for intended usage.
//
// Fence versions (and the upgrades infrastructure entirely) were introduced
// in the 21.1 release cycle. In the same release cycle, we introduced the
// invariant that new user-defined versions (users being crdb engineers) must
// always have even-numbered Internal versions, thus reserving the odd numbers
// to slot in fence versions for each cluster version. See top-level
// documentation in pkg/clusterversion for more details.
func FenceVersionFor(
	ctx context.Context, cv clusterversion.ClusterVersion,
) clusterversion.ClusterVersion {
	if (cv.Internal % 2) != 0 {
		log.Fatalf(ctx, "only even numbered internal versions allowed, found %s", cv.Version)
	}

	// We'll pick the odd internal version preceding the cluster version,
	// slotting ourselves right before it.
	fenceCV := cv
	fenceCV.Internal--
	return fenceCV
}

// BumpSystemDatabaseSchemaVersion bumps the SystemDatabaseSchemaVersion
// field for the system database descriptor. It is called after every upgrade
// step that has an associated migration, and when upgrading to the final
// clusterversion for a release.
func BumpSystemDatabaseSchemaVersion(
	ctx context.Context, version roachpb.Version, descDB descs.DB,
) error {
	if err := descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		systemDBDesc, err := txn.Descriptors().MutableByID(txn.KV()).Database(ctx, keys.SystemDatabaseID)
		if err != nil {
			return err
		}
		if sv := systemDBDesc.GetSystemDatabaseSchemaVersion(); sv != nil {
			if version.Less(*sv) {
				return errors.AssertionFailedf(
					"new system schema version (%#v) is lower than previous system schema version (%#v)",
					version,
					*sv,
				)
			} else if version.Equal(sv) {
				return nil
			}
		}
		systemDBDesc.SystemDatabaseSchemaVersion = &version
		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, systemDBDesc, txn.KV())
	}); err != nil {
		return err
	}
	return nil
}
