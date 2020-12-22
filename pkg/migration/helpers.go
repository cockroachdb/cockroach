// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FenceVersionFor constructs the appropriate "fence version" for the given
// cluster version. Fence versions allow the migrations infrastructure to safely
// step through consecutive cluster versions in the presence of Nodes (running
// any binary version) being added to the cluster. See the migration manager
// above for intended usage.
//
// Fence versions (and the migrations infrastructure entirely) were introduced
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
