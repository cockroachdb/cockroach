// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

func dropPayloadProgressFromSystemJobs(
	ctx context.Context, cv clusterversion.ClusterVersion, deps upgrade.TenantDeps,
) error {
	_, err := deps.InternalExecutor.ExecEx(ctx, "drop-payload-payload", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, `
ALTER TABLE system.jobs DROP COLUMN IF EXISTS payload;
`)
	if err != nil {
		return err
	}

	_, err = deps.InternalExecutor.ExecEx(ctx, "drop-payload-progress", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride, `
ALTER TABLE system.jobs DROP COLUMN IF EXISTS progress;
`)
	if err != nil {
		return err
	}

	return bumpSystemDatabaseSchemaVersion(ctx, cv, deps)
}
