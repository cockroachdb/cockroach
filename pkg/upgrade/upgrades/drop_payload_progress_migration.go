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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

const dropPayloadQuery = `
ALTER TABLE system.jobs DROP COLUMN payload
`

const dropProgressQuery = `
ALTER TABLE system.jobs DROP COLUMN progress
`

func dropPayloadProgressFromJobsTable(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := d.InternalExecutor.ExecEx(ctx, "drop-jobs-payload", txn,
			sessiondata.NodeUserSessionDataOverride, dropPayloadQuery)
		if err != nil {
			panic(errors.Wrap(err, "failed to drop system.jobs payload"))
		}

		_, err = d.InternalExecutor.ExecEx(ctx, "drop-jobs-progress", txn,
			sessiondata.NodeUserSessionDataOverride, dropProgressQuery)
		return errors.Wrap(err, "failed to drop system.jobs progress")
	})
}
